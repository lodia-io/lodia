// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

func TestWhatHealthSendsBackOfATokenAndAVersionAtTheirRawBoundsIsPinned(t *testing.T) {
	sizedForTheBounds(t)
	for _, c := range []struct {
		name         string
		byte         string
		token        int
		query        int
		heldVersion  int
		patchVersion int
	}{
		{"letters", "a", 4096, 4115, 64, 64},
		{"characters JSON escapes as six", "<", 4096, 12307, 64, 384},
		{"invalid UTF-8, decoded to three bytes each", "\xff", 12288, 36883, 192, 192},
	} {
		t.Run(c.name, func(t *testing.T) {
			var (
				mu    sync.Mutex
				next  string
				query int
				patch string
			)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				mu.Lock()
				defer mu.Unlock()
				switch {
				case req.URL.Path == nodeLeasesPath && req.URL.Query().Get("continue") == "":
					_, _ = io.WriteString(w, `{"metadata":{"continue":"`+strings.Repeat(c.byte, maxContinue)+`"},"items":[]}`)
				case req.URL.Path == nodeLeasesPath:
					next, query = req.URL.Query().Get("continue"), len(req.URL.RawQuery)
					_, _ = io.WriteString(w, `{"metadata":{},"items":[]}`)
				case req.Method == http.MethodPatch:
					body, _ := io.ReadAll(req.Body)
					patch = string(body)
					_, _ = io.WriteString(w, `{"metadata":{"resourceVersion":"2"},"spec":{}}`)
				default:
					_, _ = io.WriteString(w, `{"metadata":{"resourceVersion":"`+strings.Repeat(c.byte, maxVersionRaw)+`"},"spec":{"holderIdentity":"lodia"}}`)
				}
			}))
			t.Cleanup(server.Close)
			r := &NodesReconciler{Timeout: time.Minute, MaxNodes: 5000}
			tenant, err := r.tenant(types.NamespacedName{Namespace: "team-a", Name: "dev"}, kubeconfigFor(t, server.URL))
			if err != nil {
				t.Fatal(err)
			}
			if _, stop, err := r.listLeases(t.Context(), tenant); err != nil || stop != "" {
				t.Fatalf("the list stopped %q with error %v", stop, err)
			}
			if len(next) != c.token || query != c.query {
				t.Errorf("the second page was asked with a token of %d bytes in a query of %d, want %d and %d", len(next), query, c.token, c.query)
			}
			read, err := r.readLease(t.Context(), tenant)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := r.writeLease(t.Context(), tenant, read, renewLease, time.Now()); err != nil {
				t.Fatal(err)
			}
			const key = `"resourceVersion":"`
			start := strings.Index(patch, key) + len(key)
			sent := strings.Index(patch[start:], `"`)
			if len(read.Metadata.ResourceVersion) != c.heldVersion || sent != c.patchVersion {
				t.Errorf("a resourceVersion of %d raw bytes is held as %d bytes and patched as %d, want %d and %d", maxVersionRaw, len(read.Metadata.ResourceVersion), sent, c.heldVersion, c.patchVersion)
			}
		})
	}
}
