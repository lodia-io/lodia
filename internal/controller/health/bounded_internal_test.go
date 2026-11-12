// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

type counting struct {
	next  http.RoundTripper
	bytes atomic.Int64
}

func (c *counting) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := c.next.RoundTrip(req)
	if err == nil {
		resp.Body = countedBody{resp.Body, &c.bytes}
	}
	return resp, err
}

type countedBody struct {
	io.ReadCloser
	bytes *atomic.Int64
}

func (b countedBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.bytes.Add(int64(n))
	return n, err
}

func kubeconfigFor(t *testing.T, server string) []byte {
	t.Helper()
	out, err := clientcmd.Write(clientcmdapi.Config{
		Clusters:       map[string]*clientcmdapi.Cluster{"tenant": {Server: server}},
		AuthInfos:      map[string]*clientcmdapi.AuthInfo{"lodia": {}},
		Contexts:       map[string]*clientcmdapi.Context{"tenant": {Cluster: "tenant", AuthInfo: "lodia"}},
		CurrentContext: "tenant",
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func leaseListOf(size int) []byte {
	const head = `{"kind":"LeaseList","apiVersion":"coordination.k8s.io/v1","metadata":{"resourceVersion":"1"},"items":[{"metadata":{"name":"big","annotations":{"pad":"`
	const tail = `"}}}]}`
	return []byte(head + strings.Repeat("x", size-len(head)-len(tail)) + tail)
}

func TestAResponseLongerThanTheBoundFailsBeforeItIsDecoded(t *testing.T) {
	for _, c := range []struct {
		name       string
		size       int
		chunked    bool
		wantErr    bool
		wantBytes  int64
		wantLeases int
	}{
		{"exactly the bound", maxResponseBytes, false, false, maxResponseBytes, 1},
		{"exactly the bound, chunked", maxResponseBytes, true, false, maxResponseBytes, 1},
		{"a byte over", maxResponseBytes + 1, false, true, maxResponseBytes, 0},
		{"a byte over, chunked", maxResponseBytes + 1, true, true, maxResponseBytes, 0},
		{"far over, chunked", 3 * maxResponseBytes, true, true, maxResponseBytes, 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			body := leaseListOf(c.size)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if c.chunked {
					for rest := body; len(rest) > 0; {
						n := min(len(rest), 64<<10)
						_, _ = w.Write(rest[:n])
						w.(http.Flusher).Flush()
						rest = rest[n:]
					}
					return
				}
				w.Header().Set("Content-Length", strconv.Itoa(len(body)))
				_, _ = w.Write(body)
			}))
			t.Cleanup(server.Close)
			r := &Reconciler{}
			key := types.NamespacedName{Namespace: "team-a", Name: "dev"}
			tenant, err := r.tenant(key, kubeconfigFor(t, server.URL))
			if err != nil {
				t.Fatal(err)
			}
			read := &counting{next: r.clients[key].transport.Transport}
			r.clients[key].transport.Transport = read

			leases, err := tenant.nodeLeases.List(t.Context(), metav1.ListOptions{})
			if c.wantErr != (err != nil) || c.wantErr && !errors.Is(err, errResponseTooLong) {
				t.Fatalf("list of %d bytes returned %v, want the error of the bound: %t", c.size, err, c.wantErr)
			}
			if c.wantErr && !strings.Contains(err.Error(), strconv.Itoa(maxResponseBytes)) {
				t.Errorf("the error %q does not name the bound %d", err, maxResponseBytes)
			}
			if got := read.bytes.Load(); got != c.wantBytes {
				t.Errorf("the client read %d bytes of a response of %d, want %d", got, c.size, c.wantBytes)
			}
			if err == nil && len(leases.Items) != c.wantLeases {
				t.Errorf("the response decoded to %d Leases, want %d", len(leases.Items), c.wantLeases)
			}
		})
	}
}

type idleCloser struct {
	http.RoundTripper
	closed atomic.Bool
}

func (i *idleCloser) CloseIdleConnections() { i.closed.Store(true) }

func TestTheCapStillLetsAForgottenControlPlaneCloseItsIdleConnections(t *testing.T) {
	below := &idleCloser{}
	utilnet.CloseIdleConnectionsFor(capResponses(below))
	if !below.closed.Load() {
		t.Error("CloseIdleConnectionsFor did not reach the transport below the cap")
	}
}
