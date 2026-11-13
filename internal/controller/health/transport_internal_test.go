// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	logf "sigs.k8s.io/controller-runtime/pkg/log"
	crzap "sigs.k8s.io/controller-runtime/pkg/log/zap"
)

func TestARequestThatFailsBeforeItsAnswerCarriesNoByteOfTheTokenItAsksWith(t *testing.T) {
	type list struct {
		name string
		path string
		read func(context.Context, *NodesReconciler, *tenant) error
	}
	lists := []list{
		{"Leases", nodeLeasesPath, func(ctx context.Context, r *NodesReconciler, t *tenant) error {
			_, _, err := r.listLeases(ctx, t)
			return err
		}},
		{"Nodes", nodesPath, func(ctx context.Context, r *NodesReconciler, t *tenant) error {
			_, _, err := r.listNodes(ctx, t)
			return err
		}},
	}
	for _, l := range lists {
		for _, kind := range []struct {
			name  string
			token string
			seen  []string
		}{
			{"letters", strings.Repeat("a", maxContinue), []string{"aaaaaaaa"}},
			{"<", strings.Repeat("<", maxContinue), []string{"<<<<", "%3C%3C%3C"}},
			{"0xff", strings.Repeat("\xff", maxContinue), []string{"\xff\xff", "%EF%BF%BD", "�"}},
		} {
			for _, failure := range []struct {
				name string
				is   error
			}{
				{"a deadline", context.DeadlineExceeded},
				{"a reset", syscall.ECONNRESET},
			} {
				t.Run(l.name+", "+kind.name+", "+failure.name, func(t *testing.T) {
					r, tenant := http1.tenant(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
						switch {
						case req.URL.Path != l.path:
							w.Header().Set("Content-Type", "application/json")
							_, _ = w.Write([]byte(`{"metadata":{},"items":[]}`))
						case req.URL.Query().Get("continue") == "":
							w.Header().Set("Content-Type", "application/json")
							_, _ = w.Write([]byte(`{"metadata":{"continue":"` + kind.token + `"},"items":[]}`))
						case errors.Is(failure.is, syscall.ECONNRESET):
							conn, _, err := w.(http.Hijacker).Hijack()
							if err != nil {
								t.Error(err)
								return
							}
							if tcp, ok := conn.(*net.TCPConn); ok {
								_ = tcp.SetLinger(0)
							}
							_ = conn.Close()
						default:
							<-req.Context().Done()
						}
					}))
					r.Timeout = 500 * time.Millisecond
					ctx, cancel := context.WithTimeout(t.Context(), r.Timeout)
					defer cancel()
					err := l.read(ctx, r, tenant)
					if err == nil {
						t.Fatal("the list did not fail")
					}
					var logged bytes.Buffer
					logger := crzap.New(crzap.WriteTo(&logged))
					logger.Error(err, "list the tenant's "+l.name)
					if _, err := r.list(logf.IntoContext(t.Context(), logger), tenant, &sampling{}, time.Now()); err != nil {
						logger.Error(err, "list the node Leases")
					}
					t.Logf("the error is %d bytes: %q; the log %d bytes", len(err.Error()), err.Error(), logged.Len())
					if !strings.HasPrefix(err.Error(), "Get "+strconv.Quote(l.path)+": ") {
						t.Errorf("the error is %q, want one that names the verb and the path and no query", err)
					}
					if !errors.Is(err, failure.is) {
						t.Errorf("the error is %v, want one that is %v", err, failure.is)
					}
					for what, text := range map[string]string{"error": err.Error(), "log": logged.String()} {
						for _, seen := range append(kind.seen, "continue") {
							if strings.Contains(text, seen) {
								t.Errorf("the %s carries the token: %.200q", what, text)
							}
						}
					}
					if len(err.Error()) > 256 || logged.Len() > 2048 {
						t.Errorf("the error is %d bytes and the log %d", len(err.Error()), logged.Len())
					}
				})
			}
		}
	}
}

func TestAWriteThatFailsBeforeItsAnswerIsTheErrorWithItsVerbAndItsPath(t *testing.T) {
	const lease = "/apis/coordination.k8s.io/v1/namespaces/kube-system/leases"
	for _, c := range []struct {
		name string
		verb string
		path string
		do   func(context.Context, *tenant) error
	}{
		{"a read", "Get", lease + "/kube-controller-manager", func(ctx context.Context, t *tenant) error {
			_, err := t.getLease(ctx, "kube-controller-manager")
			return err
		}},
		{"a create", "Post", lease, func(ctx context.Context, t *tenant) error {
			_, err := t.createLease(ctx, []byte(`{"metadata":{"name":"kube-controller-manager"}}`))
			return err
		}},
		{"a patch", "Patch", lease + "/kube-controller-manager", func(ctx context.Context, t *tenant) error {
			_, err := t.patchLease(ctx, []byte(`{}`))
			return err
		}},
	} {
		for _, failure := range []struct {
			name string
			is   error
		}{
			{"a deadline", context.DeadlineExceeded},
			{"a reset", syscall.ECONNRESET},
		} {
			t.Run(c.name+", "+failure.name, func(t *testing.T) {
				_, tenant := http1.tenant(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
					_, _ = io.Copy(io.Discard, req.Body)
					if !errors.Is(failure.is, syscall.ECONNRESET) {
						<-req.Context().Done()
						return
					}
					conn, _, err := w.(http.Hijacker).Hijack()
					if err != nil {
						t.Error(err)
						return
					}
					if tcp, ok := conn.(*net.TCPConn); ok {
						_ = tcp.SetLinger(0)
					}
					_ = conn.Close()
				}))
				ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
				defer cancel()
				err := c.do(ctx, tenant)
				if err == nil {
					t.Fatal("the request did not fail")
				}
				t.Logf("the error is %d bytes: %q", len(err.Error()), err.Error())
				if !strings.HasPrefix(err.Error(), c.verb+" "+strconv.Quote(c.path)+": ") || !errors.Is(err, failure.is) {
					t.Errorf("the error is %q, want one that names %s %s and is %v", err, c.verb, c.path, failure.is)
				}
			})
		}
	}
}
