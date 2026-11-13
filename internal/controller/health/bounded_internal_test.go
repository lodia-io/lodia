// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
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

			leases, err := tenant.leasePage(t.Context(), metav1.ListOptions{})
			if c.wantErr != (err != nil) || c.wantErr && !errors.Is(err, errResponseTooLong) {
				t.Fatalf("list of %d bytes returned %v, want the error of the bound: %t", c.size, err, c.wantErr)
			}
			if c.wantErr && !strings.Contains(err.Error(), strconv.Itoa(maxResponseBytes)) {
				t.Errorf("the error %q does not name the bound %d", err, maxResponseBytes)
			}
			if got := read.bytes.Load(); got != c.wantBytes {
				t.Errorf("the client read %d bytes of a response of %d, want %d", got, c.size, c.wantBytes)
			}
			if err == nil && len(leases.Items.items) != c.wantLeases {
				t.Errorf("the response decoded to %d Leases, want %d", len(leases.Items.items), c.wantLeases)
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

func failureOver(code int, reason, message string, size int) []byte {
	status := `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"` + reason + `","code":` + strconv.Itoa(code) + `,"message":"` + message + `"}`
	return []byte(status + strings.Repeat(" ", size-len(status)))
}

func TestAResponseOverTheBoundIsAStopOrAFailedReadAndNeverDecodedWhateverItsStatus(t *testing.T) {
	const (
		leasesStop = "a response listing the Leases of kube-node-lease is longer than 8388608 bytes"
		nodesStop  = "a response listing the Nodes is longer than 8388608 bytes"
	)
	failed := func(read func(context.Context, *tenant) error) func(context.Context, *tenant, *NodesReconciler) (string, error) {
		return func(ctx context.Context, t *tenant, _ *NodesReconciler) (string, error) { return "", read(ctx, t) }
	}
	for _, read := range []struct {
		name string
		do   func(context.Context, *tenant, *NodesReconciler) (string, error)
		stop string
	}{
		{"the Leases of kube-node-lease", func(ctx context.Context, t *tenant, r *NodesReconciler) (string, error) {
			_, stop, err := r.listLeases(ctx, t)
			return stop, err
		}, leasesStop},
		{"the Nodes", func(ctx context.Context, t *tenant, r *NodesReconciler) (string, error) {
			_, stop, err := r.listNodes(ctx, t)
			return stop, err
		}, nodesStop},
		{"a leader Lease", failed(func(ctx context.Context, t *tenant) error {
			_, err := t.getLease(ctx, "kube-controller-manager")
			return err
		}), ""},
		{"a Lease created", failed(func(ctx context.Context, t *tenant) error { _, err := t.createLease(ctx, []byte(`{}`)); return err }), ""},
		{"a Lease patched", failed(func(ctx context.Context, t *tenant) error { _, err := t.patchLease(ctx, []byte(`{}`)); return err }), ""},
		{"Services", failed(func(ctx context.Context, t *tenant) error {
			_, err := firstPage[service](ctx, t, servicesPath)
			return err
		}), ""},
		{"PersistentVolumes", failed(func(ctx context.Context, t *tenant) error {
			_, err := firstPage[persistentVolume](ctx, t, volumesPath)
			return err
		}), ""},
		{"PersistentVolumeClaims", failed(func(ctx context.Context, t *tenant) error {
			_, err := firstPage[persistentVolumeClaim](ctx, t, claimsPath)
			return err
		}), ""},
		{"ValidatingWebhookConfigurations", failed(func(ctx context.Context, t *tenant) error {
			_, err := outsideWebhooks(ctx, t, validatingPath)
			return err
		}), ""},
		{"MutatingWebhookConfigurations", failed(func(ctx context.Context, t *tenant) error {
			_, err := outsideWebhooks(ctx, t, mutatingPath)
			return err
		}), ""},
		{"APIServices", failed(func(ctx context.Context, t *tenant) error {
			_, err := firstPage[apiService](ctx, t, apiServicePath)
			return err
		}), ""},
		{"CronJobs", failed(func(ctx context.Context, t *tenant) error {
			_, err := firstPage[cronJob](ctx, t, cronJobsPath)
			return err
		}), ""},
	} {
		for _, status := range []struct {
			code    int
			reason  string
			message string
			is      func(error) bool
		}{
			{http.StatusInternalServerError, "InternalError", "an error", apierrors.IsInternalError},
			{http.StatusConflict, "Conflict", "the object has been modified", apierrors.IsConflict},
			{http.StatusNotFound, "NotFound", "not found", apierrors.IsNotFound},
		} {
			t.Run(read.name+", "+strconv.Itoa(status.code), func(t *testing.T) {
				body := failureOver(status.code, status.reason, status.message, maxResponseBytes+1)
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					w.Header().Set("Content-Length", strconv.Itoa(len(body)))
					w.WriteHeader(status.code)
					_, _ = w.Write(body)
				}))
				t.Cleanup(server.Close)
				r := &NodesReconciler{Timeout: time.Minute, MaxNodes: 5000}
				tenant, err := r.tenant(types.NamespacedName{Namespace: "team-a", Name: "dev"}, kubeconfigFor(t, server.URL))
				if err != nil {
					t.Fatal(err)
				}
				stop, err := read.do(t.Context(), tenant, r)
				if read.stop != "" {
					if err != nil || stop != read.stop {
						t.Errorf("the list returned stop %q and error %v, want stop %q and none", stop, err, read.stop)
					}
					return
				}
				if !errors.Is(err, errResponseTooLong) || status.is(err) {
					t.Errorf("the read returned %v, want the error of the bound and nothing the Status of the body says", err)
				}
			})
		}
	}
}

func TestAFailedRequestOfExactlyTheBoundIsStillItsStatus(t *testing.T) {
	body := failureOver(http.StatusConflict, "Conflict", "the object has been modified", maxResponseBytes)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write(body)
	}))
	t.Cleanup(server.Close)
	tenant, err := (&NodesReconciler{}).tenant(types.NamespacedName{Namespace: "team-a", Name: "dev"}, kubeconfigFor(t, server.URL))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tenant.getLease(t.Context(), "kube-controller-manager"); !apierrors.IsConflict(err) {
		t.Errorf("a Status of exactly %d bytes returned %v, want the conflict it says", maxResponseBytes, err)
	}
}
