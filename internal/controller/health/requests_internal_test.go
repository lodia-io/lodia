// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"bufio"
	"encoding/pem"
	"net"
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
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

func TestARequestIsSentOnceWhateverTheServerSaysOfWhenToSendItAgain(t *testing.T) {
	for _, status := range []struct {
		code int
		is   func(error) bool
	}{
		{http.StatusTooManyRequests, apierrors.IsTooManyRequests},
		{http.StatusServiceUnavailable, apierrors.IsServiceUnavailable},
	} {
		for _, retryAfter := range []string{"0", "1"} {
			for _, read := range everyRead() {
				t.Run(read.name+", "+strconv.Itoa(status.code)+" after "+retryAfter, func(t *testing.T) {
					var sent atomic.Int32
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
						sent.Add(1)
						w.Header().Set("Retry-After", retryAfter)
						w.WriteHeader(status.code)
					}))
					t.Cleanup(server.Close)
					r := &NodesReconciler{Timeout: time.Minute, MaxNodes: 5000}
					tenant, err := r.tenant(types.NamespacedName{Namespace: "team-a", Name: "dev"}, kubeconfigFor(t, server.URL))
					if err != nil {
						t.Fatal(err)
					}
					stop, err := read.do(t.Context(), tenant, r)
					if !status.is(err) || stop != "" {
						t.Errorf("the read returned stop %q and error %v, want the error of the status", stop, err)
					}
					if n := sent.Load(); n != 1 {
						t.Errorf("the read sent %d requests, want 1", n)
					}
				})
			}
		}
	}
}

// answerOnTCP answers every request on a connection of its own with head and
// body, and counts the requests.
func answerOnTCP(t *testing.T, head, body string) (url string, requests *atomic.Int32) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	requests = new(atomic.Int32)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer func() { _ = conn.Close() }()
				reader := bufio.NewReader(conn)
				for {
					request, err := http.ReadRequest(reader)
					if err != nil {
						return
					}
					requests.Add(1)
					_ = request.Body.Close()
					if _, err := conn.Write([]byte(head + body)); err != nil {
						return
					}
				}
			}()
		}
	}()
	return "http://" + listener.Addr().String(), requests
}

// headOf is the status line and headers of an answer with body, padded by one
// header to exactly size bytes.
func headOf(size int, body string) string {
	start := "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: " + strconv.Itoa(len(body)) + "\r\nX-Pad: "
	const end = "\r\n\r\n"
	return start + strings.Repeat("a", size-len(start)-len(end)) + end
}

func TestTheHeadersOfAResponseFailTheReadOnceTheyAreOneBytePastTheBound(t *testing.T) {
	const body = `{"metadata":{},"items":[]}`
	for _, c := range []struct {
		name string
		head int
		fail bool
	}{
		{"a head of the bound", maxResponseHeaderBytes, false},
		{"a head of the bound less one", maxResponseHeaderBytes - 1, false},
		{"a head one byte past the bound", maxResponseHeaderBytes + 1, true},
		{"a head of 10 MiB", 10 << 20, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			head := headOf(c.head, body)
			if len(head) != c.head {
				t.Fatalf("the head has %d bytes, want %d", len(head), c.head)
			}
			url, requests := answerOnTCP(t, head, body)
			r := &NodesReconciler{Timeout: time.Minute, MaxNodes: 5000}
			tenant, err := r.tenant(types.NamespacedName{Namespace: "team-a", Name: "dev"}, kubeconfigFor(t, url))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tenant.leasePage(t.Context(), metav1.ListOptions{Limit: leasePage}); (err != nil) != c.fail {
				t.Errorf("a head of %d bytes returned %v, want a failure: %t", c.head, err, c.fail)
			} else if err != nil && !strings.Contains(err.Error(), "exceeded "+strconv.Itoa(maxResponseHeaderBytes)+" bytes") {
				t.Errorf("the error %q does not name the bound %d", err, maxResponseHeaderBytes)
			}
			if n := requests.Load(); n != 1 {
				t.Errorf("the read sent %d requests, want 1", n)
			}
		})
	}
}

func TestTheHeadersOfAnHTTP2ResponseFailTheReadPastTheBound(t *testing.T) {
	for _, c := range []struct {
		name string
		pad  int
		fail bool
	}{
		{"headers of a quarter of the bound", maxResponseHeaderBytes / 4, false},
		{"headers of twice the bound", 2 * maxResponseHeaderBytes, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			pad := strings.Repeat("a", c.pad)
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if req.ProtoMajor != 2 {
					t.Errorf("the request came over HTTP/%d", req.ProtoMajor)
				}
				w.Header().Set("X-Pad", pad)
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"metadata":{},"items":[]}`))
			}))
			server.EnableHTTP2 = true
			server.StartTLS()
			t.Cleanup(server.Close)
			kubeconfig, err := clientcmd.Write(clientcmdapi.Config{
				Clusters:       map[string]*clientcmdapi.Cluster{"tenant": {Server: server.URL, CertificateAuthorityData: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}},
				AuthInfos:      map[string]*clientcmdapi.AuthInfo{"lodia": {}},
				Contexts:       map[string]*clientcmdapi.Context{"tenant": {Cluster: "tenant", AuthInfo: "lodia"}},
				CurrentContext: "tenant",
			})
			if err != nil {
				t.Fatal(err)
			}
			r := &NodesReconciler{Timeout: time.Minute, MaxNodes: 5000}
			tenant, err := r.tenant(types.NamespacedName{Namespace: "team-a", Name: "dev"}, kubeconfig)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tenant.leasePage(t.Context(), metav1.ListOptions{Limit: leasePage}); (err != nil) != c.fail {
				t.Errorf("headers of %d bytes returned %v, want a failure: %t", c.pad, err, c.fail)
			}
		})
	}
}
