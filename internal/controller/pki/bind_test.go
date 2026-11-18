// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package pki_test

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/controller/pki"
)

func TestATenantThatNeverAnswersEndsTheBindWithinTenSecondsAndKeepsTheStatusWritable(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var held []net.Conn
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			held = append(held, conn)
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		<-done
		for _, conn := range held {
			_ = conn.Close()
		}
	})
	r := newReconciler(time.Hour)
	r.Dial = func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, listener.Addr().String())
	}
	cp := controlPlane(t, "pki-bind-silent")
	publishEndpoint(t, cp)
	reconcile(t, r, cp)
	setAPIServerReady(t, cp)

	start := time.Now()
	_, err = r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)})
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("a pass whose tenant answered nothing succeeded")
	}
	if elapsed < 9*time.Second || elapsed > 12*time.Second {
		t.Errorf("the bind took %s, want the bindTimeout of 10 s", elapsed)
	}
	expectCondition(t, cp, lodiav1alpha1.ConditionAccessReady, metav1.ConditionFalse, lodiav1alpha1.ReasonBinding)
}

type sent struct {
	method, path, manager, force, identity string
}

type tenantStandIn struct {
	open     atomic.Int32
	mu       sync.Mutex
	requests []sent
	held     []net.Conn
	dial     func(ctx context.Context, network, address string) (net.Conn, error)
}

type holdsTheFirst struct {
	net.Listener
	stand *tenantStandIn
	first atomic.Bool
}

func (l *holdsTheFirst) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	if l.first.CompareAndSwap(false, true) {
		l.stand.mu.Lock()
		l.stand.held = append(l.stand.held, conn)
		l.stand.mu.Unlock()
		return l.Accept()
	}
	return conn, nil
}

const forbidden = `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Forbidden","code":403,"message":"forbidden"}`

func standingInForTheTenant(t *testing.T, cp *lodiav1alpha1.ControlPlane, holdFirstHandshake bool, handler func(n int, w http.ResponseWriter, r *http.Request)) *tenantStandIn {
	t.Helper()
	serving := secretOf(t, cp, "dev-apiserver-serving")
	pair, err := tls.X509KeyPair(serving.Data[lodiav1alpha1.TLSCertKey], serving.Data[lodiav1alpha1.TLSPrivateKeyKey])
	if err != nil {
		t.Fatal(err)
	}
	stand := &tenantStandIn{}
	var count atomic.Int32
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		identity := ""
		if r.TLS != nil && len(r.TLS.PeerCertificates) > 0 {
			identity = r.TLS.PeerCertificates[0].Subject.CommonName
		}
		stand.mu.Lock()
		stand.requests = append(stand.requests, sent{r.Method, r.URL.Path, r.URL.Query().Get("fieldManager"), r.URL.Query().Get("force"), identity})
		stand.mu.Unlock()
		_, _ = io.Copy(io.Discard, r.Body)
		handler(int(count.Add(1)), w, r)
	}))
	if holdFirstHandshake {
		server.Listener = &holdsTheFirst{Listener: server.Listener, stand: stand}
	}
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		switch state {
		case http.StateNew:
			stand.open.Add(1)
		case http.StateClosed, http.StateHijacked:
			stand.open.Add(-1)
		default:
		}
	}
	server.TLS = &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12, ClientAuth: tls.RequestClientCert}
	server.StartTLS()
	t.Cleanup(func() {
		server.CloseClientConnections()
		server.Close()
		stand.mu.Lock()
		defer stand.mu.Unlock()
		for _, conn := range stand.held {
			_ = conn.Close()
		}
	})
	address := server.Listener.Addr().String()
	stand.dial = func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, address)
	}
	return stand
}

func answers(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{}`))
}

func bindingOf(t *testing.T, namespace string, reconciler *pki.Reconciler) (*lodiav1alpha1.ControlPlane, ctrl.Request) {
	t.Helper()
	cp := controlPlane(t, namespace)
	publishEndpoint(t, cp)
	reconcile(t, reconciler, cp)
	setAPIServerReady(t, cp)
	return cp, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}
}

func TestATenantThatCompletesTLSAndNeverAnswersEndsTheBindWithinTenSeconds(t *testing.T) {
	r := newReconciler(time.Hour)
	cp, request := bindingOf(t, "pki-bind-no-answer", r)
	r.Dial = standingInForTheTenant(t, cp, false, func(_ int, _ http.ResponseWriter, req *http.Request) { <-req.Context().Done() }).dial
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	start := time.Now()
	_, err := r.Reconcile(ctx, request)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("a pass whose tenant answered nothing succeeded")
	}
	if elapsed < 9*time.Second || elapsed > 12*time.Second {
		t.Errorf("the bind took %s, want the bindTimeout of 10 s", elapsed)
	}
	expectCondition(t, cp, lodiav1alpha1.ConditionAccessReady, metav1.ConditionFalse, lodiav1alpha1.ReasonBinding)
}

func TestAHandshakeHeldPastTheBindTimeoutGivesItsPlaceBackAndTheNextBindDialsAfterwards(t *testing.T) {
	r := newReconciler(time.Hour)
	cp, request := bindingOf(t, "pki-bind-handshake", r)
	stand := standingInForTheTenant(t, cp, true, func(_ int, w http.ResponseWriter, req *http.Request) { answers(w, req) })
	r.Dial = stand.dial
	start := time.Now()
	if _, err := r.Reconcile(t.Context(), request); err == nil {
		t.Fatal("a pass whose tenant held its handshake succeeded")
	}
	if elapsed := time.Since(start); elapsed < 9*time.Second || elapsed > 12*time.Second {
		t.Errorf("the bind took %s, want the bindTimeout of 10 s", elapsed)
	}
	start = time.Now()
	if _, err := r.Reconcile(t.Context(), request); err != nil {
		t.Fatalf("the bind behind a handshake that ended at its timeout: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("the bind behind it took %s, want a dial of its own as soon as the handshake's place is given back", elapsed)
	}
	expectCondition(t, cp, lodiav1alpha1.ConditionAccessReady, metav1.ConditionTrue, lodiav1alpha1.ReasonBound)
}

func TestTheBindThroughTheBootstrapEndsWithinTwentySeconds(t *testing.T) {
	r := newReconciler(time.Hour)
	cp, request := bindingOf(t, "pki-bind-bootstrap-silent", r)
	r.Dial = standingInForTheTenant(t, cp, false, func(n int, w http.ResponseWriter, req *http.Request) {
		if n > 1 {
			<-req.Context().Done()
			return
		}
		select {
		case <-time.After(9 * time.Second):
		case <-req.Context().Done():
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(forbidden))
	}).dial
	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Second)
	defer cancel()
	start := time.Now()
	_, err := r.Reconcile(ctx, request)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("a pass whose tenant forbids lodia:operator and then answers nothing succeeded")
	}
	if elapsed < 19*time.Second || elapsed > 22*time.Second {
		t.Errorf("the bind took %s, want the 9 s of lodia:operator's answer and the bindTimeout of 10 s of the bootstrap's", elapsed)
	}
}

func TestTheBindThroughTheBootstrapAppliesAsLodiaPKIForcingAndClosesItsConnection(t *testing.T) {
	r := newReconciler(time.Hour)
	cp, request := bindingOf(t, "pki-bind-bootstrap", r)
	stand := standingInForTheTenant(t, cp, false, func(n int, w http.ResponseWriter, req *http.Request) {
		if n == 1 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(forbidden))
			return
		}
		answers(w, req)
	})
	r.Dial = stand.dial
	if _, err := r.Reconcile(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	expectCondition(t, cp, lodiav1alpha1.ConditionAccessReady, metav1.ConditionTrue, lodiav1alpha1.ReasonBound)
	stand.mu.Lock()
	requests := slices.Clone(stand.requests)
	stand.mu.Unlock()
	patch := func(binding, identity string) sent {
		return sent{http.MethodPatch, "/apis/rbac.authorization.k8s.io/v1/clusterrolebindings/" + binding, "lodia/pki", "true", identity}
	}
	want := []sent{
		patch("lodia:operator", "lodia:operator"),
		patch("lodia:operator", "lodia:bootstrap"), patch("lodia:cluster-admins", "lodia:bootstrap"), patch("lodia:kubelet-api-admins", "lodia:bootstrap"),
	}
	if !slices.Equal(requests, want) {
		t.Errorf("the tenant got %+v, want %+v", requests, want)
	}
	waitFor(t, 5*time.Second, func() bool { return stand.open.Load() == 1 })
}

func TestAControlPlaneThatIsGoneLeavesNoClientAndNoConnectionOfPKI(t *testing.T) {
	r := newReconciler(time.Hour)
	cp, request := bindingOf(t, "pki-bind-forgotten", r)
	stand := standingInForTheTenant(t, cp, false, func(_ int, w http.ResponseWriter, req *http.Request) { answers(w, req) })
	r.Dial = stand.dial
	if _, err := r.Reconcile(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if got := stand.open.Load(); got != 1 {
		t.Fatalf("the bind left %d connections open, want its kept client's one", got)
	}
	if err := k8s.Delete(t.Context(), cp); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 10*time.Second, func() bool {
		if _, err := r.Reconcile(t.Context(), request); err != nil {
			t.Fatal(err)
		}
		return stand.open.Load() == 0
	})
}
