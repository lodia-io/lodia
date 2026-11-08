// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/certs"
	"go.lodia.io/lodia/internal/controller/health"
)

type readyz struct {
	code atomic.Int32
	hang atomic.Bool
}

func (s *readyz) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if s.hang.Load() {
		<-r.Context().Done()
		return
	}
	if r.URL.Path != "/readyz" {
		http.NotFound(w, r)
		return
	}
	w.WriteHeader(int(s.code.Load()))
}

func unstartedReadyz(t *testing.T, ca *certs.Certificate, handler http.Handler, profile certs.Profile) *httptest.Server {
	t.Helper()
	profile.CommonName, profile.Usages = "kube-apiserver", []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
	leaf, err := ca.Issue(profile, time.Now(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	certPEM, err := certs.CertPEM(leaf.Cert)
	if err != nil {
		t.Fatal(err)
	}
	keyPEM, err := certs.KeyPEM(leaf.Key)
	if err != nil {
		t.Fatal(err)
	}
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	s := httptest.NewUnstartedServer(handler)
	s.TLS = &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12}
	return s
}

type request struct {
	method, host, path, authorization, serverName string
	clientCertificates                            int
}

type service struct {
	*httptest.Server
	readyz
	opened atomic.Int32
	closed chan struct{}
	mu     sync.Mutex
	seen   []request
}

func (s *service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.seen = append(s.seen, request{r.Method, r.Host, r.URL.Path, r.Header.Get("Authorization"), r.TLS.ServerName, len(r.TLS.PeerCertificates)})
	s.mu.Unlock()
	s.readyz.ServeHTTP(w, r)
}

func (s *service) requests() []request {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]request(nil), s.seen...)
}

var services sync.Map

func internalHost(cp *lodiav1alpha1.ControlPlane) string {
	return cp.Name + "-kube-apiserver." + cp.Namespace + ".svc:443"
}

func internalURL(cp *lodiav1alpha1.ControlPlane) string {
	return "https://" + internalHost(cp) + "/readyz"
}

func serveService(t *testing.T, cp *lodiav1alpha1.ControlPlane, ca *certs.Certificate) *service {
	t.Helper()
	host, _, err := net.SplitHostPort(internalHost(cp))
	if err != nil {
		t.Fatal(err)
	}
	s := &service{closed: make(chan struct{}, 16)}
	s.code.Store(http.StatusOK)
	s.Server = unstartedReadyz(t, ca, s, certs.Profile{DNSNames: []string{host}})
	s.TLS.ClientAuth = tls.RequestClientCert
	s.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			s.opened.Add(1)
		}
		if state == http.StateClosed {
			select {
			case s.closed <- struct{}{}:
			default:
			}
		}
	}
	s.StartTLS()
	t.Cleanup(s.Close)
	services.Store(internalHost(cp), s.Listener.Addr().String())
	t.Cleanup(func() { services.Delete(internalHost(cp)) })
	return s
}

func dialService(ctx context.Context, network, address string) (net.Conn, error) {
	if to, ok := services.Load(address); ok {
		address = to.(string)
	} else if host, _, _ := net.SplitHostPort(address); strings.HasSuffix(host, ".svc") {
		return nil, &net.DNSError{Err: "no such host", Name: host, IsNotFound: true}
	}
	return (&net.Dialer{}).DialContext(ctx, network, address)
}

func dialVia(redirects map[string]string) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		if to, ok := redirects[address]; ok {
			return (&net.Dialer{}).DialContext(ctx, network, to)
		}
		return dialService(ctx, network, address)
	}
}

func blackHole(ctx context.Context, _, _ string) (net.Conn, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func newReconciler(timeout time.Duration) *health.Reconciler {
	return &health.Reconciler{Client: k8s, Reader: k8s, FailuresBeforeFalse: failuresBeforeFalse, DialContext: dialService, Period: time.Hour, Timeout: timeout, Clock: clock.RealClock{}}
}

func settled(t *testing.T, before int) {
	t.Helper()
	var now int
	err := wait.PollUntilContextTimeout(t.Context(), 50*time.Millisecond, 10*time.Second, true, func(context.Context) (bool, error) {
		now = runtime.NumGoroutine()
		return now <= before, nil
	})
	if err != nil {
		t.Errorf("%d goroutines 10s after the pass, %d before it", now, before)
	}
}

type paths struct {
	cp       *lodiav1alpha1.ControlPlane
	api      *endpoint
	prefix   string
	answered string
}

var modes = []struct {
	name  string
	start func(*testing.T) *paths
}{
	{"nodePort", func(t *testing.T) *paths {
		cp := controlPlaneWith(t, lodiav1alpha1.Endpoint{NodePort: &lodiav1alpha1.NodePortEndpoint{}})
		e := serve(t)
		writeBundle(t, cp, e)
		publish(t, cp, e)
		p := &paths{cp: cp, api: e, answered: e.url + " answered 200"}
		eventually(t, func() error { return expect(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReachable, p.answered) })
		return p
	}},
	{"gateway", func(t *testing.T) *paths {
		f := newGateway(t)
		f.publishWorking(t)
		eventually(t, func() error {
			return expect(t, f.cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReachable, f.allAnswered())
		})
		return &paths{cp: f.cp, api: f.api, prefix: "api: ", answered: f.allAnswered()}
	}},
}

func TestTheReasonNamesTheInternalPathWhenItFailed(t *testing.T) {
	for _, m := range modes {
		t.Run(m.name, func(t *testing.T) {
			for _, c := range []struct {
				name               string
				internal, external bool
				reason             string
				message            func(*paths) string
			}{
				{"the Service alone", true, false, lodiav1alpha1.ReasonInternalProbeFailed, func(p *paths) string {
					return "internal: " + internalURL(p.cp) + " answered 503"
				}},
				{"both", true, true, lodiav1alpha1.ReasonInternalProbeFailed, func(p *paths) string {
					return "internal: " + internalURL(p.cp) + " answered 503; " + p.prefix + p.api.url + " answered 503"
				}},
				{"the public endpoint alone", false, true, lodiav1alpha1.ReasonExternalProbeFailed, func(p *paths) string {
					return p.prefix + p.api.url + " answered 503"
				}},
			} {
				t.Run(c.name, func(t *testing.T) {
					p := m.start(t)
					if c.internal {
						p.api.service.code.Store(http.StatusServiceUnavailable)
					}
					if c.external {
						p.api.code.Store(http.StatusServiceUnavailable)
					}
					probe := newReconciler(time.Second)
					for failed := 1; failed < failuresBeforeFalse; failed++ {
						reconcile(t, probe, p.cp)
						if err := expect(t, p.cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReachable, p.answered); err != nil {
							t.Fatalf("after %d failed passes: %v", failed, err)
						}
					}
					reconcile(t, probe, p.cp)
					if err := expect(t, p.cp, metav1.ConditionFalse, c.reason, c.message(p)); err != nil {
						t.Fatal(err)
					}
				})
			}
		})
	}
}

func TestFailuresOnEitherPathCountTogether(t *testing.T) {
	for _, m := range modes {
		t.Run(m.name, func(t *testing.T) {
			p := m.start(t)
			probe := newReconciler(time.Second)
			p.api.code.Store(http.StatusServiceUnavailable)
			for failed := 1; failed < failuresBeforeFalse; failed++ {
				reconcile(t, probe, p.cp)
				if err := expect(t, p.cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReachable, p.answered); err != nil {
					t.Fatalf("after %d failed passes of the public endpoint: %v", failed, err)
				}
			}
			p.api.code.Store(http.StatusOK)
			p.api.service.code.Store(http.StatusServiceUnavailable)
			reconcile(t, probe, p.cp)
			if err := expect(t, p.cp, metav1.ConditionFalse, lodiav1alpha1.ReasonInternalProbeFailed, "internal: "+internalURL(p.cp)+" answered 503"); err != nil {
				t.Fatalf("a failed pass of the Service after %d of the public endpoint: %v", failuresBeforeFalse-1, err)
			}

			p.api.service.code.Store(http.StatusOK)
			reconcile(t, probe, p.cp)
			if err := expect(t, p.cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReachable, p.answered); err != nil {
				t.Fatal(err)
			}
			p.api.code.Store(http.StatusServiceUnavailable)
			for failed := 1; failed < failuresBeforeFalse; failed++ {
				reconcile(t, probe, p.cp)
				if err := expect(t, p.cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReachable, p.answered); err != nil {
					t.Fatalf("after %d failed passes following a pass in which every path answered: %v", failed, err)
				}
			}
		})
	}
}

func TestTheMessageNamesEveryFailedPathAndTheInternalOneFirst(t *testing.T) {
	f := newGateway(t)
	f.publishWorking(t)
	eventually(t, func() error {
		return expect(t, f.cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReachable, f.allAnswered())
	})
	f.api.code.Store(http.StatusInternalServerError)
	f.api.service.code.Store(http.StatusInternalServerError)
	f.tunnel.code.Store(http.StatusBadGateway)
	konnectivity := deadAddress(t)
	publishGateway(t, f.cp, f.apiAddress(), konnectivity, f.tunnel.address)
	probe := newReconciler(500 * time.Millisecond)
	for range failuresBeforeFalse {
		reconcile(t, probe, f.cp)
	}
	want := "internal: " + internalURL(f.cp) + " answered 500; api: " + f.api.url + " answered 500; tunnel: " + f.api.url + " through " + f.tunnel.address +
		" answered 502; konnectivity: " + konnectivity + " did not complete a TLS handshake"
	if err := expect(t, f.cp, metav1.ConditionFalse, lodiav1alpha1.ReasonInternalProbeFailed, want); err != nil {
		t.Fatal(err)
	}
}

func TestTheInternalProbeVerifiesTheBundleAndSendsNoCredentials(t *testing.T) {
	p := modes[0].start(t)
	probe := newReconciler(time.Second)
	reconcile(t, probe, p.cp)
	seen := p.api.service.requests()
	if len(seen) == 0 {
		t.Fatal("the Service saw no request")
	}
	host, _, _ := net.SplitHostPort(internalHost(p.cp))
	for _, r := range seen {
		if want := (request{method: http.MethodGet, host: internalHost(p.cp), path: "/readyz", serverName: host}); r != want {
			t.Errorf("the Service saw %+v, want %+v: no credentials, the name it is reached by", r, want)
		}
	}

	other, err := certs.NewCA("other-ca", time.Now(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	p.api.service = serveService(t, p.cp, other)
	for range failuresBeforeFalse {
		reconcile(t, probe, p.cp)
	}
	if err := expect(t, p.cp, metav1.ConditionFalse, lodiav1alpha1.ReasonInternalProbeFailed, "internal: "+internalURL(p.cp)+" did not answer"); err != nil {
		t.Fatal(err)
	}
}

func TestARotatedBundleReachesTheInternalProbe(t *testing.T) {
	p := modes[0].start(t)
	other, err := certs.NewCA("rotated-ca", time.Now(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	p.api.service = serveService(t, p.cp, other)
	probe := newReconciler(time.Second)
	for range failuresBeforeFalse {
		reconcile(t, probe, p.cp)
	}
	if err := expect(t, p.cp, metav1.ConditionFalse, lodiav1alpha1.ReasonInternalProbeFailed, "internal: "+internalURL(p.cp)+" did not answer"); err != nil {
		t.Fatal(err)
	}

	rotated, err := certs.CertPEM(other.Cert)
	if err != nil {
		t.Fatal(err)
	}
	bundle := &corev1.Secret{}
	if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: p.cp.Namespace, Name: lodiav1alpha1.CABundleSecret.Name(p.cp.Name)}, bundle); err != nil {
		t.Fatal(err)
	}
	bundle.Data[lodiav1alpha1.CACertKey] = append(bundle.Data[lodiav1alpha1.CACertKey], rotated...)
	if err := k8s.Update(t.Context(), bundle); err != nil {
		t.Fatal(err)
	}
	reconcile(t, probe, p.cp)
	if err := expect(t, p.cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReachable, p.answered); err != nil {
		t.Fatalf("after the bundle trusted the Service's CA: %v", err)
	}
}

func TestEveryInternalProbeDialsAnewAndLeavesNoConnectionOpen(t *testing.T) {
	const passes = 3
	p := modes[0].start(t)
	counted := serveService(t, p.cp, p.api.ca)
	probe := newReconciler(5 * time.Second)
	for range passes {
		reconcile(t, probe, p.cp)
	}
	if got := counted.opened.Load(); got != passes {
		t.Errorf("%d passes opened %d connections to the Service, want one each", passes, got)
	}
	for done := range passes {
		select {
		case <-counted.closed:
		case <-time.After(10 * time.Second):
			t.Fatalf("%d of %d connections to the Service closed within 10s of the passes", done, passes)
		}
	}
}

func pointedAt(t *testing.T, address string) client.WithWatch {
	t.Helper()
	at := addressOf(t, address)
	return interceptor.NewClient(k8s, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if err := c.Get(ctx, key, obj, opts...); err != nil {
				return err
			}
			if cp, ok := obj.(*lodiav1alpha1.ControlPlane); ok {
				cp.Status.Endpoint.APIServer = at
			}
			return nil
		},
	})
}

func TestAServerThatAcceptsAndNeverAnswersEndsWithinTimeoutInEveryMode(t *testing.T) {
	const timeout = time.Second
	for name, endpoint := range map[string]lodiav1alpha1.Endpoint{
		"loadBalancer": {LoadBalancer: &lodiav1alpha1.LoadBalancerEndpoint{}},
		"nodePort":     {NodePort: &lodiav1alpha1.NodePortEndpoint{}},
	} {
		t.Run(name, func(t *testing.T) {
			cp := controlPlaneWith(t, endpoint)
			e := serve(t)
			writeBundle(t, cp, e)
			publish(t, cp, e)
			eventually(t, func() error {
				return expect(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReachable, e.url+" answered 200")
			})
			stalled, closed := stallListener(t)
			probe := newReconciler(timeout)
			probe.Client = pointedAt(t, stalled)
			before := runtime.NumGoroutine()
			start := time.Now()
			reconcile(t, probe, cp)
			if took := time.Since(start); took < timeout || took >= 2*timeout {
				t.Errorf("a pass over a server that never answers took %s, want one Timeout of %s", took, timeout)
			}
			select {
			case <-closed:
			case <-time.After(3 * timeout):
				t.Fatalf("the probe left the stalled connection open longer than %s", 3*timeout)
			}
			settled(t, before)
		})
	}
}

func TestAServerThatAcceptsAndNeverAnswersAtTheServiceEndsWithinTimeout(t *testing.T) {
	const timeout = time.Second
	for _, c := range []struct {
		name   string
		public bool
	}{
		{"the Service", false},
		{"the Service and the public endpoint", true},
	} {
		t.Run(c.name, func(t *testing.T) {
			p := modes[0].start(t)
			stalled, closed := stallListener(t)
			probe := newReconciler(timeout)
			probe.DialContext = dialVia(map[string]string{internalHost(p.cp): stalled})
			stalls := 1
			if c.public {
				probe.Client = pointedAt(t, stalled)
				stalls++
			}
			before := runtime.NumGoroutine()
			start := time.Now()
			reconcile(t, probe, p.cp)
			if took := time.Since(start); took < timeout || took >= 2*timeout {
				t.Errorf("a pass over servers that never answer took %s, want one Timeout of %s", took, timeout)
			}
			for range stalls {
				select {
				case <-closed:
				case <-time.After(3 * timeout):
					t.Fatalf("the probe left a stalled connection open longer than %s", 3*timeout)
				}
			}
			settled(t, before)
		})
	}
}

func TestAPassWithEveryPathBlackHoledCostsOneTimeout(t *testing.T) {
	const timeout = time.Second
	p := modes[0].start(t)
	probe := newReconciler(timeout)
	probe.DialContext = blackHole
	before := runtime.NumGoroutine()
	start := time.Now()
	reconcile(t, probe, p.cp)
	if took := time.Since(start); took < timeout || took >= 2*timeout {
		t.Errorf("a pass over paths nothing answers took %s, want one Timeout of %s", took, timeout)
	}
	settled(t, before)
}
