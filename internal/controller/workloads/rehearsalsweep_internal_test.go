// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package workloads

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"encoding/pem"
	"errors"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"k8s.io/utils/clock"
	clocktesting "k8s.io/utils/clock/testing"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/tenantclient"
)

const (
	coreConfigMaps = `{"kind":"APIGroupDiscoveryList","apiVersion":"apidiscovery.k8s.io/v2","metadata":{},"items":[{"metadata":{},"versions":[{"version":"v1","resources":[{"resource":"configmaps","scope":"Namespaced","verbs":["get","list"]}]}]}]}`
	noGroups       = `{"kind":"APIGroupDiscoveryList","apiVersion":"apidiscovery.k8s.io/v2","metadata":{},"items":[]}`
	emptyList      = `{"kind":"List","metadata":{},"items":[]}`
	configMapsPath = "/api/v1/configmaps"
)

type answer func(w http.ResponseWriter, r *http.Request)

type side struct {
	mu      sync.Mutex
	answers map[string][]answer
	reads   []string
}

func (s *side) serveHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.reads = append(s.reads, r.URL.RequestURI())
	queue := s.answers[r.URL.Path]
	var next answer
	switch {
	case len(queue) > 1:
		next, s.answers[r.URL.Path] = queue[0], queue[1:]
	case len(queue) == 1:
		next = queue[0]
	}
	s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	if next == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	next(w, r)
}

func (s *side) read() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.reads)
}

func stub(t *testing.T, answers map[string][]answer) (*tenantclient.Client, *side) {
	t.Helper()
	s := &side{answers: map[string][]answer{
		"/api":  {body(coreConfigMaps)},
		"/apis": {body(noGroups)},
		"/apis/apiregistration.k8s.io/v1/apiservices":             {body(emptyList)},
		"/apis/apiextensions.k8s.io/v1/customresourcedefinitions": {body(emptyList)},
		configMapsPath: {rows(2, "")},
	}}
	maps.Copy(s.answers, answers)
	server := httptest.NewTLSServer(http.HandlerFunc(s.serveHTTP))
	t.Cleanup(server.Close)
	cfg := &rest.Config{Host: server.URL, TLSClientConfig: rest.TLSClientConfig{CAData: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}}
	c, err := tenantclient.New(cfg, tenantclient.Options{Connections: 1, Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c, s
}

func body(b string) answer {
	return func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(b)) }
}

func rows(n int, next string) answer {
	return func(w http.ResponseWriter, _ *http.Request) {
		cells := strings.TrimSuffix(strings.Repeat(`{"cells":["a"]},`, n), ",")
		_, _ = w.Write([]byte(`{"kind":"Table","apiVersion":"meta.k8s.io/v1","metadata":{"continue":"` + next + `"},"columnDefinitions":[],"rows":[` + cells + `]}`))
	}
}

func status(code int, reason, message string) answer {
	return func(w http.ResponseWriter, _ *http.Request) {
		if code == http.StatusTooManyRequests {
			w.Header().Set("Retry-After", "1")
		}
		w.WriteHeader(code)
		_, _ = w.Write([]byte(`{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","message":"` + message + `","reason":"` + reason + `","code":` + strconv.Itoa(code) + `}`))
	}
}

func dropped(w http.ResponseWriter, _ *http.Request) {
	conn, _, err := w.(http.Hijacker).Hijack()
	if err == nil {
		_ = conn.Close()
	}
}

func cutMidBody(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"kind":"Table","apiVersion":"meta.k8s.io/v1","metadata":{},"columnDefinitions":[],"rows":[`))
	w.(http.Flusher).Flush()
	panic(http.ErrAbortHandler)
}

func resetMidBody(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"kind":"Table","apiVersion":"meta.k8s.io/v1","metadata":{},"columnDefinitions":[],"rows":[`))
	w.(http.Flusher).Flush()
	conn, _, err := w.(http.Hijacker).Hijack()
	if err != nil {
		return
	}
	tcp := conn.(*tls.Conn).NetConn().(*net.TCPConn)
	_ = tcp.SetLinger(0)
	_ = tcp.Close()
}

var undecodable = status(http.StatusInternalServerError, "InternalError", "proto: illegal wireType 6")

func transients() map[string][]answer {
	return map[string][]answer{
		"429":           {status(http.StatusTooManyRequests, "TooManyRequests", "too many requests")},
		"503":           {status(http.StatusServiceUnavailable, "ServiceUnavailable", "apiserver is shutting down")},
		"504":           {status(http.StatusGatewayTimeout, "Timeout", "request did not complete within the allotted timeout")},
		"ServerTimeout": {status(http.StatusInternalServerError, "ServerTimeout", "the server cannot complete the requested operation at this time, try again later")},
		"no answer":     {dropped, dropped},
		"a body cut":    {cutMidBody},
		"a body reset":  {resetMidBody},
		"a 502 that is no Status with Retry-After": {func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/plain")
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte("upstream is restarting"))
		}},
	}
}

func sweptAll(t *testing.T, s *sweep) (again int) {
	t.Helper()
	for range 20 {
		switch err := s.run(t.Context()); {
		case err == nil:
			return again
		case errors.Is(err, errAgain):
			again++
		default:
			t.Fatal(err)
		}
	}
	t.Fatal("the sweep did not end")
	return again
}

func TestATransientErrorThroughTheCopyIsReadAgain(t *testing.T) {
	for name, once := range transients() {
		t.Run(name, func(t *testing.T) {
			copyClient, _ := stub(t, map[string][]answer{configMapsPath: append(once, rows(2, ""))})
			tenant, _ := stub(t, nil)
			s := &sweep{copy: copyClient, tenant: tenant}
			if again := sweptAll(t, s); again != 1 || len(s.stopping) > 0 || len(s.failed) > 0 || s.objects != 2 {
				t.Errorf("the sweep sent %d reads again and ends with %d objects, failed %v, stopping %q, want 1, 2 and nothing", again, s.objects, s.failed, s.stopping)
			}
		})
	}
}

func TestAPageWithNoAnswerInTimeIsReadAgain(t *testing.T) {
	t.Parallel()
	late := func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(readTimeout + 5*time.Second):
		case <-r.Context().Done():
		}
	}
	copyClient, _ := stub(t, map[string][]answer{configMapsPath: {late, rows(2, "")}})
	tenant, _ := stub(t, nil)
	s := &sweep{copy: copyClient, tenant: tenant}
	if again := sweptAll(t, s); again != 1 || len(s.stopping) > 0 || len(s.failed) > 0 || s.objects != 2 {
		t.Errorf("the sweep sent %d reads again and ends with %d objects, failed %v, stopping %q, want 1, 2 and nothing", again, s.objects, s.failed, s.stopping)
	}
}

func TestATransientErrorThroughEithersDiscoveryIsReadAgain(t *testing.T) {
	for name, once := range map[string]answer{
		"503":          status(http.StatusServiceUnavailable, "ServiceUnavailable", "apiserver is shutting down"),
		"a body cut":   cutMidBody,
		"a body reset": resetMidBody,
	} {
		for _, side := range []string{"copy", "tenant"} {
			t.Run(side+" "+name, func(t *testing.T) {
				discovery := map[string][]answer{"/api": {once, body(coreConfigMaps)}}
				copyAnswers, tenantAnswers := discovery, map[string][]answer(nil)
				if side == "tenant" {
					copyAnswers, tenantAnswers = nil, discovery
				}
				copyClient, _ := stub(t, copyAnswers)
				tenant, _ := stub(t, tenantAnswers)
				s := &sweep{copy: copyClient, tenant: tenant}
				if again := sweptAll(t, s); again != 1 || len(s.stopping) > 0 || s.objects != 2 {
					t.Errorf("the sweep sent %d reads again and ends with %d objects, stopping %q, want 1, 2 and nothing", again, s.objects, s.stopping)
				}
			})
		}
	}
}

func TestAnErrorThroughTheCopyThatIsNotTransientIsAFinding(t *testing.T) {
	copyClient, _ := stub(t, map[string][]answer{configMapsPath: {undecodable}})
	tenant, _ := stub(t, nil)
	s := &sweep{copy: copyClient, tenant: tenant}
	if again := sweptAll(t, s); again != 0 || !slices.Equal(s.stopping, []string{"configmaps of v1 lists through the tenant and not through the copy: proto: illegal wireType 6"}) {
		t.Errorf("the sweep sent %d reads again and stops on %q", again, s.stopping)
	}
}

func TestATransientErrorThroughTheTenantIsReadAgainAndTheCopysFailureStops(t *testing.T) {
	for name, once := range transients() {
		t.Run(name, func(t *testing.T) {
			copyClient, _ := stub(t, map[string][]answer{configMapsPath: {undecodable}})
			tenant, _ := stub(t, map[string][]answer{configMapsPath: append(once, rows(2, ""))})
			s := &sweep{copy: copyClient, tenant: tenant}
			if again := sweptAll(t, s); again != 1 || len(s.reported) > 0 || !slices.Equal(s.stopping, []string{"configmaps of v1 lists through the tenant and not through the copy: proto: illegal wireType 6"}) {
				t.Errorf("the sweep sent %d reads again, stops on %q and reports %q, want 1 and the copy's failure stopping", again, s.stopping, s.reported)
			}
		})
	}
}

func TestATenantsExpiredTokenListsItAgainFromItsStart(t *testing.T) {
	copyClient, _ := stub(t, map[string][]answer{configMapsPath: {undecodable}})
	tenant, reads := stub(t, map[string][]answer{configMapsPath: {rows(500, "t1"), status(http.StatusGone, "Expired", "the provided continue parameter is too old"), rows(500, "t2"), rows(1, "")}})
	s := &sweep{copy: copyClient, tenant: tenant}
	sweptAll(t, s)
	if len(s.reported) > 0 || len(s.stopping) != 1 {
		t.Errorf("the sweep stops on %q and reports %q, want the copy's failure stopping", s.stopping, s.reported)
	}
	var lists []string
	for _, read := range reads.read() {
		if strings.HasPrefix(read, configMapsPath) {
			lists = append(lists, read)
		}
	}
	if len(lists) != 4 || !strings.Contains(lists[1], "continue=t1") || strings.Contains(lists[2], "continue=") || !strings.Contains(lists[3], "continue=t2") {
		t.Errorf("the tenant's lists are %q, want a first page, the expired token, then the list again from its start", lists)
	}
}

func TestAResourceThatFailsThroughBothIsReportedWithEachSidesError(t *testing.T) {
	copyClient, _ := stub(t, map[string][]answer{configMapsPath: {undecodable}})
	tenant, _ := stub(t, map[string][]answer{configMapsPath: {status(http.StatusInternalServerError, "InternalError", "illegal base64 data at input byte 4")}})
	s := &sweep{copy: copyClient, tenant: tenant}
	sweptAll(t, s)
	want := "configmaps of v1 fails through the tenant too: illegal base64 data at input byte 4; through the copy: proto: illegal wireType 6"
	if len(s.stopping) > 0 || !slices.Equal(s.reported, []string{want}) {
		t.Errorf("the sweep stops on %q and reports %q, want %q reported", s.stopping, s.reported, want)
	}
}

func TestAReadThatStillFailsStopsTheStepOnlyWithAnAnswer(t *testing.T) {
	for _, c := range []struct {
		name   string
		copy   map[string][]answer
		tenant []answer
		what   string
		stops  bool
	}{
		{"a list through the copy answered 503", map[string][]answer{configMapsPath: {status(http.StatusServiceUnavailable, "ServiceUnavailable", "injected")}}, nil, "the list of configmaps of v1", true},
		{"a list through the copy without an answer", map[string][]answer{configMapsPath: {dropped}}, nil, "the list of configmaps of v1", false},
		{"a list through the copy cut", map[string][]answer{configMapsPath: {cutMidBody}}, nil, "the list of configmaps of v1", true},
		{"the copy's discovery cut", map[string][]answer{"/api": {cutMidBody}}, nil, "its discovery", true},
		{"the copy's list of APIServices cut", map[string][]answer{"/apis/apiregistration.k8s.io/v1/apiservices": {cutMidBody}}, nil, "its list of APIServices", true},
		{"the copy's list of CustomResourceDefinitions cut", map[string][]answer{"/apis/apiextensions.k8s.io/v1/customresourcedefinitions": {cutMidBody}}, nil, "its list of CustomResourceDefinitions", true},
		{"the tenant's list of a resource the copy failed", map[string][]answer{configMapsPath: {undecodable}}, []answer{status(http.StatusTooManyRequests, "TooManyRequests", "injected")}, "the tenant's list of configmaps of v1 (through the copy: proto: illegal wireType 6)", true},
		{"the tenant's list of a resource the copy failed, cut", map[string][]answer{configMapsPath: {undecodable}}, []answer{cutMidBody}, "the tenant's list of configmaps of v1 (through the copy: proto: illegal wireType 6)", true},
	} {
		t.Run(c.name, func(t *testing.T) {
			copyClient, _ := stub(t, c.copy)
			answers := map[string][]answer{}
			if c.tenant != nil {
				answers[configMapsPath] = c.tenant
			}
			tenant, _ := stub(t, answers)
			s := &sweep{copy: copyClient, tenant: tenant}
			for range 3 {
				if err := s.run(t.Context()); !errors.Is(err, errAgain) {
					t.Fatalf("the pass ends with %v, want the read sent again", err)
				}
			}
			if s.failing == nil || s.failing.what != c.what || s.failing.stops != c.stops {
				t.Errorf("the sweep sends again %+v, want %q stopping the step at the deadline: %t", s.failing, c.what, c.stops)
			}
		})
	}
}

func TestASweepGoesOnWhereThePassBudgetEndedIt(t *testing.T) {
	pass, end := context.WithCancel(t.Context())
	endPass := func(w http.ResponseWriter, r *http.Request) {
		end()
		<-r.Context().Done()
	}
	copyClient, reads := stub(t, map[string][]answer{configMapsPath: {rows(500, "c1"), endPass, rows(3, "")}})
	tenant, _ := stub(t, nil)
	s := &sweep{copy: copyClient, tenant: tenant}
	if err := s.run(pass); !errors.Is(err, errBudget) {
		t.Fatalf("the pass ends with %v, want errBudget", err)
	}
	sweptAll(t, s)
	var lists []string
	for _, read := range reads.read() {
		if strings.HasPrefix(read, configMapsPath) {
			lists = append(lists, read)
		}
	}
	if len(s.failed) > 0 || len(s.stopping) > 0 || s.objects != 503 || len(lists) != 3 || !strings.Contains(lists[2], "continue=c1") {
		t.Errorf("the sweep ends with %d objects, failed %v and stopping %q after the lists %q, want 503 objects and the second page read again with its token", s.objects, s.failed, s.stopping, lists)
	}
}

func TestAPassReadsNoLongerThanWhatIsLeftOfTheCopysMinutes(t *testing.T) {
	late := func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(readTimeout + 5*time.Second):
		case <-r.Context().Done():
		}
	}
	copyClient, _ := stub(t, map[string][]answer{"/api": {late}})
	tenant, _ := stub(t, nil)
	now := time.Now()
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{UID: "copy-uid", CreationTimestamp: metav1.NewTime(now.Add(2*time.Second - copyDeadline))}}
	r := &RehearsalReconciler{Clock: clocktesting.NewFakePassiveClock(now), sweeps: map[types.UID]*sweep{pod.UID: {copy: copyClient, tenant: tenant}}}
	failed := &metav1.Condition{Type: lodiav1alpha1.ConditionRehearsed, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonAPIServerFailed}
	began := time.Now()
	result, err := r.sweep(t.Context(), &lodiav1alpha1.ControlPlane{}, "v1.37.1", failed, pod)
	if took := time.Since(began); err != nil || took > 5*time.Second || result.RequeueAfter > 2*time.Second {
		t.Errorf("a pass 2 s before the copy's deadline ends after %s with %v and waits %s, want it within the 2 s left", took.Round(time.Second), err, result.RequeueAfter)
	}
}

func TestAReadThatAnswersAgainNoLongerFailsAtTheDeadline(t *testing.T) {
	pass, end := context.WithCancel(t.Context())
	endPass := func(w http.ResponseWriter, r *http.Request) {
		end()
		<-r.Context().Done()
	}
	copyClient, _ := stub(t, map[string][]answer{configMapsPath: {status(http.StatusServiceUnavailable, "ServiceUnavailable", "injected"), rows(500, "c1"), endPass}})
	tenant, _ := stub(t, nil)
	s := &sweep{copy: copyClient, tenant: tenant}
	if err := s.run(t.Context()); !errors.Is(err, errAgain) {
		t.Fatalf("the pass ends with %v, want the read sent again", err)
	}
	if err := s.run(pass); !errors.Is(err, errBudget) {
		t.Fatalf("the pass ends with %v, want errBudget", err)
	}
	cp := &lodiav1alpha1.ControlPlane{ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "dev", UID: "cp-uid", Generation: 3}}
	pod := &corev1.Pod{ObjectMeta: objectMeta(cp, copyName(cp.Name)+"-expired")}
	pod.UID = "copy-uid"
	sc := scheme(t)
	if err := corev1.AddToScheme(sc); err != nil {
		t.Fatal(err)
	}
	if err := networkingv1.AddToScheme(sc); err != nil {
		t.Fatal(err)
	}
	var applied []byte
	c := interceptor.NewClient(fake.NewClientBuilder().WithScheme(sc).WithObjects(cp, pod).Build(), interceptor.Funcs{
		SubResourceApply: func(_ context.Context, _ client.Client, _ string, obj runtime.ApplyConfiguration, _ ...client.SubResourceApplyOption) error {
			var err error
			applied, err = json.Marshal(obj)
			return err
		},
	})
	r := &RehearsalReconciler{Client: c, Reader: c, Clock: clock.RealClock{}, sweeps: map[types.UID]*sweep{pod.UID: s}}
	if _, err := r.expired(t.Context(), cp, "v1.37.1", pod); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(applied), `"reason":"CopyFailed"`) || len(s.stopping) > 0 {
		t.Errorf("the deadline after a 503 that answered later writes %s and stops on %q, want CopyFailed", applied, s.stopping)
	}
}

func TestTheSweepOfACopyThatIsGoneOrBeingDeletedIsDropped(t *testing.T) {
	sc := scheme(t)
	if err := corev1.AddToScheme(sc); err != nil {
		t.Fatal(err)
	}
	if err := networkingv1.AddToScheme(sc); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name     string
		step     lodiav1alpha1.UpgradeStep
		deleting bool
	}{
		{"gone at Rehearsal", lodiav1alpha1.UpgradeStepRehearsal, false},
		{"being deleted at Rehearsal", lodiav1alpha1.UpgradeStepRehearsal, true},
		{"gone once the step left Rehearsal", lodiav1alpha1.UpgradeStepBackup, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			cp := &lodiav1alpha1.ControlPlane{
				ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "dev", UID: "cp-uid", Generation: 3},
				Spec:       lodiav1alpha1.ControlPlaneSpec{Version: "v1.37.1"},
				Status: lodiav1alpha1.ControlPlaneStatus{
					Target:     &lodiav1alpha1.Target{Version: "v1.36.6"},
					Kubernetes: &lodiav1alpha1.KubernetesStatus{Upgrade: &lodiav1alpha1.UpgradeStatus{Step: c.step}},
				},
			}
			objects := []client.Object{cp}
			if c.deleting {
				pod := &corev1.Pod{ObjectMeta: objectMeta(cp, copyName(cp.Name)+"-deleting")}
				pod.UID, pod.Labels, pod.Finalizers = "copy-uid", podLabels(cp.Name, lodiav1alpha1.RehearsalComponent), []string{"test/node-gone"}
				pod.DeletionTimestamp = new(metav1.Now())
				objects = append(objects, pod)
			}
			fakeClient := fake.NewClientBuilder().WithScheme(sc).WithObjects(objects...).WithStatusSubresource(cp).Build()
			copyClient, _ := stub(t, nil)
			r := &RehearsalReconciler{Client: fakeClient, Reader: fakeClient, MaxConcurrent: 1, Clock: clock.RealClock{},
				sweeps: map[types.UID]*sweep{"copy-uid": {controlPlane: cp.UID, key: client.ObjectKeyFromObject(cp), copy: copyClient}}}
			if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); err != nil {
				t.Fatal(err)
			}
			if len(r.sweeps) != 0 {
				t.Errorf("the reconciler keeps the sweeps %v of a copy %s", slices.Collect(maps.Keys(r.sweeps)), c.name)
			}
		})
	}
}

func crdsOfTheCopy(crds ...string) answer {
	var items []string
	for _, crd := range crds {
		plural, version, _ := strings.Cut(crd, "/")
		items = append(items, `{"spec":{"group":"example.com","names":{"plural":"`+plural+`"},"versions":[{"name":"`+version+`","served":true}]}}`)
	}
	return body(`{"kind":"List","metadata":{},"items":[` + strings.Join(items, ",") + `]}`)
}

func serves(versions map[string][]string) answer {
	var out []string
	for _, version := range slices.Sorted(maps.Keys(versions)) {
		var resources []string
		for _, r := range versions[version] {
			resources = append(resources, `{"resource":"`+r+`","verbs":["list"]}`)
		}
		out = append(out, `{"version":"`+version+`","resources":[`+strings.Join(resources, ",")+`]}`)
	}
	if len(out) == 0 {
		return body(noGroups)
	}
	return body(`{"kind":"APIGroupDiscoveryList","apiVersion":"apidiscovery.k8s.io/v2","metadata":{},"items":[{"metadata":{"name":"example.com"},"versions":[` + strings.Join(out, ",") + `]}]}`)
}

func TestACustomResourceTheTenantServesStopsTheStepOnlyWhenTheCopysOwnCRDServesIt(t *testing.T) {
	for _, c := range []struct {
		name               string
		crds               []string
		copyServes, tenant map[string][]string
		stopping, reported []string
	}{
		{"a CRD the copy holds and does not serve, its group-version missing", []string{"as/v1"}, nil, map[string][]string{"v1": {"as"}},
			[]string{"the copy does not serve as of example.com/v1, which its own CustomResourceDefinition serves"}, nil},
		{"a CRD the copy holds and does not serve, beside one it serves", []string{"as/v1", "bs/v1"}, map[string][]string{"v1": {"as"}}, map[string][]string{"v1": {"as", "bs"}},
			[]string{"the copy does not serve bs of example.com/v1, which its own CustomResourceDefinition serves"}, nil},
		{"a CRD created after the snapshot beside one the copy serves", []string{"as/v1"}, map[string][]string{"v1": {"as"}}, map[string][]string{"v1": {"as", "bs"}},
			nil, []string{"only the tenant serves bs of example.com/v1"}},
		{"a version added after the snapshot to a CRD the copy holds", []string{"as/v1"}, map[string][]string{"v1": {"as"}}, map[string][]string{"v1": {"as"}, "v2": {"as"}},
			nil, []string{"only the tenant serves example.com/v2"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			copyClient, _ := stub(t, map[string][]answer{
				"/apis": {serves(c.copyServes)},
				"/apis/apiextensions.k8s.io/v1/customresourcedefinitions": {crdsOfTheCopy(c.crds...)},
				"/apis/example.com/v1/as":                                 {rows(1, "")},
			})
			tenant, _ := stub(t, map[string][]answer{"/apis": {serves(c.tenant)}})
			s := &sweep{copy: copyClient, tenant: tenant}
			sweptAll(t, s)
			if !slices.Equal(s.stopping, c.stopping) || !slices.Equal(s.reported, c.reported) {
				t.Errorf("the sweep stops on %q and reports %q, want %q and %q", s.stopping, s.reported, c.stopping, c.reported)
			}
		})
	}
}

func TestAControlPlaneGoneTakesItsSweepsAndVerdictTimeAlong(t *testing.T) {
	key := types.NamespacedName{Namespace: "team-a", Name: "dev"}
	for _, deleting := range []bool{false, true} {
		c := fake.NewClientBuilder().WithScheme(scheme(t)).Build()
		if deleting {
			cp := &lodiav1alpha1.ControlPlane{ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: key.Name, UID: "cp-uid", Finalizers: []string{"test/finalizer"}}}
			if err := c.Create(t.Context(), cp); err != nil {
				t.Fatal(err)
			}
			if err := c.Delete(t.Context(), cp); err != nil {
				t.Fatal(err)
			}
		}
		copyClient, _ := stub(t, nil)
		other, _ := stub(t, nil)
		r := &RehearsalReconciler{Client: c, Reader: c, MaxConcurrent: 1, Timeout: time.Second}
		r.sweeps = map[types.UID]*sweep{"copy-uid": {controlPlane: "cp-uid", key: key, copy: copyClient, lists: make([]list, 10000)}, "other-copy-uid": {controlPlane: "other-uid", key: types.NamespacedName{Namespace: "team-b", Name: "dev"}, copy: other}}
		r.verdicts = map[types.UID]verdictAt{"cp-uid": {key: key, at: time.Now()}, "other-uid": {key: types.NamespacedName{Namespace: "team-b", Name: "dev"}, at: time.Now()}}
		if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: key}); err != nil {
			t.Fatal(err)
		}
		if _, kept := r.sweeps["copy-uid"]; kept || len(r.sweeps) != 1 || len(r.verdicts) != 1 {
			t.Errorf("deleting %t: the reconciler keeps the sweeps %v and the verdict times %v, want the other ControlPlane's alone", deleting, slices.Collect(maps.Keys(r.sweeps)), r.verdicts)
		}
	}
}

type slowLists struct{ client.Client }

func (s slowLists) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	time.Sleep(100 * time.Millisecond)
	return s.Client.List(ctx, list, opts...)
}

func TestTwoPlacesAtOnceUnderALimitOfOneCreateOneCopy(t *testing.T) {
	s := scheme(t)
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	c := slowLists{fake.NewClientBuilder().WithScheme(s).Build()}
	r := &RehearsalReconciler{Client: c, Reader: c, MaxConcurrent: 1}
	var wg sync.WaitGroup
	for _, name := range []string{"a", "b"} {
		cp := &lodiav1alpha1.ControlPlane{ObjectMeta: metav1.ObjectMeta{Namespace: "team-" + name, Name: "dev", UID: types.UID(name)}}
		pod := &corev1.Pod{ObjectMeta: objectMeta(cp, copyName(cp.Name))}
		pod.Labels = podLabels(cp.Name, lodiav1alpha1.RehearsalComponent)
		wg.Go(func() {
			if _, err := r.place(t.Context(), cp, pod); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	list := &corev1.PodList{}
	if err := c.List(t.Context(), list); err != nil {
		t.Fatal(err)
	}
	if len(list.Items) != 1 {
		t.Errorf("two places at once under a limit of 1 created %d copies", len(list.Items))
	}
}

func TestACopyLeftBesideAPassedVerdictIsDeleted(t *testing.T) {
	s := scheme(t)
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := networkingv1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	cp := &lodiav1alpha1.ControlPlane{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "dev", UID: "cp-uid", Generation: 3},
		Spec:       lodiav1alpha1.ControlPlaneSpec{Version: "v1.37.1"},
		Status: lodiav1alpha1.ControlPlaneStatus{
			Target:     &lodiav1alpha1.Target{Version: "v1.36.6"},
			Kubernetes: &lodiav1alpha1.KubernetesStatus{Upgrade: &lodiav1alpha1.UpgradeStatus{Step: lodiav1alpha1.UpgradeStepRehearsal}},
			Rehearsal:  &lodiav1alpha1.RehearsalStatus{Version: "v1.37.1"},
			Conditions: []metav1.Condition{{Type: lodiav1alpha1.ConditionRehearsed, Status: metav1.ConditionTrue, Reason: lodiav1alpha1.ReasonPassed, ObservedGeneration: 3, LastTransitionTime: metav1.Now()}},
		},
	}
	left := &corev1.Pod{ObjectMeta: objectMeta(cp, copyName(cp.Name)+"-left")}
	left.Labels = podLabels(cp.Name, lodiav1alpha1.RehearsalComponent)
	left.Annotations = map[string]string{versionAnnotation: "v1.37.1", generationAnnotation: "3"}
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(cp, left).WithStatusSubresource(cp).Build()
	r := &RehearsalReconciler{Client: c, Reader: c, MaxConcurrent: 1, Clock: clock.RealClock{}}
	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(t.Context(), client.ObjectKeyFromObject(left), &corev1.Pod{}); !apierrors.IsNotFound(err) {
		t.Errorf("the copy left beside a Passed verdict is still there: %v", err)
	}
}

func TestPlaceFindsTheCopyTheCacheMissed(t *testing.T) {
	s := scheme(t)
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	cp := &lodiav1alpha1.ControlPlane{ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "dev", UID: "cp-uid"}}
	created := &corev1.Pod{ObjectMeta: objectMeta(cp, copyName(cp.Name)+"-created")}
	created.Labels = podLabels(cp.Name, lodiav1alpha1.RehearsalComponent)
	api := fake.NewClientBuilder().WithScheme(s).WithObjects(created).Build()
	r := &RehearsalReconciler{Client: fake.NewClientBuilder().WithScheme(s).Build(), Reader: api, MaxConcurrent: 5}
	another := &corev1.Pod{ObjectMeta: objectMeta(cp, "")}
	another.GenerateName, another.Labels = copyName(cp.Name)+"-", created.Labels
	if _, err := r.place(t.Context(), cp, another); !errors.Is(err, errCopied) {
		t.Errorf("place over a copy the cache lacks ends with %v, want errCopied", err)
	}
}
