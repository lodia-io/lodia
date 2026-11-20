// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package workloads_test

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	apiregistrationv1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/controller/workloads"
	"go.lodia.io/lodia/internal/testenv"
)

const (
	operatorNamespace = "lodia-system"
	copyIP            = "10.99.0.1"
	rehearsedFrom     = "v1.36.6"
	unknownGate       = `E1119 21:04:24.723547       1 run.go:72] "command failed" err="unrecognized feature gate: APIServerTracing"`
)

type offsetClock struct{ offset atomic.Int64 }

func (c *offsetClock) Now() time.Time { return time.Now().Add(time.Duration(c.offset.Load())) }

func (c *offsetClock) Since(t time.Time) time.Duration { return c.Now().Sub(t) }

var rehearsalClock = &offsetClock{}

type copies struct {
	*httptest.Server
	tenantProxy   *httptest.Server
	tenantAddress string
	refuse        atomic.Bool
	refused       atomic.Int32
	admin         client.Client
	etcd          string
	tenant        string
	mu            sync.Mutex
	paths         []string
	faults        map[string]*fault
	delays        map[string]*delays
}

type delays struct {
	each    []time.Duration
	queries []string
}

type fault struct {
	code, left int
	served     atomic.Int32
}

var (
	copyServers  *copies
	testenvAdmin *rest.Config
	apis         client.Client
	newRehearsal func() *workloads.RehearsalReconciler
)

func startCopies(env *testenv.Env) *copies {
	scheme := testenv.Scheme(apiextensionsv1.AddToScheme, apiregistrationv1.AddToScheme)
	testenvAdmin = env.Admin()
	var err error
	if apis, err = client.New(testenvAdmin, client.Options{Scheme: scheme}); err != nil {
		env.Fatal("the tenant's client", err)
	}
	second := env.StartSecond()
	admin, err := client.New(second, client.Options{Scheme: scheme})
	if err != nil {
		env.Fatal("the copy's client", err)
	}
	if err := copyCRDs(apis, admin); err != nil {
		env.Fatal("the copy's CRDs", err)
	}
	transport, err := rest.TransportFor(second)
	if err != nil {
		env.Fatal("the copy's transport", err)
	}
	target, err := url.Parse(second.Host)
	if err != nil {
		env.Fatal("the copy's address", err)
	}
	certFile, keyFile := env.ServingCertificate()
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		env.Fatal("the serving certificate", err)
	}
	tenantTransport, err := rest.TransportFor(testenvAdmin)
	if err != nil {
		env.Fatal("the tenant's transport", err)
	}
	tenant, err := url.Parse(testenvAdmin.Host)
	if err != nil {
		env.Fatal("the tenant's address", err)
	}
	c := &copies{admin: admin, etcd: env.SecondEtcd(), tenant: env.Etcd(), tenantAddress: tenant.Host, faults: map[string]*fault{}, delays: map[string]*delays{}}
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.Transport = transport
	c.Server = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.mu.Lock()
		c.paths = append(c.paths, r.URL.Path)
		var wait time.Duration
		if d := c.delays[r.URL.Path]; d != nil {
			d.queries = append(d.queries, r.URL.RawQuery)
			if len(d.each) > 0 {
				wait, d.each = d.each[0], d.each[1:]
			}
		}
		c.mu.Unlock()
		select {
		case <-time.After(wait):
		case <-r.Context().Done():
			return
		}
		if !c.fail("copy", w, r) {
			proxy.ServeHTTP(w, r)
		}
	}))
	c.TLS = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	c.StartTLS()
	toTenant := httputil.NewSingleHostReverseProxy(tenant)
	toTenant.Transport = tenantTransport
	c.tenantProxy = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !c.fail("tenant", w, r) {
			toTenant.ServeHTTP(w, r)
		}
	}))
	c.tenantProxy.TLS = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	c.tenantProxy.StartTLS()
	copyServers = c
	return c
}

func (c *copies) Close() {
	c.tenantProxy.Close()
	c.Server.Close()
}

func (c *copies) inject(t *testing.T, side, path string, code, times int) *fault {
	t.Helper()
	f := &fault{code: code, left: times}
	c.mu.Lock()
	c.faults[side+" "+path] = f
	c.mu.Unlock()
	t.Cleanup(func() {
		c.mu.Lock()
		delete(c.faults, side+" "+path)
		c.mu.Unlock()
	})
	return f
}

func (c *copies) slow(t *testing.T, path string, each ...time.Duration) *delays {
	t.Helper()
	d := &delays{each: each}
	c.mu.Lock()
	c.delays[path] = d
	c.mu.Unlock()
	t.Cleanup(func() {
		c.mu.Lock()
		delete(c.delays, path)
		c.mu.Unlock()
	})
	return d
}

func (c *copies) queries(d *delays) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(d.queries)
}

func (c *copies) fail(side string, w http.ResponseWriter, r *http.Request) bool {
	c.mu.Lock()
	f := c.faults[side+" "+r.URL.Path]
	answer := f != nil && f.left != 0
	if answer && f.left > 0 {
		f.left--
	}
	c.mu.Unlock()
	if !answer {
		return false
	}
	f.served.Add(1)
	reason := map[int]string{http.StatusTooManyRequests: "TooManyRequests", http.StatusServiceUnavailable: "ServiceUnavailable"}[f.code]
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(f.code)
	_, _ = fmt.Fprintf(w, `{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","message":"injected","reason":%q,"code":%d}`, reason, f.code)
	return true
}

func (c *copies) dial(ctx context.Context, network, address string) (net.Conn, error) {
	if address == net.JoinHostPort(copyIP, "6443") {
		if c.refuse.Load() {
			c.refused.Add(1)
			return nil, errors.New("connection refused")
		}
		address = c.Listener.Addr().String()
	} else if address == c.tenantAddress {
		address = c.tenantProxy.Listener.Addr().String()
	}
	return (&net.Dialer{}).DialContext(ctx, network, address)
}

func (c *copies) read() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.paths)
}

func copyCRDs(tenant, copyAdmin client.Client) error {
	ctx := context.Background()
	list := &apiextensionsv1.CustomResourceDefinitionList{}
	if err := tenant.List(ctx, list); err != nil {
		return err
	}
	for _, crd := range list.Items {
		if err := copyAdmin.Create(ctx, &apiextensionsv1.CustomResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: crd.Name, Annotations: crd.Annotations}, Spec: crd.Spec}); err != nil {
			return err
		}
	}
	deadline := time.Now().Add(30 * time.Second)
	for _, crd := range list.Items {
		for {
			served := &apiextensionsv1.CustomResourceDefinition{}
			if err := copyAdmin.Get(ctx, client.ObjectKey{Name: crd.Name}, served); err != nil {
				return err
			}
			if slices.ContainsFunc(served.Status.Conditions, func(c apiextensionsv1.CustomResourceDefinitionCondition) bool {
				return c.Type == apiextensionsv1.Established && c.Status == apiextensionsv1.ConditionTrue
			}) {
				break
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("CRD %s is not established in the copy", crd.Name)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	return nil
}

func rehearsing(t *testing.T) *lodiav1alpha1.ControlPlane {
	t.Helper()
	cp := controlPlaneForRehearsal(t)
	startRehearsal(t, cp)
	return cp
}

func controlPlaneForRehearsal(t *testing.T) *lodiav1alpha1.ControlPlane {
	t.Helper()
	cp := controlPlane(t, loadBalancer())
	setTargetAt(t, cp, rehearsedFrom, "")
	applyStatus(t, cp, "test/endpoint", map[string]any{"endpoint": map[string]any{
		"apiServer":    map[string]any{"host": lbAddress, "port": int64(443)},
		"konnectivity": map[string]any{"host": lbAddress, "port": int64(8132)},
		"inCluster":    map[string]any{"host": lbAddress, "port": int64(6443)},
	}})
	setDatastore(t, cp, lodiav1alpha1.HostPort{Host: "dev-etcd-client." + cp.Namespace + ".svc", Port: 2379})
	writeKubeconfig(t, cp)
	return cp
}

func startRehearsal(t *testing.T, cp *lodiav1alpha1.ControlPlane) {
	t.Helper()
	applyStatus(t, cp, "test/lifecycle", step(lodiav1alpha1.UpgradeStepRehearsal))
	t.Cleanup(func() { endRehearsal(t, cp) })
}

func step(s lodiav1alpha1.UpgradeStep) map[string]any {
	return map[string]any{"kubernetes": map[string]any{"upgrade": map[string]any{"step": string(s), "startedAt": "2026-11-20T00:00:00Z"}}}
}

func endRehearsal(t *testing.T, cp *lodiav1alpha1.ControlPlane) {
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": lodiav1alpha1.GroupVersion.String(),
		"kind":       "ControlPlane",
		"metadata":   map[string]any{"name": cp.Name, "namespace": cp.Namespace},
		"status":     step(lodiav1alpha1.UpgradeStepBackup),
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := k8s.Status().Apply(ctx, client.ApplyConfigurationFromUnstructured(u), client.FieldOwner("test/lifecycle"), client.ForceOwnership); err != nil {
		t.Error(err)
		return
	}
	for ctx.Err() == nil {
		if pod, err := liveCopy(ctx, cp); err == nil && pod == nil {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Errorf("the copy of %s/%s is still there after its step", cp.Namespace, cp.Name)
}

func copyPods(ctx context.Context, cp *lodiav1alpha1.ControlPlane) ([]corev1.Pod, error) {
	list := &corev1.PodList{}
	if err := k8s.List(ctx, list, client.InNamespace(cp.Namespace), client.MatchingLabels{lodiav1alpha1.ControlPlaneLabel: cp.Name, lodiav1alpha1.ComponentLabel: lodiav1alpha1.RehearsalComponent}); err != nil {
		return nil, err
	}
	return list.Items, nil
}

func liveCopy(ctx context.Context, cp *lodiav1alpha1.ControlPlane) (*corev1.Pod, error) {
	all, err := copyPods(ctx, cp)
	if err != nil {
		return nil, err
	}
	for i := range all {
		if all[i].DeletionTimestamp.IsZero() && metav1.IsControlledBy(&all[i], cp) {
			return &all[i], nil
		}
	}
	return nil, nil
}

func writeKubeconfig(t *testing.T, cp *lodiav1alpha1.ControlPlane) {
	t.Helper()
	admin := testenvAdmin
	config := clientcmdapi.NewConfig()
	config.Clusters["tenant"] = &clientcmdapi.Cluster{Server: admin.Host, CertificateAuthorityData: admin.CAData}
	config.AuthInfos["lodia"] = &clientcmdapi.AuthInfo{ClientCertificateData: admin.CertData, ClientKeyData: admin.KeyData}
	config.Contexts["lodia"] = &clientcmdapi.Context{Cluster: "tenant", AuthInfo: "lodia"}
	config.CurrentContext = "lodia"
	kubeconfig, err := clientcmd.Write(*config)
	if err != nil {
		t.Fatal(err)
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: cp.Namespace, Name: lodiav1alpha1.LodiaKubeconfigSecret.Name(cp.Name), Labels: map[string]string{lodiav1alpha1.ControlPlaneLabel: cp.Name}},
		Data:       map[string][]byte{lodiav1alpha1.KubeconfigKey: kubeconfig},
	}
	if err := k8s.Create(t.Context(), secret); err != nil {
		t.Fatal(err)
	}
}

func copyOf(t *testing.T, cp *lodiav1alpha1.ControlPlane) *corev1.Pod {
	t.Helper()
	pod, err := liveCopy(t.Context(), cp)
	if err != nil {
		t.Fatal(err)
	}
	return pod
}

func eventuallyCopy(t *testing.T, cp *lodiav1alpha1.ControlPlane, generation int64) *corev1.Pod {
	t.Helper()
	var pod *corev1.Pod
	testenv.Eventually(t, func() error {
		if pod = copyOf(t, cp); pod == nil || pod.Annotations["lodia.io/rehearsal-generation"] != fmt.Sprint(generation) {
			return fmt.Errorf("no copy of generation %d: %+v", generation, pod)
		}
		return nil
	})
	return pod
}

func setCopy(t *testing.T, pod *corev1.Pod, status corev1.PodStatus) {
	t.Helper()
	testenv.Eventually(t, func() error {
		fresh := &corev1.Pod{}
		if err := k8s.Get(t.Context(), client.ObjectKeyFromObject(pod), fresh); err != nil {
			return err
		}
		fresh.Status = status
		return k8s.Status().Update(t.Context(), fresh)
	})
}

func readyCopy() corev1.PodStatus {
	return corev1.PodStatus{Phase: corev1.PodRunning, PodIP: copyIP, ContainerStatuses: []corev1.ContainerStatus{{
		Name: "kube-apiserver", Ready: true, Started: new(true), Image: "registry.k8s.io/kube-apiserver:" + version,
		State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.Now()}},
	}}}
}

func exitedCopy(message string) corev1.PodStatus {
	return corev1.PodStatus{Phase: corev1.PodRunning, PodIP: copyIP, ContainerStatuses: []corev1.ContainerStatus{{
		Name: "kube-apiserver", Image: "registry.k8s.io/kube-apiserver:" + version,
		State:                corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}},
		LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1, Reason: "Error", Message: message}},
	}}}
}

func rehearsed(t *testing.T, cp *lodiav1alpha1.ControlPlane) (*metav1.Condition, string) {
	t.Helper()
	fresh := get(t, cp)
	rehearsalVersion := ""
	if fresh.Status.Rehearsal != nil {
		rehearsalVersion = fresh.Status.Rehearsal.Version
	}
	return meta.FindStatusCondition(fresh.Status.Conditions, lodiav1alpha1.ConditionRehearsed), rehearsalVersion
}

func expectRehearsed(t *testing.T, cp *lodiav1alpha1.ControlPlane, status metav1.ConditionStatus, reason string, contains ...string) *metav1.Condition {
	t.Helper()
	var c *metav1.Condition
	testenv.Eventually(t, func() error {
		var at string
		c, at = rehearsed(t, cp)
		switch {
		case c == nil || c.Status != status || c.Reason != reason || c.ObservedGeneration != get(t, cp).Generation || at != version:
			return fmt.Errorf("Rehearsed is %+v for %q", c, at)
		case slices.ContainsFunc(contains, func(s string) bool { return !strings.Contains(c.Message, s) }):
			return fmt.Errorf("Rehearsed's message is %q, want %q in it", c.Message, contains)
		}
		return nil
	})
	return c
}

func expectNoCopy(t *testing.T, cp *lodiav1alpha1.ControlPlane) {
	t.Helper()
	testenv.Eventually(t, func() error {
		if pod := copyOf(t, cp); pod != nil {
			return fmt.Errorf("the copy %s is still there", pod.Name)
		}
		policy := &networkingv1.NetworkPolicy{}
		if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: cp.Namespace, Name: cp.Name + "-rehearsal"}, policy); !apierrors.IsNotFound(err) {
			return fmt.Errorf("the copy's NetworkPolicy is still there: %w", err)
		}
		return nil
	})
}

func TestAReadyCopyPassesWithItsReport(t *testing.T) {
	cp := rehearsing(t)
	generation := get(t, cp).Generation
	pod := eventuallyCopy(t, cp, generation)
	expectRehearsed(t, cp, metav1.ConditionUnknown, lodiav1alpha1.ReasonRehearsing, version)
	if !metav1.IsControlledBy(pod, get(t, cp)) || pod.Labels[lodiav1alpha1.ComponentLabel] != lodiav1alpha1.RehearsalComponent || pod.Spec.Containers[0].Image != "registry.k8s.io/kube-apiserver:"+version {
		t.Errorf("the copy is labelled %v, owned by %+v and runs %s", pod.Labels, pod.OwnerReferences, pod.Spec.Containers[0].Image)
	}
	policy := &networkingv1.NetworkPolicy{}
	if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: cp.Namespace, Name: cp.Name + "-rehearsal"}, policy); err != nil {
		t.Fatal(err)
	}
	if from := policy.Spec.Ingress[0].From[0]; from.NamespaceSelector.MatchLabels[corev1.LabelMetadataName] != operatorNamespace {
		t.Errorf("the copy's NetworkPolicy admits %+v", from)
	}
	before := len(copyServers.read())
	setCopy(t, pod, readyCopy())
	expectRehearsed(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonPassed, "the copy at "+version+" listed ", " objects of ")
	expectNoCopy(t, cp)
	testenv.Eventually(t, func() error {
		list := &eventsv1.EventList{}
		if err := k8s.List(t.Context(), list, client.InNamespace(cp.Namespace)); err != nil {
			return err
		}
		if !slices.ContainsFunc(list.Items, func(e eventsv1.Event) bool {
			return e.ReportingController == "lodia.io/rehearsal" && e.Reason == lodiav1alpha1.ReasonPassed && e.Type == corev1.EventTypeNormal
		}) {
			return fmt.Errorf("no event of lodia.io/rehearsal says Passed: %d events", len(list.Items))
		}
		return nil
	})
	for _, entry := range get(t, cp).ManagedFields {
		fields := string(entry.FieldsV1.GetRawBytes())
		owns := strings.Contains(fields, `"f:rehearsal"`) || strings.Contains(fields, `"k:{\"type\":\"Rehearsed\"}"`)
		if owns != (entry.Manager == "lodia/rehearsal" && entry.Subresource == "status") {
			t.Errorf("%s writes %s of the status: %t", entry.Manager, fields, owns)
		}
	}
	read := copyServers.read()[before:]
	for _, path := range []string{"/api", "/apis", "/apis/apiregistration.k8s.io/v1/apiservices", "/apis/apiextensions.k8s.io/v1/customresourcedefinitions", "/api/v1/configmaps", "/apis/lodia.io/v1alpha1/controlplanes"} {
		if !slices.Contains(read, path) {
			t.Errorf("the sweep did not read %s through the copy: %v", path, read)
		}
	}
}

func TestACopyThatDoesNotAnswerIsReadAgainAndNotFailed(t *testing.T) {
	copyServers.refuse.Store(true)
	t.Cleanup(func() { copyServers.refuse.Store(false) })
	cp := rehearsing(t)
	setCopy(t, eventuallyCopy(t, cp, get(t, cp).Generation), readyCopy())
	time.Sleep(3 * time.Second)
	if c, _ := rehearsed(t, cp); c == nil || c.Reason != lodiav1alpha1.ReasonRehearsing {
		t.Errorf("Rehearsed is %+v while the copy does not answer, want Rehearsing", c)
	}
	copyServers.refuse.Store(false)
	testenv.EventuallyWithin(t, 2*time.Minute, func() error {
		c, _ := rehearsed(t, cp)
		if c == nil || c.Reason != lodiav1alpha1.ReasonPassed {
			return fmt.Errorf("Rehearsed is %+v", c)
		}
		return nil
	})
}

func TestAReadyCopyWhoseNodeIsGoneEndsAtItsDeadline(t *testing.T) {
	copyServers.refuse.Store(true)
	t.Cleanup(func() { copyServers.refuse.Store(false) })
	for _, after := range []time.Duration{20 * time.Minute, time.Hour, 24 * time.Hour} {
		t.Run(after.String(), func(t *testing.T) {
			cp := rehearsing(t)
			gone := eventuallyCopy(t, cp, get(t, cp).Generation)
			keep(t, gone)
			refused := copyServers.refused.Load()
			setCopy(t, gone, readyCopy())
			testenv.Eventually(t, func() error {
				if copyServers.refused.Load() == refused {
					return errors.New("the sweep has not dialled the copy")
				}
				return nil
			})
			if result, err := newRehearsal().Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); err != nil || result.RequeueAfter <= 0 || result.RequeueAfter > 5*time.Second {
				t.Errorf("a pass over the copy that does not answer ends with %v and waits %s, want the read sent again within 5 s", err, result.RequeueAfter)
			}
			rehearsalClock.offset.Store(int64(after))
			t.Cleanup(func() { rehearsalClock.offset.Store(0) })
			poke(t, cp)
			expectRehearsed(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonCopyFailed, "gave no verdict within 15m0s of its creation: ", "connection refused")
			testenv.Eventually(t, func() error {
				if err := k8s.Get(t.Context(), client.ObjectKeyFromObject(gone), gone); err != nil {
					return err
				}
				if gone.DeletionTimestamp.IsZero() {
					return errors.New("the copy is not being deleted")
				}
				return nil
			})
			rehearsalClock.offset.Store(0)

			other := rehearsing(t)
			setCopy(t, eventuallyCopy(t, other, get(t, other).Generation), corev1.PodStatus{Phase: corev1.PodPending})
			expectRehearsed(t, other, metav1.ConditionUnknown, lodiav1alpha1.ReasonRehearsing)
			endRehearsal(t, other)
			fresh := get(t, cp)
			patch := client.MergeFrom(fresh.DeepCopy())
			fresh.Spec.APIServer = &lodiav1alpha1.APIServer{FeatureGates: map[string]bool{"APIServerTracing": true}}
			if err := k8s.Patch(t.Context(), fresh, patch); err != nil {
				t.Fatal(err)
			}
			if replaced := eventuallyCopy(t, cp, fresh.Generation); replaced.Name == gone.Name {
				t.Errorf("the copy of generation %d took the name %s of the one being deleted", fresh.Generation, replaced.Name)
			}
		})
	}
}

func keep(t *testing.T, pod *corev1.Pod) {
	t.Helper()
	testenv.Eventually(t, func() error {
		fresh := &corev1.Pod{}
		if err := k8s.Get(t.Context(), client.ObjectKeyFromObject(pod), fresh); err != nil {
			return err
		}
		fresh.Finalizers = append(fresh.Finalizers, "test/node-gone")
		return k8s.Update(t.Context(), fresh)
	})
	t.Cleanup(func() {
		testenv.Eventually(t, func() error {
			fresh := &corev1.Pod{}
			if err := k8s.Get(context.Background(), client.ObjectKeyFromObject(pod), fresh); err != nil {
				return client.IgnoreNotFound(err)
			}
			fresh.Finalizers = slices.DeleteFunc(fresh.Finalizers, func(f string) bool { return f == "test/node-gone" })
			return k8s.Update(context.Background(), fresh)
		})
	})
}

func TestAnotherOwnersPodWithTheCopysLabelsIsNeitherJudgedNorDeleted(t *testing.T) {
	cp := controlPlaneForRehearsal(t)
	owner := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: cp.Namespace, Name: "owner"}}
	if err := k8s.Create(t.Context(), owner); err != nil {
		t.Fatal(err)
	}
	foreign := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: cp.Namespace, Name: cp.Name + "-rehearsal-foreign",
			Labels:          map[string]string{lodiav1alpha1.ControlPlaneLabel: cp.Name, lodiav1alpha1.ComponentLabel: lodiav1alpha1.RehearsalComponent},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "v1", Kind: "ConfigMap", Name: owner.Name, UID: owner.UID, Controller: new(true)}},
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "kube-apiserver", Image: "registry.example/kube-apiserver:" + version}}},
	}
	if err := k8s.Create(t.Context(), foreign); err != nil {
		t.Fatal(err)
	}
	setCopy(t, foreign, readyCopy())
	startRehearsal(t, cp)
	expectRehearsed(t, cp, metav1.ConditionUnknown, lodiav1alpha1.ReasonRehearsalLimit)
	time.Sleep(2 * time.Second)
	if err := k8s.Get(t.Context(), client.ObjectKeyFromObject(foreign), foreign); err != nil || !foreign.DeletionTimestamp.IsZero() {
		t.Fatalf("the other owner's Pod is gone or being deleted: %v", err)
	}
	if c, _ := rehearsed(t, cp); c.Reason != lodiav1alpha1.ReasonRehearsalLimit {
		t.Errorf("Rehearsed is %s beside the other owner's Pod, want RehearsalLimit", c.Reason)
	}
	if err := k8s.Delete(t.Context(), foreign); err != nil {
		t.Fatal(err)
	}
	poke(t, cp)
	setCopy(t, eventuallyCopy(t, cp, get(t, cp).Generation), readyCopy())
	expectRehearsed(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonPassed)
}

func TestTwoTenantsUnderALimitOfOneNeverGetTwoCopies(t *testing.T) {
	watcher, err := k8s.Watch(t.Context(), &corev1.PodList{}, client.MatchingLabels{lodiav1alpha1.ComponentLabel: lodiav1alpha1.RehearsalComponent})
	if err != nil {
		t.Fatal(err)
	}
	var most atomic.Int32
	watched := make(chan struct{})
	go func() {
		defer close(watched)
		running := map[string]bool{}
		for event := range watcher.ResultChan() {
			pod, ok := event.Object.(*corev1.Pod)
			if !ok {
				continue
			}
			key := pod.Namespace + "/" + pod.Name
			running[key] = event.Type != watch.Deleted
			n := int32(0)
			for _, r := range running {
				if r {
					n++
				}
			}
			if n > most.Load() {
				most.Store(n)
			}
		}
	}()
	a, b := rehearsing(t), rehearsing(t)
	var first, second *lodiav1alpha1.ControlPlane
	testenv.Eventually(t, func() error {
		switch {
		case copyOf(t, a) != nil:
			first, second = a, b
		case copyOf(t, b) != nil:
			first, second = b, a
		default:
			return errors.New("neither tenant has a copy")
		}
		return nil
	})
	expectRehearsed(t, second, metav1.ConditionUnknown, lodiav1alpha1.ReasonRehearsalLimit, "fleet.maxConcurrentRehearsals, 1, copies run")
	setCopy(t, eventuallyCopy(t, first, get(t, first).Generation), readyCopy())
	expectRehearsed(t, first, metav1.ConditionTrue, lodiav1alpha1.ReasonPassed)
	setCopy(t, eventuallyCopy(t, second, get(t, second).Generation), readyCopy())
	expectRehearsed(t, second, metav1.ConditionTrue, lodiav1alpha1.ReasonPassed)
	watcher.Stop()
	<-watched
	if n := most.Load(); n != 1 {
		t.Errorf("%d copies ran at once under a limit of 1", n)
	}
}

func TestAnAPIServerThatExitsFailsTheRehearsalWhichRunsAgainKeepingItsVerdict(t *testing.T) {
	cp := rehearsing(t)
	generation := get(t, cp).Generation
	setCopy(t, eventuallyCopy(t, cp, generation), exitedCopy(unknownGate))
	failed := expectRehearsed(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonAPIServerFailed, "exited 1 (Error): "+unknownGate)
	expectNoCopy(t, cp)

	rehearsalClock.offset.Store(int64(11 * time.Minute))
	t.Cleanup(func() { rehearsalClock.offset.Store(0) })
	poke(t, cp)
	again := eventuallyCopy(t, cp, generation)
	if c, _ := rehearsed(t, cp); c.Reason != failed.Reason || c.Message != failed.Message || !c.LastTransitionTime.Equal(&failed.LastTransitionTime) {
		t.Errorf("Rehearsed is %+v while the rehearsal runs again, want %+v kept", c, failed)
	}
	setCopy(t, again, readyCopy())
	expectRehearsed(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonPassed)
}

func TestTheWaitBeforeARunAgainSurvivesARestart(t *testing.T) {
	cp := rehearsing(t)
	setCopy(t, eventuallyCopy(t, cp, get(t, cp).Generation), exitedCopy(unknownGate))
	failed := expectRehearsed(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonAPIServerFailed)
	expectNoCopy(t, cp)

	rehearsalClock.offset.Store(int64(4 * time.Minute))
	t.Cleanup(func() { rehearsalClock.offset.Store(0) })
	restarted := newRehearsal()
	result, err := restarted.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)})
	if err != nil {
		t.Fatal(err)
	}
	if pod := copyOf(t, cp); pod != nil || result.RequeueAfter <= 5*time.Minute || result.RequeueAfter > 6*time.Minute {
		t.Errorf("a restarted operator 4 minutes after the verdict of %s made a copy (%t) and waits %s, want no copy and the rest of the 10 minutes", failed.LastTransitionTime, pod != nil, result.RequeueAfter)
	}
	rehearsalClock.offset.Store(int64(10*time.Minute + time.Second))
	if _, err := restarted.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); err != nil {
		t.Fatal(err)
	}
	eventuallyCopy(t, cp, get(t, cp).Generation)
}

func TestAPolicyAnotherOwnerKeepsIsTriedAgainSooner(t *testing.T) {
	cp := controlPlaneForRehearsal(t)
	foreign := &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: cp.Namespace, Name: cp.Name + "-rehearsal"},
		Spec:       networkingv1.NetworkPolicySpec{PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress}},
	}
	if err := k8s.Create(t.Context(), foreign); err != nil {
		t.Fatal(err)
	}
	owner := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: cp.Namespace, Name: "owner"}}
	if err := k8s.Create(t.Context(), owner); err != nil {
		t.Fatal(err)
	}
	foreign.OwnerReferences = []metav1.OwnerReference{{APIVersion: "v1", Kind: "ConfigMap", Name: owner.Name, UID: owner.UID, Controller: new(true)}}
	if err := k8s.Update(t.Context(), foreign); err != nil {
		t.Fatal(err)
	}
	startRehearsal(t, cp)
	expectRehearsed(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonObjectsControlledElsewhere, "NetworkPolicy "+cp.Name+"-rehearsal")
	if err := k8s.Delete(t.Context(), foreign); err != nil {
		t.Fatal(err)
	}
	testenv.Eventually(t, func() error {
		if err := k8s.Get(t.Context(), client.ObjectKeyFromObject(foreign), &networkingv1.NetworkPolicy{}); !apierrors.IsNotFound(err) {
			return fmt.Errorf("the other owner's NetworkPolicy is still there: %w", err)
		}
		return nil
	})
	poke(t, cp)
	eventuallyCopy(t, cp, get(t, cp).Generation)
}

func TestAPodTheAPIServerRefusesIsRefusedAndTriedAgainSooner(t *testing.T) {
	cp := controlPlaneForRehearsal(t)
	quota := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Namespace: cp.Namespace, Name: "no-pods"},
		Spec:       corev1.ResourceQuotaSpec{Hard: corev1.ResourceList{corev1.ResourcePods: resource.MustParse("0")}},
	}
	if err := k8s.Create(t.Context(), quota); err != nil {
		t.Fatal(err)
	}
	quota.Status = corev1.ResourceQuotaStatus{Hard: quota.Spec.Hard, Used: corev1.ResourceList{corev1.ResourcePods: resource.MustParse("0")}}
	if err := k8s.Status().Update(t.Context(), quota); err != nil {
		t.Fatal(err)
	}
	startRehearsal(t, cp)
	expectRehearsed(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonRefused, "the API server refuses Pod "+cp.Name+"-rehearsal-…")
	if err := k8s.Delete(t.Context(), quota); err != nil {
		t.Fatal(err)
	}
	eventuallyCopy(t, cp, get(t, cp).Generation)
}

func poke(t *testing.T, cp *lodiav1alpha1.ControlPlane) {
	t.Helper()
	fresh := get(t, cp)
	patch := client.MergeFrom(fresh.DeepCopy())
	if fresh.Annotations == nil {
		fresh.Annotations = map[string]string{}
	}
	fresh.Annotations["test/poke"] = time.Now().String()
	if err := k8s.Patch(t.Context(), fresh, patch); err != nil {
		t.Fatal(err)
	}
}

func TestANewGenerationReplacesARunningCopy(t *testing.T) {
	cp := rehearsing(t)
	old := eventuallyCopy(t, cp, get(t, cp).Generation)
	fresh := get(t, cp)
	patch := client.MergeFrom(fresh.DeepCopy())
	fresh.Spec.APIServer = &lodiav1alpha1.APIServer{FeatureGates: map[string]bool{"APIServerTracing": true}}
	if err := k8s.Patch(t.Context(), fresh, patch); err != nil {
		t.Fatal(err)
	}
	replaced := eventuallyCopy(t, cp, fresh.Generation)
	if replaced.UID == old.UID || !slices.Contains(replaced.Spec.Containers[0].Command, "--feature-gates=APIServerTracing=true") {
		t.Errorf("the copy of generation %d is %s with %v", fresh.Generation, replaced.UID, replaced.Spec.Containers[0].Command)
	}
	expectRehearsed(t, cp, metav1.ConditionUnknown, lodiav1alpha1.ReasonRehearsing)
}

func TestANewGenerationAfterAFailedVerdictIsRehearsedAtOnce(t *testing.T) {
	cp := rehearsing(t)
	setCopy(t, eventuallyCopy(t, cp, get(t, cp).Generation), exitedCopy(unknownGate))
	expectRehearsed(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonAPIServerFailed, unknownGate)
	expectNoCopy(t, cp)
	fresh := get(t, cp)
	patch := client.MergeFrom(fresh.DeepCopy())
	fresh.Spec.APIServer = &lodiav1alpha1.APIServer{FeatureGates: map[string]bool{"StorageVersionMigrator": true}}
	if err := k8s.Patch(t.Context(), fresh, patch); err != nil {
		t.Fatal(err)
	}
	eventuallyCopy(t, cp, fresh.Generation)
	expectRehearsed(t, cp, metav1.ConditionUnknown, lodiav1alpha1.ReasonRehearsing)
}

func TestAChainsSecondMinorIsRehearsedAtTheGenerationTheFirstPassedAt(t *testing.T) {
	cp := controlPlaneForRehearsal(t)
	setTargetAt(t, cp, "v1.35.10", "")
	startRehearsal(t, cp)
	generation := get(t, cp).Generation
	passed := func(step string) {
		t.Helper()
		pod := eventuallyCopy(t, cp, generation)
		if pod.Annotations["lodia.io/rehearsal-version"] != step || pod.Spec.Containers[0].Image != "registry.k8s.io/kube-apiserver:"+step {
			t.Fatalf("the copy rehearses %s with %s, want %s", pod.Annotations["lodia.io/rehearsal-version"], pod.Spec.Containers[0].Image, step)
		}
		setCopy(t, pod, readyCopy())
		testenv.Eventually(t, func() error {
			c, at := rehearsed(t, cp)
			if c == nil || c.Reason != lodiav1alpha1.ReasonPassed || c.ObservedGeneration != generation || at != step {
				return fmt.Errorf("Rehearsed is %+v for %q, want Passed for %s at %d", c, at, step, generation)
			}
			return nil
		})
		expectNoCopy(t, cp)
	}
	passed("v1.36.6")
	setTargetAt(t, cp, "v1.36.6", "")
	applyStatus(t, cp, "test/lifecycle", map[string]any{"kubernetes": map[string]any{"upgrade": map[string]any{"step": string(lodiav1alpha1.UpgradeStepRehearsal), "startedAt": "2026-11-20T01:00:00Z"}}})
	passed(version)
	if g := get(t, cp).Generation; g != generation {
		t.Errorf("the generation moved to %d, want both minors at %d", g, generation)
	}
}

func TestACopyThatDoesNotRunItsAPIServerFailsTheStep(t *testing.T) {
	cp := rehearsing(t)
	setCopy(t, eventuallyCopy(t, cp, get(t, cp).Generation), corev1.PodStatus{
		Phase: corev1.PodFailed, Reason: "DeadlineExceeded", Message: "Pod was active on the node longer than the specified deadline",
		InitContainerStatuses: []corev1.ContainerStatus{{
			Name: "snapshot", Image: "etcd", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}},
			LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1, Reason: "Error", Message: "context deadline exceeded"}},
		}},
	})
	expectRehearsed(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonCopyFailed, "init container snapshot CrashLoopBackOff, last exited 1 (Error): context deadline exceeded")
	expectNoCopy(t, cp)

	stuck := rehearsing(t)
	setCopy(t, eventuallyCopy(t, stuck, get(t, stuck).Generation), corev1.PodStatus{Phase: corev1.PodRunning, PodIP: copyIP, ContainerStatuses: []corev1.ContainerStatus{{
		Name: "kube-apiserver", Started: new(true), Image: "registry.k8s.io/kube-apiserver:" + version,
		State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.Now()}},
	}}})
	rehearsalClock.offset.Store(int64(6 * time.Minute))
	t.Cleanup(func() { rehearsalClock.offset.Store(0) })
	poke(t, stuck)
	expectRehearsed(t, stuck, metav1.ConditionFalse, lodiav1alpha1.ReasonAPIServerFailed, "runs and is not ready 5m0s after the pod's creation")
	expectNoCopy(t, stuck)
	rehearsalClock.offset.Store(0)

	unscheduled := rehearsing(t)
	eventuallyCopy(t, unscheduled, get(t, unscheduled).Generation)
	rehearsalClock.offset.Store(int64(16 * time.Minute))
	t.Cleanup(func() { rehearsalClock.offset.Store(0) })
	poke(t, unscheduled)
	expectRehearsed(t, unscheduled, metav1.ConditionFalse, lodiav1alpha1.ReasonCopyFailed, "within 15m0s of its creation: the pod is Pending")
	expectNoCopy(t, unscheduled)
}

func TestACopyItsNodeEndsIsCopyFailedWhateverRanBefore(t *testing.T) {
	ended := func(state corev1.ContainerState) corev1.PodStatus {
		return corev1.PodStatus{
			Phase: corev1.PodFailed, Reason: "DeadlineExceeded", Message: "Pod was active on the node longer than the specified deadline", PodIP: copyIP,
			ContainerStatuses: []corev1.ContainerStatus{{Name: "kube-apiserver", Image: "registry.k8s.io/kube-apiserver:" + version, Started: new(false), State: state}},
		}
	}
	for _, c := range []struct {
		name   string
		status corev1.PodStatus
		want   string
	}{
		{"its deadline after the API server exited 0", ended(corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0, Reason: "Completed"}}),
			"its node ended the copy at " + version + " before a verdict: the pod is DeadlineExceeded: Pod was active on the node longer than the specified deadline; its kube-apiserver last exited 0 (Completed)"},
		{"its deadline after the API server was killed", ended(corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 137, Reason: "Error"}}),
			"the pod is DeadlineExceeded: Pod was active on the node longer than the specified deadline; its kube-apiserver last exited 137 (Error)"},
		{"an eviction of a ready copy", corev1.PodStatus{
			Phase: corev1.PodFailed, Reason: "Evicted", Message: "The node was low on resource: memory.", PodIP: copyIP,
			ContainerStatuses: []corev1.ContainerStatus{{Name: "kube-apiserver", Image: "registry.k8s.io/kube-apiserver:" + version, Started: new(true), Ready: true, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.Now()}}}},
		}, "its node ended the copy at " + version + " before a verdict: the pod is Evicted: The node was low on resource: memory."},
		{"an OOM kill of the API server", corev1.PodStatus{
			Phase: corev1.PodRunning, PodIP: copyIP,
			ContainerStatuses: []corev1.ContainerStatus{{
				Name: "kube-apiserver", Image: "registry.k8s.io/kube-apiserver:" + version,
				State:                corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}},
				LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 137, Reason: "OOMKilled"}},
			}},
		}, "its node ended the copy's kube-apiserver at " + version + " at its memory limit: exited 137 (OOMKilled)"},
	} {
		t.Run(c.name, func(t *testing.T) {
			cp := rehearsing(t)
			setCopy(t, eventuallyCopy(t, cp, get(t, cp).Generation), c.status)
			expectRehearsed(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonCopyFailed, c.want)
			expectNoCopy(t, cp)
		})
	}
}

func TestAnObjectTheCopyCannotReadStopsTheStepAndOneNeitherCanIsReported(t *testing.T) {
	ns := namespace(t)
	corrupt(t, copyServers.etcd, "/registry/podtemplates/"+ns+"/broken")
	corrupt(t, copyServers.etcd, "/registry/controllerrevisions/"+ns+"/broken")
	corrupt(t, copyServers.tenant, "/registry/controllerrevisions/"+ns+"/broken")
	cp := rehearsing(t)
	setCopy(t, eventuallyCopy(t, cp, get(t, cp).Generation), readyCopy())
	expectRehearsed(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonListFailed,
		"podtemplates of v1 lists through the tenant and not through the copy: ",
		"reported: ", "controllerrevisions of apps/v1 fails through the tenant too: ")
	expectNoCopy(t, cp)
}

func TestATenantThatAnswers503OnceDoesNotHideWhatTheCopyCannotRead(t *testing.T) {
	ns := namespace(t)
	corrupt(t, copyServers.etcd, "/registry/podtemplates/"+ns+"/broken")
	once := copyServers.inject(t, "tenant", "/api/v1/podtemplates", http.StatusServiceUnavailable, 1)
	cp := rehearsing(t)
	setCopy(t, eventuallyCopy(t, cp, get(t, cp).Generation), readyCopy())
	expectRehearsed(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonListFailed, "podtemplates of v1 lists through the tenant and not through the copy: ")
	if n := once.served.Load(); n != 1 {
		t.Errorf("the tenant answered 503 %d times, want once", n)
	}
}

func TestATransientAnswerOfTheCopyIsReadAgain(t *testing.T) {
	discovery := copyServers.inject(t, "copy", "/api", http.StatusTooManyRequests, 1)
	list := copyServers.inject(t, "copy", "/api/v1/configmaps", http.StatusServiceUnavailable, 1)
	cp := rehearsing(t)
	setCopy(t, eventuallyCopy(t, cp, get(t, cp).Generation), readyCopy())
	expectRehearsed(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonPassed)
	if discovery.served.Load() != 1 || list.served.Load() != 1 {
		t.Errorf("the copy answered 429 %d times and 503 %d times, want once each", discovery.served.Load(), list.served.Load())
	}
}

func TestAListThatStillFailsThroughTheCopyAtItsDeadlineStopsTheStep(t *testing.T) {
	always := copyServers.inject(t, "copy", "/api/v1/configmaps", http.StatusServiceUnavailable, -1)
	cp := rehearsing(t)
	setCopy(t, eventuallyCopy(t, cp, get(t, cp).Generation), readyCopy())
	testenv.Eventually(t, func() error {
		if always.served.Load() == 0 {
			return errors.New("the copy has not answered the list of configmaps")
		}
		return nil
	})
	expectRehearsed(t, cp, metav1.ConditionUnknown, lodiav1alpha1.ReasonRehearsing)
	rehearsalClock.offset.Store(int64(16 * time.Minute))
	t.Cleanup(func() { rehearsalClock.offset.Store(0) })
	poke(t, cp)
	expectRehearsed(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonListFailed, "the list of configmaps of v1 still fails after 15m0s: injected")
	expectNoCopy(t, cp)
}

func TestASweepGoesOnFromItsContinueTokenAfterItsPassBudget(t *testing.T) {
	ns := namespace(t)
	if err := copyServers.admin.Create(t.Context(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}); err != nil {
		t.Fatal(err)
	}
	for i := range 501 {
		if err := copyServers.admin.Create(t.Context(), &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: fmt.Sprintf("page-%03d", i)}}); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		if err := copyServers.admin.DeleteAllOf(context.Background(), &corev1.ConfigMap{}, client.InNamespace(ns)); err != nil {
			t.Error(err)
		}
	})
	pages := copyServers.slow(t, "/api/v1/configmaps", 20*time.Second, 40*time.Second)
	cp := rehearsing(t)
	setCopy(t, eventuallyCopy(t, cp, get(t, cp).Generation), readyCopy())
	testenv.EventuallyWithin(t, 2*time.Minute, func() error {
		if c, _ := rehearsed(t, cp); c == nil || c.Status == metav1.ConditionUnknown {
			return fmt.Errorf("Rehearsed is %+v", c)
		}
		return nil
	})
	expectRehearsed(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonPassed)
	queries := copyServers.queries(pages)
	if len(queries) != 3 || strings.Contains(queries[0], "continue=") || !strings.Contains(queries[1], "continue=") || queries[2] != queries[1] {
		t.Errorf("the copy's lists of configmaps were %q, want a first page, then the second cut by the pass's 45 s and read again with its continue token", queries)
	}
}

func corrupt(t *testing.T, endpoint, key string) {
	t.Helper()
	etcd, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := etcd.Delete(context.Background(), key); err != nil {
			t.Error(err)
		}
		_ = etcd.Close()
	})
	if _, err := etcd.Put(t.Context(), key, "k8s\x00not an object"); err != nil {
		t.Fatal(err)
	}
}

func TestAnAggregatedAPIAndAWebhookConvertedCRDAreReportedNotRead(t *testing.T) {
	service := &apiregistrationv1.APIService{
		ObjectMeta: metav1.ObjectMeta{Name: "v1beta1.metrics.rehearsal.example"},
		Spec: apiregistrationv1.APIServiceSpec{
			Group: "metrics.rehearsal.example", Version: "v1beta1", GroupPriorityMinimum: 100, VersionPriority: 100, InsecureSkipTLSVerify: true,
			Service: &apiregistrationv1.ServiceReference{Namespace: "default", Name: "missing"},
		},
	}
	converted := &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "widgets.rehearsal.example"},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: "rehearsal.example", Scope: apiextensionsv1.NamespaceScoped,
			Names: apiextensionsv1.CustomResourceDefinitionNames{Plural: "widgets", Singular: "widget", Kind: "Widget", ListKind: "WidgetList"},
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
				{Name: "v1", Served: true, Storage: true, Schema: &apiextensionsv1.CustomResourceValidation{OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{Type: "object", XPreserveUnknownFields: new(true)}}},
				{Name: "v2", Served: true, Schema: &apiextensionsv1.CustomResourceValidation{OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{Type: "object", XPreserveUnknownFields: new(true)}}},
			},
			Conversion: &apiextensionsv1.CustomResourceConversion{Strategy: apiextensionsv1.WebhookConverter, Webhook: &apiextensionsv1.WebhookConversion{
				ClientConfig:             &apiextensionsv1.WebhookClientConfig{Service: &apiextensionsv1.ServiceReference{Namespace: "default", Name: "missing", Path: new("/convert")}},
				ConversionReviewVersions: []string{"v1"},
			}},
		},
	}
	for _, c := range []client.Client{apis, copyServers.admin} {
		for _, obj := range []client.Object{service.DeepCopy(), converted.DeepCopy()} {
			if err := c.Create(t.Context(), obj); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := client.IgnoreNotFound(c.Delete(context.Background(), obj)); err != nil {
					t.Error(err)
				}
			})
		}
	}
	cp := rehearsing(t)
	before := len(copyServers.read())
	setCopy(t, eventuallyCopy(t, cp, get(t, cp).Generation), readyCopy())
	expectRehearsed(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonPassed,
		"metrics.rehearsal.example/v1beta1 is served by an APIService that names a Service, not read through a copy",
		"widgets.rehearsal.example is converted by a webhook, not read through a copy")
	for _, path := range copyServers.read()[before:] {
		if strings.HasPrefix(path, "/apis/metrics.rehearsal.example/") || strings.HasPrefix(path, "/apis/rehearsal.example/") {
			t.Errorf("the sweep read %s through the copy", path)
		}
	}
}
