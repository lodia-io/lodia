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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	apiregistrationv1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
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
	refuse  atomic.Bool
	admin   client.Client
	etcd    string
	tenant  string
	mu      sync.Mutex
	paths   []string
	tenants []string
}

var (
	copyServers  *copies
	testenvAdmin *rest.Config
	apis         client.Client
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
	c := &copies{admin: admin, etcd: env.SecondEtcd(), tenant: env.Etcd()}
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.Transport = transport
	c.Server = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.mu.Lock()
		c.paths = append(c.paths, r.URL.Path)
		c.mu.Unlock()
		proxy.ServeHTTP(w, r)
	}))
	c.TLS = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	c.StartTLS()
	copyServers = c
	return c
}

func (c *copies) dial(ctx context.Context, network, address string) (net.Conn, error) {
	if address == net.JoinHostPort(copyIP, "6443") {
		if c.refuse.Load() {
			return nil, errors.New("connection refused")
		}
		address = c.Listener.Addr().String()
	} else {
		c.mu.Lock()
		c.tenants = append(c.tenants, address)
		c.mu.Unlock()
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
	cp := controlPlane(t, loadBalancer())
	setTargetAt(t, cp, rehearsedFrom, "")
	applyStatus(t, cp, "test/endpoint", map[string]any{"endpoint": map[string]any{
		"apiServer":    map[string]any{"host": lbAddress, "port": int64(443)},
		"konnectivity": map[string]any{"host": lbAddress, "port": int64(8132)},
		"inCluster":    map[string]any{"host": lbAddress, "port": int64(6443)},
	}})
	setDatastore(t, cp, lodiav1alpha1.HostPort{Host: "dev-etcd-client." + cp.Namespace + ".svc", Port: 2379})
	writeKubeconfig(t, cp)
	applyStatus(t, cp, "test/lifecycle", step(lodiav1alpha1.UpgradeStepRehearsal))
	t.Cleanup(func() { endRehearsal(t, cp) })
	return cp
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
		pod := &corev1.Pod{}
		if err := k8s.Get(ctx, client.ObjectKey{Namespace: cp.Namespace, Name: cp.Name + "-rehearsal"}, pod); apierrors.IsNotFound(err) {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Errorf("the copy of %s/%s is still there after its step", cp.Namespace, cp.Name)
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
	pod := &corev1.Pod{}
	err := k8s.Get(t.Context(), client.ObjectKey{Namespace: cp.Namespace, Name: cp.Name + "-rehearsal"}, pod)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	return pod
}

func eventuallyCopy(t *testing.T, cp *lodiav1alpha1.ControlPlane, generation int64) *corev1.Pod {
	t.Helper()
	var pod *corev1.Pod
	testenv.Eventually(t, func() error {
		if pod = copyOf(t, cp); pod == nil || pod.Annotations["lodia.io/rehearsal-generation"] != fmt.Sprint(generation) || !pod.DeletionTimestamp.IsZero() {
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
