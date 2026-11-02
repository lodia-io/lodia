// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"testing"
	"time"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	apiregistrationv1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/controller/health"
)

const actsOn = "spec.restoreFrom.release is Held; once released, the controller manager acts on: "

func TestAHeldRestoreNamesWhatItsControllerManagerActsOn(t *testing.T) {
	kubeconfig, inside := startTenant(t, func(api *envtest.APIServer) {
		api.Configure().Set("service-cluster-ip-range", "10.0.0.0/16")
	})
	for _, ns := range []string{"shop", "billing"} {
		create(t, inside, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}})
	}
	for _, name := range []string{"web", "api", "db", "cache"} {
		create(t, inside, loadBalancer("shop", name))
	}
	create(t, inside, &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: "shop", Name: "internal"},
		Spec:       corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 80}}},
	})
	for name, policy := range map[string]corev1.PersistentVolumeReclaimPolicy{"pv-b": corev1.PersistentVolumeReclaimDelete, "pv-a": corev1.PersistentVolumeReclaimDelete, "pv-kept": corev1.PersistentVolumeReclaimRetain} {
		create(t, inside, &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec: corev1.PersistentVolumeSpec{
				Capacity:                      corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
				AccessModes:                   []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				PersistentVolumeReclaimPolicy: policy,
				PersistentVolumeSource:        corev1.PersistentVolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/data/" + name}},
			},
		})
	}
	for _, name := range []string{"data", "logs"} {
		create(t, inside, &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Namespace: "shop", Name: name},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources:   corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")}},
			},
		})
	}
	bound := &corev1.PersistentVolumeClaim{}
	if err := inside.Get(t.Context(), client.ObjectKey{Namespace: "shop", Name: "logs"}, bound); err != nil {
		t.Fatal(err)
	}
	bound.Status.Phase = corev1.ClaimBound
	if err := inside.Status().Update(t.Context(), bound); err != nil {
		t.Fatal(err)
	}
	url := "https://hooks.example.com/validate"
	create(t, inside, &admissionregistrationv1.ValidatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{Name: "guard"},
		Webhooks: []admissionregistrationv1.ValidatingWebhook{
			validating("c.guard.example.com", nil, nil),
			validating("b.guard.example.com", &url, new(admissionregistrationv1.Ignore)),
			validating("a.guard.example.com", nil, new(admissionregistrationv1.Ignore)),
		},
	})
	inject := validating("x.audit.example.com", nil, new(admissionregistrationv1.Fail))
	create(t, inside, &admissionregistrationv1.MutatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{Name: "audit"},
		Webhooks: []admissionregistrationv1.MutatingWebhook{{
			Name: inject.Name, ClientConfig: inject.ClientConfig, Rules: inject.Rules, FailurePolicy: inject.FailurePolicy,
			SideEffects: inject.SideEffects, AdmissionReviewVersions: inject.AdmissionReviewVersions,
		}},
	})
	create(t, inside, &apiregistrationv1.APIService{
		ObjectMeta: metav1.ObjectMeta{Name: "v1.widgets.example.com"},
		Spec: apiregistrationv1.APIServiceSpec{
			Group: "widgets.example.com", Version: "v1", GroupPriorityMinimum: 1000, VersionPriority: 15, InsecureSkipTLSVerify: true,
			Service: &apiregistrationv1.ServiceReference{Namespace: "shop", Name: "widgets"},
		},
	})
	for name, suspended := range map[string]bool{"nightly": false, "hourly": true} {
		create(t, inside, &batchv1.CronJob{
			ObjectMeta: metav1.ObjectMeta{Namespace: "shop", Name: name},
			Spec: batchv1.CronJobSpec{
				Schedule: "0 * * * *",
				Suspend:  new(suspended),
				JobTemplate: batchv1.JobTemplateSpec{Spec: batchv1.JobSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers:    []corev1.Container{{Name: "job", Image: "registry.example/job"}},
				}}}},
			},
		})
	}

	cp := restoredControlPlane(t, lodiav1alpha1.RestoreHeld)
	setAccess(t, cp)
	writeKubeconfig(t, cp, kubeconfig)
	r := &health.Reconciler{Client: k8s, Reader: k8s, Period: time.Hour, Timeout: 2 * time.Second, Clock: clock.RealClock{}}
	reconcile(t, r, cp)
	rest := "; 2 PersistentVolumes with reclaimPolicy Delete (pv-a, pv-b); 1 pending PersistentVolumeClaims (shop/data)" +
		"; 3 webhooks that call a URL or fail closed (audit/x.audit.example.com, guard/b.guard.example.com, guard/c.guard.example.com)" +
		"; 1 APIServices served by a Service (v1.widgets.example.com); 1 CronJobs not suspended (shop/nightly)"
	if err := expectLeading(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonAwaitingRelease, actsOn+"4 LoadBalancer Services (shop/api, shop/cache, shop/db…)"+rest); err != nil {
		t.Fatal(err)
	}

	for i := range 501 {
		create(t, inside, loadBalancer("billing", fmt.Sprintf("svc-%03d", i)))
	}
	reconcile(t, r, cp)
	if err := expectLeading(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonAwaitingRelease,
		actsOn+"500 or more LoadBalancer Services (billing/svc-000, billing/svc-001, billing/svc-002…)"+rest); err != nil {
		t.Error(err)
	}
}

func TestAKindTheTenantDoesNotServeIsSaidInItsPlace(t *testing.T) {
	kubeconfig, _ := startTenant(t, func(api *envtest.APIServer) {
		api.Configure().Set("runtime-config", "batch/v1=false")
	})
	cp := restoredControlPlane(t, lodiav1alpha1.RestoreHeld)
	setAccess(t, cp)
	writeKubeconfig(t, cp, kubeconfig)
	reconcile(t, &health.Reconciler{Client: k8s, Reader: k8s, Period: time.Hour, Timeout: 2 * time.Second, Clock: clock.RealClock{}}, cp)
	if err := expectLeading(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonAwaitingRelease, actsOn+"0 LoadBalancer Services; 0 PersistentVolumes with reclaimPolicy Delete"+
		"; 0 pending PersistentVolumeClaims; 0 webhooks that call a URL or fail closed; 0 APIServices served by a Service; CronJobs could not be listed"); err != nil {
		t.Error(err)
	}
}

func TestEachListWaitsItsOwnTimeout(t *testing.T) {
	cfg, err := clientcmd.RESTConfigFromKubeConfig(tenantKubeconfig)
	if err != nil {
		t.Fatal(err)
	}
	transport, err := rest.TransportFor(cfg)
	if err != nil {
		t.Fatal(err)
	}
	target, err := url.Parse(cfg.Host)
	if err != nil {
		t.Fatal(err)
	}
	forward := httputil.NewSingleHostReverseProxy(target)
	forward.Transport = transport
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path == "/api/v1/services" {
			<-req.Context().Done()
			return
		}
		forward.ServeHTTP(w, req)
	}))
	t.Cleanup(proxy.Close)
	kubeconfig, err := kubeconfigOf(proxy.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	cp := restoredControlPlane(t, lodiav1alpha1.RestoreHeld)
	setAccess(t, cp)
	writeKubeconfig(t, cp, kubeconfig)
	reconcile(t, &health.Reconciler{Client: k8s, Reader: k8s, Period: time.Hour, Timeout: 500 * time.Millisecond, Clock: clock.RealClock{}}, cp)
	if err := expectLeading(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonAwaitingRelease, actsOn+"Services could not be listed; 0 PersistentVolumes with reclaimPolicy Delete"+
		"; 0 pending PersistentVolumeClaims; 0 webhooks that call a URL or fail closed; 0 APIServices served by a Service; 0 CronJobs not suspended"); err != nil {
		t.Error(err)
	}
}

func TestARestoreLeadsOnceReleasedWithItsSourceGone(t *testing.T) {
	cp := restoredControlPlane(t, lodiav1alpha1.RestoreHeld)
	prod := &lodiav1alpha1.ControlPlane{
		ObjectMeta: metav1.ObjectMeta{Name: "prod", Namespace: cp.Namespace},
		Spec:       *cp.Spec.DeepCopy(),
	}
	prod.Spec.RestoreFrom, prod.Spec.Paused = nil, true
	if err := k8s.Create(t.Context(), prod); err != nil {
		t.Fatal(err)
	}
	setRestore(t, cp, prod)
	setAccess(t, cp)
	writeKubeconfig(t, cp, tenantKubeconfig)
	removeLeases(t)
	r := &health.Reconciler{Client: k8s, Reader: k8s, Period: 10 * time.Second, Timeout: 2 * time.Second, Clock: clock.RealClock{}}
	reconcile(t, r, cp)
	if err := expectLeading(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonAwaitingRelease, actsOn+"0 LoadBalancer Services; 0 PersistentVolumes with reclaimPolicy Delete"+
		"; 0 pending PersistentVolumeClaims; 0 webhooks that call a URL or fail closed; 0 APIServices served by a Service; 0 CronJobs not suspended"); err != nil {
		t.Fatal(err)
	}

	releaseRestore(t, cp)
	reconcile(t, r, cp)
	if err := expectLeading(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonSourceExists,
		"ControlPlane prod (UID "+string(prod.UID)+"), the Backup's source, still exists in the namespace; delete it to release this one"); err != nil {
		t.Fatal(err)
	}

	if err := k8s.Delete(t.Context(), prod); err != nil {
		t.Fatal(err)
	}
	reconcile(t, r, cp)
	if err := expectLeading(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonControllerManagerNotLeading,
		"Lease kube-system/kube-controller-manager does not exist; Lease kube-system/kube-scheduler does not exist"); err != nil {
		t.Error(err)
	}
}

func TestAReleasedRestoreNotCompleteReadsTheLeases(t *testing.T) {
	cp := restoredControlPlane(t, lodiav1alpha1.RestoreReleased)
	setAccess(t, cp)
	writeKubeconfig(t, cp, tenantKubeconfig)
	removeLeases(t)
	reconcile(t, &health.Reconciler{Client: k8s, Reader: k8s, Period: 10 * time.Second, Timeout: 2 * time.Second, Clock: clock.RealClock{}}, cp)
	if err := expectLeading(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonControllerManagerNotLeading,
		"Lease kube-system/kube-controller-manager does not exist; Lease kube-system/kube-scheduler does not exist"); err != nil {
		t.Error(err)
	}
}

func startTenant(t *testing.T, configure func(*envtest.APIServer)) ([]byte, client.Client) {
	t.Helper()
	env := &envtest.Environment{UseExistingCluster: new(false)}
	configure(env.ControlPlane.GetAPIServer())
	cfg, err := env.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := env.Stop(); err != nil {
			t.Error(err)
		}
	})
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{clientgoscheme.AddToScheme, apiregistrationv1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	inside, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	kubeconfig, err := kubeconfigOf(cfg.Host, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return kubeconfig, inside
}

func create(t *testing.T, c client.Client, obj client.Object) {
	t.Helper()
	if err := c.Create(t.Context(), obj); err != nil {
		t.Fatal(err)
	}
}

func loadBalancer(ns, name string) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec: corev1.ServiceSpec{
			Type:                          corev1.ServiceTypeLoadBalancer,
			Ports:                         []corev1.ServicePort{{Port: 443}},
			AllocateLoadBalancerNodePorts: new(false),
		},
	}
}

func validating(name string, url *string, policy *admissionregistrationv1.FailurePolicyType) admissionregistrationv1.ValidatingWebhook {
	config := admissionregistrationv1.WebhookClientConfig{URL: url}
	if url == nil {
		config.Service = &admissionregistrationv1.ServiceReference{Namespace: "shop", Name: "hooks"}
	}
	return admissionregistrationv1.ValidatingWebhook{
		Name:         name,
		ClientConfig: config,
		Rules: []admissionregistrationv1.RuleWithOperations{{
			Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Create},
			Rule:       admissionregistrationv1.Rule{APIGroups: []string{"widgets.example.com"}, APIVersions: []string{"v1"}, Resources: []string{"widgets"}},
		}},
		FailurePolicy:           policy,
		SideEffects:             new(admissionregistrationv1.SideEffectClassNone),
		AdmissionReviewVersions: []string{"v1"},
	}
}

func restoredControlPlane(t *testing.T, release lodiav1alpha1.RestoreRelease) *lodiav1alpha1.ControlPlane {
	t.Helper()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "health-"}}
	if err := k8s.Create(t.Context(), ns); err != nil {
		t.Fatal(err)
	}
	cp := &lodiav1alpha1.ControlPlane{
		ObjectMeta: metav1.ObjectMeta{Name: "dev", Namespace: ns.Name},
		Spec: lodiav1alpha1.ControlPlaneSpec{
			Version:     "v1.35.0",
			Datastore:   lodiav1alpha1.DatastoreChoice{Dedicated: &lodiav1alpha1.DedicatedDatastore{}},
			Endpoint:    lodiav1alpha1.Endpoint{LoadBalancer: &lodiav1alpha1.LoadBalancerEndpoint{}},
			Network:     lodiav1alpha1.Network{ServiceCIDRs: []lodiav1alpha1.CIDR{"10.96.0.0/16"}, PodCIDRs: []lodiav1alpha1.CIDR{"10.244.0.0/16"}},
			RestoreFrom: &lodiav1alpha1.RestoreFrom{BackupRef: lodiav1alpha1.LocalReference{Name: "prod-backup"}, Release: release},
		},
	}
	if err := k8s.Create(t.Context(), cp); err != nil {
		t.Fatal(err)
	}
	return cp
}

func setRestore(t *testing.T, cp, source *lodiav1alpha1.ControlPlane) {
	t.Helper()
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": lodiav1alpha1.GroupVersion.String(),
		"kind":       "ControlPlane",
		"metadata":   map[string]any{"name": cp.Name, "namespace": cp.Namespace},
		"status": map[string]any{"datastore": map[string]any{
			"endpoints": []any{map[string]any{"host": "dev-etcd-client." + cp.Namespace + ".svc", "port": int64(2379)}},
			"members":   map[string]any{"total": int64(1), "ready": int64(1)},
			"restore": map[string]any{
				"backup":      map[string]any{"name": "prod-backup", "uid": "5f3a9c1e-2b7d-4e8f-a6c0-d4b1e9f27a35"},
				"source":      map[string]any{"name": source.Name, "uid": string(source.UID)},
				"completedAt": time.Now().UTC().Format(time.RFC3339),
			},
		}},
	}}
	if err := k8s.Status().Apply(t.Context(), client.ApplyConfigurationFromUnstructured(u), client.FieldOwner("test/datastore"), client.ForceOwnership); err != nil {
		t.Fatal(err)
	}
}

func releaseRestore(t *testing.T, cp *lodiav1alpha1.ControlPlane) {
	t.Helper()
	fresh := get(t, cp)
	fresh.Spec.RestoreFrom.Release = lodiav1alpha1.RestoreReleased
	if err := k8s.Update(t.Context(), fresh); err != nil {
		t.Fatal(err)
	}
}
