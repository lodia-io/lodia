// Copyright 2026 The Brygga Authors.
// SPDX-License-Identifier: Apache-2.0

package datastore_test

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	druidapicommon "github.com/gardener/etcd-druid/api/common"
	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	"github.com/gardener/etcd-druid/api/core/v1alpha1/crds"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/yaml"

	bryggav1alpha1 "github.com/segla-io/brygga/api/v1alpha1"
	"github.com/segla-io/brygga/internal/controller/datastore"
	"github.com/segla-io/brygga/internal/controller/datastore/datastoretest"
	"github.com/segla-io/brygga/internal/operator"
)

var k8s client.WithWatch

var datastoreRules = []rbacv1.PolicyRule{
	{APIGroups: []string{bryggav1alpha1.GroupVersion.Group}, Resources: []string{"controlplanes"}, Verbs: []string{"list", "watch", "patch"}},
	{APIGroups: []string{bryggav1alpha1.GroupVersion.Group}, Resources: []string{"controlplanes/status"}, Verbs: []string{"patch"}},
	{APIGroups: []string{bryggav1alpha1.GroupVersion.Group}, Resources: []string{"controlplanes/finalizers"}, Verbs: []string{"update"}},
	{APIGroups: []string{bryggav1alpha1.GroupVersion.Group}, Resources: []string{"datastoreclasses"}, Verbs: []string{"list", "watch"}},
	{APIGroups: []string{druidv1alpha1.SchemeGroupVersion.Group}, Resources: []string{"etcds"}, Verbs: []string{"get", "list", "watch", "create", "patch", "delete"}},
	{APIGroups: []string{""}, Resources: []string{"secrets"}, Verbs: []string{"get", "list", "watch", "create", "patch", "delete"}},
	{APIGroups: []string{""}, Resources: []string{"persistentvolumeclaims"}, Verbs: []string{"list", "patch", "delete"}},
	{APIGroups: []string{appsv1.GroupName}, Resources: []string{"statefulsets"}, Verbs: []string{"get"}},
}

func TestMain(m *testing.M) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		fmt.Println("skipping the datastore controller tests: KUBEBUILDER_ASSETS is not set; run make test")
		os.Exit(0)
	}
	druidCRDs, err := druidCRDs()
	if err != nil {
		fmt.Fprintln(os.Stderr, "etcd-druid CRDs:", err)
		os.Exit(1)
	}
	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
		CRDs:                  druidCRDs,
		UseExistingCluster:    new(false),
	}
	env.ControlPlane.GetAPIServer().Configure().Append("enable-admission-plugins", "OwnerReferencesPermissionEnforcement")
	cfg, err := env.Start()
	if err != nil {
		fmt.Fprintln(os.Stderr, "start envtest:", err)
		os.Exit(1)
	}
	stop := func(what string, err error) {
		fmt.Fprintln(os.Stderr, what+":", err)
		_ = env.Stop()
		os.Exit(1)
	}
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{clientgoscheme.AddToScheme, bryggav1alpha1.AddToScheme, druidv1alpha1.AddToScheme} {
		if err := add(scheme); err != nil {
			stop("scheme", err)
		}
	}
	if k8s, err = client.NewWithWatch(cfg, client.Options{Scheme: scheme}); err != nil {
		stop("client", err)
	}
	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))
	controller, err := asDatastoreController(cfg)
	if err != nil {
		stop("the controller's ClusterRole", err)
	}
	cache, err := operator.Cache()
	if err != nil {
		stop("cache options", err)
	}
	cache.ReaderFailOnMissingInformer = true
	mgr, err := ctrl.NewManager(controller, ctrl.Options{Scheme: scheme, Metrics: metricsserver.Options{BindAddress: "0"}, Cache: cache})
	if err != nil {
		stop("manager", err)
	}
	if err := (&datastore.Reconciler{Client: mgr.GetClient(), Reader: mgr.GetAPIReader()}).SetupWithManager(mgr, 1); err != nil {
		stop("controller", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan error, 1)
	go func() { started <- mgr.Start(ctx) }()
	code := m.Run()
	cancel()
	if err := <-started; err != nil {
		fmt.Fprintln(os.Stderr, "manager:", err)
		code = 1
	}
	if err := env.Stop(); err != nil {
		fmt.Fprintln(os.Stderr, "stop envtest:", err)
	}
	os.Exit(code)
}

func asDatastoreController(cfg *rest.Config) (*rest.Config, error) {
	const user = "brygga-datastore"
	ctx := context.Background()
	role := &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: user}, Rules: datastoreRules}
	binding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: user},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: role.Name},
		Subjects:   []rbacv1.Subject{{APIGroup: rbacv1.GroupName, Kind: rbacv1.UserKind, Name: user}},
	}
	for _, obj := range []client.Object{role, binding} {
		if err := k8s.Create(ctx, obj); err != nil {
			return nil, err
		}
	}
	err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		review := &authorizationv1.SubjectAccessReview{Spec: authorizationv1.SubjectAccessReviewSpec{
			User:               user,
			ResourceAttributes: &authorizationv1.ResourceAttributes{Group: bryggav1alpha1.GroupVersion.Group, Resource: "controlplanes", Verb: "watch"},
		}}
		if err := k8s.Create(ctx, review); err != nil {
			return false, err
		}
		return review.Status.Allowed, nil
	})
	if err != nil {
		return nil, err
	}
	controller := rest.CopyConfig(cfg)
	controller.Impersonate = rest.ImpersonationConfig{UserName: user}
	return controller, nil
}

func druidCRDs() ([]*apiextensionsv1.CustomResourceDefinition, error) {
	manifests, err := crds.GetAll("1.35")
	if err != nil {
		return nil, err
	}
	var out []*apiextensionsv1.CustomResourceDefinition
	for name, manifest := range manifests {
		crd := &apiextensionsv1.CustomResourceDefinition{}
		if err := yaml.Unmarshal([]byte(manifest), crd); err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		out = append(out, crd)
	}
	return out, nil
}

func eventually(t *testing.T, check func() error) {
	t.Helper()
	var last error
	err := wait.PollUntilContextTimeout(t.Context(), 100*time.Millisecond, 30*time.Second, true, func(context.Context) (bool, error) {
		last = check()
		return last == nil, nil
	})
	if err != nil {
		t.Fatalf("%v: %v", err, last)
	}
}

func namespace(t *testing.T) string {
	t.Helper()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "datastore-"}}
	if err := k8s.Create(t.Context(), ns); err != nil {
		t.Fatal(err)
	}
	return ns.Name
}

func create(t *testing.T, obj client.Object) {
	t.Helper()
	if err := k8s.Create(t.Context(), obj); err != nil {
		t.Fatal(err)
	}
}

func createClass(t *testing.T, class *bryggav1alpha1.DatastoreClass) {
	t.Helper()
	create(t, class)
	t.Cleanup(func() {
		if err := client.IgnoreNotFound(k8s.Delete(context.Background(), class)); err != nil {
			t.Error(err)
		}
	})
}

func newClass(name string, reclaimPolicy string) *bryggav1alpha1.DatastoreClass {
	return &bryggav1alpha1.DatastoreClass{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       bryggav1alpha1.DatastoreClassSpec{Storage: bryggav1alpha1.EtcdStorage{Size: resource.MustParse("1Gi"), ReclaimPolicy: reclaimPolicy}},
	}
}

func fullClass(name, credentialsNamespace string) *bryggav1alpha1.DatastoreClass {
	return &bryggav1alpha1.DatastoreClass{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: bryggav1alpha1.DatastoreClassSpec{
			Etcd: &bryggav1alpha1.EtcdSettings{
				QuotaBackendBytes: new(resource.MustParse("2Gi")),
				AutoCompaction:    &bryggav1alpha1.AutoCompaction{Mode: "revision", Retention: "1000"},
				Defragmentation:   &bryggav1alpha1.Defragmentation{Schedule: "0 3 * * *"},
				Resources: &corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("30m"), corev1.ResourceMemory: resource.MustParse("150M")},
					Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("2Gi")},
				},
			},
			Storage: bryggav1alpha1.EtcdStorage{ClassName: "fast", Size: resource.MustParse("20Gi"), ReclaimPolicy: "Retain"},
			Backup: &bryggav1alpha1.EtcdBackup{
				Store: bryggav1alpha1.BackupStore{
					Provider: "S3", Bucket: "brygga-backups", Prefix: "prod", Region: "eu-central-1", Endpoint: "https://s3.example.test",
					SecretRef: bryggav1alpha1.SecretReference{Name: "s3", Namespace: credentialsNamespace},
				},
				FullSnapshotSchedule: "0 2 * * *",
				DeltaSnapshotPeriod:  &metav1.Duration{Duration: 5 * time.Minute},
				Retention:            &bryggav1alpha1.BackupRetention{Days: 30},
			},
			Placement: &bryggav1alpha1.Placement{
				NodeSelector:      map[string]string{"node-role.example.com/control-planes": "true"},
				Tolerations:       []corev1.Toleration{{Key: "control-planes", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule}},
				PriorityClassName: "control-plane",
			},
		},
	}
}

func controlPlane(ns, name, className string) *bryggav1alpha1.ControlPlane {
	return &bryggav1alpha1.ControlPlane{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: bryggav1alpha1.ControlPlaneSpec{
			Version:   "v1.37.1",
			Datastore: bryggav1alpha1.DatastoreChoice{Dedicated: &bryggav1alpha1.DedicatedDatastore{ClassName: className}},
			Endpoint:  bryggav1alpha1.Endpoint{LoadBalancer: &bryggav1alpha1.LoadBalancerEndpoint{}},
			Network:   bryggav1alpha1.Network{ServiceCIDRs: []bryggav1alpha1.CIDR{"10.96.0.0/16"}, PodCIDRs: []bryggav1alpha1.CIDR{"10.244.0.0/16"}},
		},
	}
}

func credentials(ns string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "s3", Namespace: ns},
		Data:       map[string][]byte{"accessKeyID": []byte("spike-access"), "secretAccessKey": []byte("spike-secret-key"), "s3ForcePathStyle": []byte("true")},
	}
}

func writeGeneration(t *testing.T, ns, cp string, generation int, only ...bryggav1alpha1.EtcdCertificateRole) {
	t.Helper()
	secrets, err := datastoretest.Generation(getControlPlane(t, ns, cp), generation)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range secrets {
		if len(only) > 0 && !slices.ContainsFunc(only, func(certificate bryggav1alpha1.EtcdCertificateRole) bool {
			return secret.Name == bryggav1alpha1.EtcdCertificateSecretName(cp, certificate, generation)
		}) {
			continue
		}
		create(t, secret)
	}
}

func getControlPlane(t *testing.T, ns, name string) *bryggav1alpha1.ControlPlane {
	t.Helper()
	cp := &bryggav1alpha1.ControlPlane{}
	if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: name}, cp); err != nil {
		t.Fatal(err)
	}
	return cp
}

func consistently(t *testing.T, check func() error) {
	t.Helper()
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
		if err := check(); err != nil {
			t.Fatal(err)
		}
	}
}

func setPaused(t *testing.T, ns, name string, paused bool) {
	t.Helper()
	cp := getControlPlane(t, ns, name)
	patch := client.MergeFrom(cp.DeepCopy())
	cp.Spec.Paused = paused
	if err := k8s.Patch(t.Context(), cp, patch); err != nil {
		t.Fatal(err)
	}
}

func waitCondition(t *testing.T, ns, name string, status metav1.ConditionStatus, reason, message string) {
	t.Helper()
	eventually(t, func() error {
		cp := &bryggav1alpha1.ControlPlane{}
		if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: name}, cp); err != nil {
			return err
		}
		condition := meta.FindStatusCondition(cp.Status.Conditions, bryggav1alpha1.ConditionDatastoreReady)
		switch {
		case condition == nil:
			return fmt.Errorf("no %s condition", bryggav1alpha1.ConditionDatastoreReady)
		case condition.Status != status || condition.Reason != reason || !strings.Contains(condition.Message, message):
			return fmt.Errorf("want %s/%s with %q, got %s/%s %q", status, reason, message, condition.Status, condition.Reason, condition.Message)
		case condition.ObservedGeneration != cp.Generation:
			return fmt.Errorf("observedGeneration %d, generation %d", condition.ObservedGeneration, cp.Generation)
		}
		return nil
	})
}

func getEtcd(t *testing.T, ns, cp string) *druidv1alpha1.Etcd {
	t.Helper()
	etcd := &druidv1alpha1.Etcd{}
	if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: cp + "-etcd"}, etcd); err != nil {
		t.Fatal(err)
	}
	return etcd
}

func waitEtcd(t *testing.T, ns, cp string, check func(*druidv1alpha1.Etcd) error) *druidv1alpha1.Etcd {
	t.Helper()
	etcd := &druidv1alpha1.Etcd{}
	eventually(t, func() error {
		if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: cp + "-etcd"}, etcd); err != nil {
			return err
		}
		if check != nil {
			return check(etcd)
		}
		return nil
	})
	return etcd
}

func namedGeneration(etcd *druidv1alpha1.Etcd) (int, error) {
	if etcd.Spec.Etcd.ClientUrlTLS == nil || etcd.Spec.Etcd.PeerUrlTLS == nil || etcd.Spec.Backup.TLS == nil {
		return 0, fmt.Errorf("TLS is not set on client, peer and backup together: %+v", etcd.Spec)
	}
	var names []string
	generations := map[int]bool{}
	for _, tls := range []druidv1alpha1.TLSConfig{*etcd.Spec.Etcd.ClientUrlTLS, etcd.Spec.Etcd.PeerUrlTLS.TLSConfig, *etcd.Spec.Backup.TLS} {
		for _, ref := range []corev1.SecretReference{tls.TLSCASecretRef.SecretReference, tls.ServerTLSSecretRef, tls.ClientTLSSecretRef} {
			if ref.Name == "" {
				continue
			}
			names = append(names, ref.Name)
			generation, err := strconv.Atoi(ref.Name[strings.LastIndex(ref.Name, "-")+1:])
			if err != nil {
				return 0, fmt.Errorf("%s names no generation", ref.Name)
			}
			generations[generation] = true
		}
	}
	if len(generations) != 1 {
		return 0, fmt.Errorf("the TLS blocks mix generations: %s", strings.Join(names, ", "))
	}
	for generation := range generations {
		return generation, nil
	}
	return 0, nil
}

func namesGeneration(want int) func(*druidv1alpha1.Etcd) error {
	return func(etcd *druidv1alpha1.Etcd) error {
		got, err := namedGeneration(etcd)
		if err != nil {
			return err
		}
		if got != want {
			return fmt.Errorf("the Etcd names generation %d, want %d", got, want)
		}
		return nil
	}
}

func settle(t *testing.T, ns, name string) {
	t.Helper()
	cp := getControlPlane(t, ns, name)
	patch := client.MergeFrom(cp.DeepCopy())
	version := cp.Spec.Version
	dot := strings.LastIndex(version, ".")
	next, err := strconv.Atoi(version[dot+1:])
	if err != nil {
		t.Fatal(err)
	}
	cp.Spec.Version = version[:dot+1] + strconv.Itoa(next+1)
	if err := k8s.Patch(t.Context(), cp, patch); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() error {
		current := getControlPlane(t, ns, name)
		condition := meta.FindStatusCondition(current.Status.Conditions, bryggav1alpha1.ConditionDatastoreReady)
		if condition == nil || condition.ObservedGeneration != cp.Generation {
			return fmt.Errorf("generation %d not yet reported: %+v", cp.Generation, condition)
		}
		return nil
	})
}

func tls(c *druidv1alpha1.TLSConfig) string {
	if c == nil {
		return ""
	}
	return fmt.Sprintf("ca %s/%s server %s client %s", c.TLSCASecretRef.Name, ptr.Deref(c.TLSCASecretRef.DataKey, ""), c.ServerTLSSecretRef.Name, c.ClientTLSSecretRef.Name)
}

func peerTLS(etcd *druidv1alpha1.Etcd) string {
	if etcd.Spec.Etcd.PeerUrlTLS == nil {
		return ""
	}
	return tls(&etcd.Spec.Etcd.PeerUrlTLS.TLSConfig)
}

func quantity(q *resource.Quantity) string {
	if q == nil {
		return ""
	}
	return q.String()
}

func resources(r *corev1.ResourceRequirements) string {
	if r == nil {
		return ""
	}
	return fmt.Sprintf("requests cpu=%s memory=%s limits memory=%s", r.Requests.Cpu(), r.Requests.Memory(), r.Limits.Memory())
}

func TestFinalizerAndEtcdFromClass(t *testing.T) {
	ns, credentialsNamespace := namespace(t), namespace(t)
	create(t, credentials(credentialsNamespace))
	createClass(t, fullClass("mapped", credentialsNamespace))
	create(t, controlPlane(ns, "dev", "mapped"))

	eventually(t, func() error {
		if !controllerutil.ContainsFinalizer(getControlPlane(t, ns, "dev"), bryggav1alpha1.DatastoreFinalizer) {
			return fmt.Errorf("the finalizer %s is not on the ControlPlane", bryggav1alpha1.DatastoreFinalizer)
		}
		return nil
	})
	waitCondition(t, ns, "dev", metav1.ConditionFalse, bryggav1alpha1.ReasonWaitingForCertificates, "waiting for a complete generation of the etcd certificates")
	if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: "dev-etcd"}, &druidv1alpha1.Etcd{}); !apierrors.IsNotFound(err) {
		t.Fatalf("want no Etcd before the first certificate generation, got %v", err)
	}

	writeGeneration(t, ns, "dev", 1)
	etcd := waitEtcd(t, ns, "dev", namesGeneration(1))
	var cp *bryggav1alpha1.ControlPlane
	eventually(t, func() error {
		if cp = getControlPlane(t, ns, "dev"); cp.Status.Datastore == nil {
			return errors.New("status.datastore is not written yet")
		}
		return nil
	})
	store := ptr.Deref(etcd.Spec.Backup.Store, druidv1alpha1.StoreSpec{})
	rows := []struct {
		field     string
		got, want any
	}{
		{"spec.replicas", etcd.Spec.Replicas, int32(1)},
		{"etcd.quotaBackendBytes → spec.etcd.quota", quantity(etcd.Spec.Etcd.Quota), "2Gi"},
		{"etcd.autoCompaction.mode → spec.sharedConfig.autoCompactionMode", ptr.Deref(etcd.Spec.Common.AutoCompactionMode, ""), druidv1alpha1.Revision},
		{"etcd.autoCompaction.retention → spec.sharedConfig.autoCompactionRetention", ptr.Deref(etcd.Spec.Common.AutoCompactionRetention, ""), "1000"},
		{"etcd.defragmentation.schedule → spec.etcd.defragmentationSchedule", ptr.Deref(etcd.Spec.Etcd.DefragmentationSchedule, ""), "0 3 * * *"},
		{"etcd.resources → spec.etcd.resources", resources(etcd.Spec.Etcd.Resources), "requests cpu=30m memory=150M limits memory=2Gi"},
		{"storage.className → spec.storageClass", ptr.Deref(etcd.Spec.StorageClass, ""), "fast"},
		{"storage.size → spec.storageCapacity", quantity(etcd.Spec.StorageCapacity), "20Gi"},
		{"backup.store.provider → spec.backup.store.provider", ptr.Deref(store.Provider, ""), druidv1alpha1.StorageProvider("S3")},
		{"backup.store.bucket → spec.backup.store.container", ptr.Deref(store.Container, ""), "brygga-backups"},
		{"backup.store.prefix → spec.backup.store.prefix, the tenant and its UID below the class's prefix", store.Prefix, "prod/" + ns + "/dev/" + string(cp.UID)},
		{"backup.store.endpoint → spec.backup.store.endpointOverride", ptr.Deref(store.EndpointOverride, ""), "https://s3.example.test"},
		{"backup.store.secretRef → spec.backup.store.secretRef, the copy in the namespace", ptr.Deref(store.SecretRef, corev1.SecretReference{}).Name, "dev-etcd-backup"},
		{"backup.fullSnapshotSchedule → spec.backup.fullSnapshotSchedule", ptr.Deref(etcd.Spec.Backup.FullSnapshotSchedule, ""), "0 2 * * *"},
		{"backup.deltaSnapshotPeriod → spec.backup.deltaSnapshotPeriod", ptr.Deref(etcd.Spec.Backup.DeltaSnapshotPeriod, metav1.Duration{}).Duration, 5 * time.Minute},
		{"backup.retention.days → spec.backup.garbageCollectionPolicy", ptr.Deref(etcd.Spec.Backup.GarbageCollectionPolicy, ""), druidv1alpha1.GarbageCollectionPolicy("LimitBased")},
		{"backup.retention.days → spec.backup.maxBackupsLimitBasedGC", ptr.Deref(etcd.Spec.Backup.MaxBackupsLimitBasedGC, 0), int32(30)},
		{"placement.nodeSelector → spec.schedulingConstraints.affinity", etcd.Spec.SchedulingConstraints.Affinity, &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{
				{Key: "node-role.example.com/control-planes", Operator: corev1.NodeSelectorOpIn, Values: []string{"true"}},
			}}}},
		}}},
		{"placement.tolerations → spec.schedulingConstraints.tolerations", etcd.Spec.SchedulingConstraints.Tolerations,
			[]corev1.Toleration{{Key: "control-planes", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule}}},
		{"placement.priorityClassName → spec.priorityClassName", ptr.Deref(etcd.Spec.PriorityClassName, ""), "control-plane"},
		{"generation 1 → spec.etcd.clientUrlTls", tls(etcd.Spec.Etcd.ClientUrlTLS), "ca dev-etcd-ca-1/" + bryggav1alpha1.CACertKey + " server dev-etcd-server-1 client dev-etcd-client-1"},
		{"generation 1 → spec.etcd.peerUrlTls", peerTLS(etcd), "ca dev-etcd-ca-1/" + bryggav1alpha1.CACertKey + " server dev-etcd-peer-1 client "},
		{"generation 1 → spec.backup.tls", tls(etcd.Spec.Backup.TLS), "ca dev-etcd-ca-1/" + bryggav1alpha1.CACertKey + " server dev-etcd-backup-restore-1 client dev-etcd-client-1"},
		{"the ControlPlane → metadata.labels", etcd.Labels[bryggav1alpha1.ControlPlaneLabel], "dev"},
		{"the ControlPlane → spec.labels", etcd.Spec.Labels[bryggav1alpha1.ControlPlaneLabel], "dev"},
		{"the ControlPlane → metadata.ownerReferences", etcd.OwnerReferences, []metav1.OwnerReference{{
			APIVersion: "brygga.segla.io/v1alpha1", Kind: "ControlPlane", Name: "dev", UID: cp.UID, Controller: new(true), BlockOwnerDeletion: new(true),
		}}},
		{"the Etcd → status.datastore", ptr.Deref(cp.Status.Datastore, bryggav1alpha1.DatastoreStatus{}), bryggav1alpha1.DatastoreStatus{
			Endpoints: []bryggav1alpha1.HostPort{{Host: "dev-etcd-client." + ns + ".svc", Port: 2379}},
			Class:     &bryggav1alpha1.DatastoreClassStatus{Name: "mapped", Generation: 1},
			Members:   bryggav1alpha1.Count{Total: 1},
		}},
		{"the class → metadata.annotations[" + bryggav1alpha1.DatastoreClassAnnotation + "]", etcd.Annotations[bryggav1alpha1.DatastoreClassAnnotation], "mapped"},
		{"the class's generation → metadata.annotations[" + bryggav1alpha1.DatastoreClassGenerationAnnotation + "]", etcd.Annotations[bryggav1alpha1.DatastoreClassGenerationAnnotation], "1"},
	}
	for _, row := range rows {
		t.Run(row.field, func(t *testing.T) {
			if !reflect.DeepEqual(row.got, row.want) {
				t.Errorf("got %+v, want %+v", row.got, row.want)
			}
		})
	}
}

func TestConditionsFromState(t *testing.T) {
	ns := namespace(t)
	createClass(t, newClass("state", "Retain"))
	events, err := k8s.Watch(t.Context(), &bryggav1alpha1.ControlPlaneList{}, client.InNamespace(ns))
	if err != nil {
		t.Fatal(err)
	}
	defer events.Stop()
	create(t, controlPlane(ns, "dev", "state"))
	var first *metav1.Condition
	for first == nil {
		select {
		case event := <-events.ResultChan():
			if cp, ok := event.Object.(*bryggav1alpha1.ControlPlane); ok && event.Type == watch.Modified {
				first = meta.FindStatusCondition(cp.Status.Conditions, bryggav1alpha1.ConditionDatastoreReady)
			}
		case <-time.After(30 * time.Second):
			t.Fatal("no DatastoreReady condition was written")
		}
	}
	if first.Status != metav1.ConditionUnknown || first.Reason != bryggav1alpha1.ReasonReconciling {
		t.Errorf("the first DatastoreReady is %s/%s %q, want Unknown/%s", first.Status, first.Reason, first.Message, bryggav1alpha1.ReasonReconciling)
	}

	writeGeneration(t, ns, "dev", 1)
	waitCondition(t, ns, "dev", metav1.ConditionFalse, bryggav1alpha1.ReasonProvisioning, "etcd-druid has not observed generation 1 of Etcd dev-etcd")
	var before *bryggav1alpha1.ControlPlane
	eventually(t, func() error {
		if before = getControlPlane(t, ns, "dev"); before.Status.Datastore == nil {
			return errors.New("status.datastore is not written yet")
		}
		return nil
	})
	injected := errors.New("injected: the API server did not answer")
	failing := interceptor.NewClient(k8s, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*druidv1alpha1.Etcd); ok {
				return injected
			}
			return c.Get(ctx, key, obj, opts...)
		},
	})
	reconciler := &datastore.Reconciler{Client: failing, Reader: failing}
	if _, err := reconciler.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "dev"}}); !errors.Is(err, injected) {
		t.Fatalf("a pass whose Etcd read fails returned %v, want the read's error", err)
	}
	if after := getControlPlane(t, ns, "dev"); after.ResourceVersion != before.ResourceVersion {
		condition := meta.FindStatusCondition(after.Status.Conditions, bryggav1alpha1.ConditionDatastoreReady)
		t.Errorf("a pass whose Etcd read fails moved the ControlPlane from resourceVersion %s to %s: DatastoreReady %s/%s %q, status.datastore %+v",
			before.ResourceVersion, after.ResourceVersion, condition.Status, condition.Reason, condition.Message, after.Status.Datastore)
	}
}

func TestEtcdRefused(t *testing.T) {
	ns := namespace(t)
	createClass(t, newClass("refused", "Retain"))
	policy := &admissionregistrationv1.ValidatingAdmissionPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "refuse-etcds-in-" + ns},
		Spec: admissionregistrationv1.ValidatingAdmissionPolicySpec{
			FailurePolicy: new(admissionregistrationv1.Fail),
			MatchConstraints: &admissionregistrationv1.MatchResources{ResourceRules: []admissionregistrationv1.NamedRuleWithOperations{{
				RuleWithOperations: admissionregistrationv1.RuleWithOperations{
					Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Create, admissionregistrationv1.Update},
					Rule:       admissionregistrationv1.Rule{APIGroups: []string{druidv1alpha1.SchemeGroupVersion.Group}, APIVersions: []string{"v1alpha1"}, Resources: []string{"etcds"}},
				},
			}}},
			Validations: []admissionregistrationv1.Validation{{Expression: "object.metadata.namespace != '" + ns + "'", Message: "no Etcd in " + ns}},
		},
	}
	binding := &admissionregistrationv1.ValidatingAdmissionPolicyBinding{
		ObjectMeta: metav1.ObjectMeta{Name: policy.Name},
		Spec: admissionregistrationv1.ValidatingAdmissionPolicyBindingSpec{
			PolicyName:        policy.Name,
			ValidationActions: []admissionregistrationv1.ValidationAction{admissionregistrationv1.Deny},
		},
	}
	for _, obj := range []client.Object{policy, binding} {
		create(t, obj)
		t.Cleanup(func() {
			if err := client.IgnoreNotFound(k8s.Delete(context.Background(), obj)); err != nil {
				t.Error(err)
			}
		})
	}
	eventually(t, func() error {
		probe := &druidv1alpha1.Etcd{ObjectMeta: metav1.ObjectMeta{Name: "probe", Namespace: ns}, Spec: druidv1alpha1.EtcdSpec{Replicas: 1, Labels: map[string]string{"app": "etcd"}}}
		if err := k8s.Create(t.Context(), probe, client.DryRunAll); err == nil {
			return errors.New("the policy does not refuse Etcds yet")
		}
		return nil
	})

	create(t, controlPlane(ns, "dev", "refused"))
	writeGeneration(t, ns, "dev", 1)
	waitCondition(t, ns, "dev", metav1.ConditionFalse, bryggav1alpha1.ReasonEtcdRefused,
		"the API server refuses Etcd dev-etcd on certificate generation 1 with the fields of DatastoreClass refused generation 1; the reason is in Brygga's log")
	if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: "dev-etcd"}, &druidv1alpha1.Etcd{}); !apierrors.IsNotFound(err) {
		t.Fatalf("want no Etcd while the API server refuses it, got %v", err)
	}

	if err := k8s.Delete(t.Context(), binding); err != nil {
		t.Fatal(err)
	}
	waitEtcd(t, ns, "dev", namesGeneration(1))
	waitCondition(t, ns, "dev", metav1.ConditionFalse, bryggav1alpha1.ReasonProvisioning, "etcd-druid has not observed generation 1 of Etcd dev-etcd")
}

func TestPaused(t *testing.T) {
	ns := namespace(t)
	createClass(t, newClass("paused", "Delete"))
	paused := controlPlane(ns, "dev", "paused")
	paused.Spec.Paused = true
	create(t, paused)
	writeGeneration(t, ns, "dev", 1)
	consistently(t, func() error {
		cp := getControlPlane(t, ns, "dev")
		if controllerutil.ContainsFinalizer(cp, bryggav1alpha1.DatastoreFinalizer) || meta.FindStatusCondition(cp.Status.Conditions, bryggav1alpha1.ConditionDatastoreReady) != nil {
			return fmt.Errorf("a paused ControlPlane was reconciled: finalizers %v, conditions %+v", cp.Finalizers, cp.Status.Conditions)
		}
		if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: "dev-etcd"}, &druidv1alpha1.Etcd{}); !apierrors.IsNotFound(err) {
			return fmt.Errorf("want no Etcd for a paused ControlPlane, got %w", err)
		}
		return nil
	})

	setPaused(t, ns, "dev", false)
	waitEtcd(t, ns, "dev", namesGeneration(1))
	waitCondition(t, ns, "dev", metav1.ConditionFalse, bryggav1alpha1.ReasonProvisioning, "etcd-druid has not observed generation 1 of Etcd dev-etcd")
	if !controllerutil.ContainsFinalizer(getControlPlane(t, ns, "dev"), bryggav1alpha1.DatastoreFinalizer) {
		t.Fatal("the finalizer is not on the unpaused ControlPlane")
	}

	setPaused(t, ns, "dev", true)
	writeGeneration(t, ns, "dev", 2)
	if err := k8s.Delete(t.Context(), getControlPlane(t, ns, "dev")); err != nil {
		t.Fatal(err)
	}
	consistently(t, func() error {
		cp := getControlPlane(t, ns, "dev")
		if !controllerutil.ContainsFinalizer(cp, bryggav1alpha1.DatastoreFinalizer) {
			return errors.New("the finalizer of a paused ControlPlane was released")
		}
		if condition := meta.FindStatusCondition(cp.Status.Conditions, bryggav1alpha1.ConditionDatastoreReady); condition.Reason == bryggav1alpha1.ReasonDeleting {
			return errors.New("a paused ControlPlane reports Deleting")
		}
		etcd := getEtcd(t, ns, "dev")
		if !etcd.DeletionTimestamp.IsZero() {
			return errors.New("the Etcd of a paused ControlPlane is being deleted")
		}
		return namesGeneration(1)(etcd)
	})

	setPaused(t, ns, "dev", false)
	eventually(t, func() error {
		return gone(k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: "dev"}, &bryggav1alpha1.ControlPlane{}))
	})
	if err := gone(k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: "dev-etcd"}, &druidv1alpha1.Etcd{})); err != nil {
		t.Errorf("the Etcd: %v", err)
	}
}

func TestNewestCompleteGeneration(t *testing.T) {
	ns := namespace(t)
	createClass(t, newClass("generations", "Retain"))
	create(t, controlPlane(ns, "dev", "generations"))
	writeGeneration(t, ns, "dev", 1)
	waitEtcd(t, ns, "dev", namesGeneration(1))

	writeGeneration(t, ns, "dev", 2, bryggav1alpha1.EtcdCARole, bryggav1alpha1.EtcdServerRole, bryggav1alpha1.EtcdPeerRole, bryggav1alpha1.EtcdBackupRestoreRole)
	settle(t, ns, "dev")
	if err := namesGeneration(1)(getEtcd(t, ns, "dev")); err != nil {
		t.Fatalf("four of five Secrets of generation 2: %v", err)
	}

	secrets, err := datastoretest.Generation(getControlPlane(t, ns, "dev"), 2)
	if err != nil {
		t.Fatal(err)
	}
	terminating := secrets[slices.IndexFunc(secrets, func(s *corev1.Secret) bool {
		return s.Name == bryggav1alpha1.EtcdCertificateSecretName("dev", bryggav1alpha1.EtcdClientRole, 2)
	})]
	label := terminating.Labels[bryggav1alpha1.EtcdCertificatesLabel]
	delete(terminating.Labels, bryggav1alpha1.EtcdCertificatesLabel)
	terminating.Finalizers = []string{"brygga.segla.io/test"}
	create(t, terminating)
	if err := k8s.Delete(t.Context(), terminating); err != nil {
		t.Fatal(err)
	}
	if err := k8s.Get(t.Context(), client.ObjectKeyFromObject(terminating), terminating); err != nil {
		t.Fatal(err)
	}
	patch := client.MergeFrom(terminating.DeepCopy())
	terminating.Labels[bryggav1alpha1.EtcdCertificatesLabel] = label
	if err := k8s.Patch(t.Context(), terminating, patch); err != nil {
		t.Fatal(err)
	}
	settle(t, ns, "dev")
	if err := namesGeneration(1)(getEtcd(t, ns, "dev")); err != nil {
		t.Fatalf("the fifth Secret of generation 2 being deleted: %v", err)
	}
	patch = client.MergeFrom(terminating.DeepCopy())
	terminating.Finalizers = nil
	if err := k8s.Patch(t.Context(), terminating, patch); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() error {
		err := k8s.Get(t.Context(), client.ObjectKeyFromObject(terminating), &corev1.Secret{})
		switch {
		case err == nil:
			return errors.New("the terminating Secret is still there")
		case !apierrors.IsNotFound(err):
			return err
		}
		return nil
	})

	writeGeneration(t, ns, "dev", 2, bryggav1alpha1.EtcdClientRole)
	etcd := waitEtcd(t, ns, "dev", namesGeneration(2))

	settle(t, ns, "dev")
	if again := getEtcd(t, ns, "dev"); again.ResourceVersion != etcd.ResourceVersion {
		t.Fatalf("a further reconcile moved the Etcd from resourceVersion %s to %s", etcd.ResourceVersion, again.ResourceVersion)
	}
}

func TestGenerationsMoveForward(t *testing.T) {
	ns := namespace(t)
	createClass(t, newClass("forward", "Retain"))
	create(t, controlPlane(ns, "dev", "forward"))
	writeGeneration(t, ns, "dev", 1)
	writeGeneration(t, ns, "dev", 2)
	waitEtcd(t, ns, "dev", namesGeneration(2))

	held := &corev1.Secret{}
	if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: bryggav1alpha1.EtcdCertificateSecretName("dev", bryggav1alpha1.EtcdClientRole, 2)}, held); err != nil {
		t.Fatal(err)
	}
	patch := client.MergeFrom(held.DeepCopy())
	held.Finalizers = []string{"brygga.segla.io/test"}
	if err := k8s.Patch(t.Context(), held, patch); err != nil {
		t.Fatal(err)
	}
	if err := k8s.Delete(t.Context(), held); err != nil {
		t.Fatal(err)
	}
	settle(t, ns, "dev")
	if err := namesGeneration(2)(getEtcd(t, ns, "dev")); err != nil {
		t.Fatalf("a Secret of generation 2 being deleted, generation 1 the newest complete: %v", err)
	}

	writeGeneration(t, ns, "dev", 3)
	waitEtcd(t, ns, "dev", namesGeneration(3))
	if err := k8s.Get(t.Context(), client.ObjectKeyFromObject(held), held); err != nil {
		t.Fatal(err)
	}
	patch = client.MergeFrom(held.DeepCopy())
	held.Finalizers = nil
	if err := k8s.Patch(t.Context(), held, patch); err != nil {
		t.Fatal(err)
	}
}

func TestOnlyItsOwnGenerationsCount(t *testing.T) {
	ns := namespace(t)
	createClass(t, newClass("owned", "Retain"))
	create(t, controlPlane(ns, "dev", "owned"))
	writeWith := func(generation int, owners []metav1.OwnerReference) {
		secrets, err := datastoretest.Generation(getControlPlane(t, ns, "dev"), generation)
		if err != nil {
			t.Fatal(err)
		}
		for _, secret := range secrets {
			secret.OwnerReferences = owners
			create(t, secret)
		}
	}
	writeWith(1, []metav1.OwnerReference{{
		APIVersion: "brygga.segla.io/v1alpha1", Kind: "ControlPlane", Name: "dev", UID: "11111111-2222-3333-4444-555555555555",
		Controller: new(true), BlockOwnerDeletion: new(true),
	}})
	writeWith(2, nil)
	settle(t, ns, "dev")
	waitCondition(t, ns, "dev", metav1.ConditionFalse, bryggav1alpha1.ReasonWaitingForCertificates, "waiting for a complete generation of the etcd certificates")
	if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: "dev-etcd"}, &druidv1alpha1.Etcd{}); !apierrors.IsNotFound(err) {
		t.Fatalf("want no Etcd on a predecessor's or an unowned generation, got %v", err)
	}

	writeGeneration(t, ns, "dev", 3)
	waitEtcd(t, ns, "dev", namesGeneration(3))
}

func TestMountedSecretsMustExist(t *testing.T) {
	ns := namespace(t)
	const cp = "mounted"
	createClass(t, newClass("mounting", "Retain"))
	create(t, controlPlane(ns, cp, "mounting"))
	writeGeneration(t, ns, cp, 1)
	writeGeneration(t, ns, cp, 2)
	waitEtcd(t, ns, cp, namesGeneration(2))
	readyOn(t, ns, cp, 2)

	for _, certificate := range bryggav1alpha1.EtcdCertificateRoles() {
		if err := k8s.Delete(t.Context(), &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: bryggav1alpha1.EtcdCertificateSecretName(cp, certificate, 2)}}); err != nil {
			t.Fatal(err)
		}
	}
	waitCondition(t, ns, cp, metav1.ConditionFalse, bryggav1alpha1.ReasonProvisioning,
		"Secrets Etcd mounted-etcd mounts do not exist: mounted-etcd-backup-restore-2, mounted-etcd-ca-2, mounted-etcd-client-2, mounted-etcd-peer-2, mounted-etcd-server-2")
	if err := namesGeneration(2)(getEtcd(t, ns, cp)); err != nil {
		t.Fatalf("the Etcd left the generation it names for an older one: %v", err)
	}

	writeGeneration(t, ns, cp, 3)
	waitEtcd(t, ns, cp, namesGeneration(3))
	readyOn(t, ns, cp, 3)
	terminating := &corev1.Secret{}
	if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: bryggav1alpha1.EtcdCertificateSecretName(cp, bryggav1alpha1.EtcdCARole, 3)}, terminating); err != nil {
		t.Fatal(err)
	}
	held := client.MergeFrom(terminating.DeepCopy())
	terminating.Finalizers = []string{"brygga.segla.io/test"}
	if err := k8s.Patch(t.Context(), terminating, held); err != nil {
		t.Fatal(err)
	}
	if err := k8s.Delete(t.Context(), terminating); err != nil {
		t.Fatal(err)
	}
	settle(t, ns, cp)
	waitCondition(t, ns, cp, metav1.ConditionTrue, bryggav1alpha1.ReasonReady, "ready on certificate generation 3")
	released := client.MergeFrom(terminating.DeepCopy())
	terminating.Finalizers = nil
	if err := k8s.Patch(t.Context(), terminating, released); err != nil {
		t.Fatal(err)
	}
}

func TestDatastoreClassNotFound(t *testing.T) {
	ns := namespace(t)
	create(t, controlPlane(ns, "named", "absent"))
	waitCondition(t, ns, "named", metav1.ConditionFalse, bryggav1alpha1.ReasonDatastoreClassNotFound, "DatastoreClass absent does not exist")

	create(t, controlPlane(ns, "unnamed", ""))
	waitCondition(t, ns, "unnamed", metav1.ConditionFalse, bryggav1alpha1.ReasonDatastoreClassNotFound, "no DatastoreClass is annotated")

	for _, name := range []string{"default-a", "default-b"} {
		class := newClass(name, "Retain")
		class.Annotations = map[string]string{bryggav1alpha1.DefaultClassAnnotation: "true"}
		class.Spec.Storage.Size = resource.MustParse("3Gi")
		createClass(t, class)
	}
	waitCondition(t, ns, "unnamed", metav1.ConditionFalse, bryggav1alpha1.ReasonDatastoreClassAmbiguous, "2 DatastoreClasses are annotated")

	if err := k8s.Delete(t.Context(), newClass("default-b", "")); err != nil {
		t.Fatal(err)
	}
	waitCondition(t, ns, "unnamed", metav1.ConditionFalse, bryggav1alpha1.ReasonWaitingForCertificates, "waiting for a complete generation")
	writeGeneration(t, ns, "unnamed", 1)
	waitEtcd(t, ns, "unnamed", func(etcd *druidv1alpha1.Etcd) error {
		if size := quantity(etcd.Spec.StorageCapacity); size != "3Gi" {
			return fmt.Errorf("storageCapacity %s is not the default class's 3Gi", size)
		}
		return nil
	})
}

func TestBackupCredentialsCopied(t *testing.T) {
	ns, credentialsNamespace := namespace(t), namespace(t)
	createClass(t, fullClass("backed-up", credentialsNamespace))
	create(t, controlPlane(ns, "backed", "backed-up"))
	writeGeneration(t, ns, "backed", 1)
	waitCondition(t, ns, "backed", metav1.ConditionFalse, bryggav1alpha1.ReasonProvisioning, "DatastoreClass backed-up cannot be applied yet: its credentials Secret "+credentialsNamespace+"/s3 is missing")

	source := credentials(credentialsNamespace)
	create(t, source)
	settle(t, ns, "backed")
	waitEtcd(t, ns, "backed", func(etcd *druidv1alpha1.Etcd) error {
		if etcd.Spec.Backup.Store == nil || ptr.Deref(etcd.Spec.Backup.Store.SecretRef, corev1.SecretReference{}).Name != "backed-etcd-backup" {
			return fmt.Errorf("the store does not name the copy: %+v", etcd.Spec.Backup.Store)
		}
		return nil
	})
	cp := getControlPlane(t, ns, "backed")
	copied := &corev1.Secret{}
	if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: "backed-etcd-backup"}, copied); err != nil {
		t.Fatal(err)
	}
	want := source.Data
	want["region"] = []byte("eu-central-1")
	if !reflect.DeepEqual(copied.Data, want) {
		t.Errorf("data %q, want the class Secret's data with the region: %q", copied.Data, want)
	}
	if copied.Labels[bryggav1alpha1.ControlPlaneLabel] != "backed" {
		t.Errorf("labels %v lack %s=dev", copied.Labels, bryggav1alpha1.ControlPlaneLabel)
	}
	if len(copied.OwnerReferences) != 1 || copied.OwnerReferences[0].UID != cp.UID || !ptr.Deref(copied.OwnerReferences[0].Controller, false) {
		t.Errorf("ownerReferences %+v do not name the ControlPlane", copied.OwnerReferences)
	}

	createClass(t, newClass("plain", "Retain"))
	create(t, controlPlane(ns, "plain", "plain"))
	writeGeneration(t, ns, "plain", 1)
	etcd := waitEtcd(t, ns, "plain", nil)
	if etcd.Spec.Backup.Store != nil {
		t.Errorf("a class without backup.store rendered a store: %+v", etcd.Spec.Backup.Store)
	}
	if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: "plain-etcd-backup"}, &corev1.Secret{}); !apierrors.IsNotFound(err) {
		t.Errorf("want no credentials copy without a store, got %v", err)
	}
}

func readyOn(t *testing.T, ns, cp string, generation int) {
	t.Helper()
	runStatefulSet(t, ns, cp, generation)
	setEtcdStatus(t, ns, cp, nil)
	waitCondition(t, ns, cp, metav1.ConditionTrue, bryggav1alpha1.ReasonReady, fmt.Sprintf("ready on certificate generation %d", generation))
}

func TestHeldClass(t *testing.T) {
	ns, credentialsNamespace := namespace(t), namespace(t)
	const cp = "held"
	create(t, credentials(credentialsNamespace))
	createClass(t, fullClass("holding", credentialsNamespace))
	create(t, controlPlane(ns, cp, "holding"))
	writeGeneration(t, ns, cp, 1)
	waitEtcd(t, ns, cp, namesGeneration(1))
	readyOn(t, ns, cp, 1)

	if err := k8s.Delete(t.Context(), credentials(credentialsNamespace)); err != nil {
		t.Fatal(err)
	}
	class := &bryggav1alpha1.DatastoreClass{}
	if err := k8s.Get(t.Context(), client.ObjectKey{Name: "holding"}, class); err != nil {
		t.Fatal(err)
	}
	class.Spec.Etcd.QuotaBackendBytes = new(resource.MustParse("3Gi"))
	if err := k8s.Update(t.Context(), class); err != nil {
		t.Fatal(err)
	}
	writeGeneration(t, ns, cp, 2)
	etcd := waitEtcd(t, ns, cp, namesGeneration(2))
	if quota, recorded := quantity(etcd.Spec.Etcd.Quota), etcd.Annotations[bryggav1alpha1.DatastoreClassGenerationAnnotation]; quota != "2Gi" || recorded != "1" {
		t.Errorf("quota %s from class generation %s: the held class's edit reached the Etcd", quota, recorded)
	}
	readyOn(t, ns, cp, 2)
	waitCondition(t, ns, cp, metav1.ConditionTrue, bryggav1alpha1.ReasonReady,
		"ready on certificate generation 2, 1 of 1 replicas; DatastoreClass holding is held: its credentials Secret "+credentialsNamespace+"/s3 is missing")
	if after := reconcileNow(t, ns, cp); after != time.Minute {
		t.Errorf("a held class requeues after %s, want a minute: the class's Secret is outside the cache", after)
	}

	create(t, credentials(credentialsNamespace))
	settle(t, ns, cp)
	waitEtcd(t, ns, cp, func(etcd *druidv1alpha1.Etcd) error {
		if quota := quantity(etcd.Spec.Etcd.Quota); quota != "3Gi" {
			return fmt.Errorf("quota %s: the class edit has not been applied since its Secret is back", quota)
		}
		return nil
	})
	setEtcdStatus(t, ns, cp, nil)
	waitCondition(t, ns, cp, metav1.ConditionTrue, bryggav1alpha1.ReasonReady, "ready on certificate generation 2, 1 of 1 replicas")
	if c := meta.FindStatusCondition(getControlPlane(t, ns, cp).Status.Conditions, bryggav1alpha1.ConditionDatastoreReady); strings.Contains(c.Message, "is held") {
		t.Errorf("the class is still reported held: %s", c.Message)
	}
}

func TestBackupCopyFollowsItsEvents(t *testing.T) {
	ns, credentialsNamespace := namespace(t), namespace(t)
	const cp = "copied"
	source := credentials(credentialsNamespace)
	create(t, source)
	createClass(t, fullClass("copying", credentialsNamespace))
	create(t, controlPlane(ns, cp, "copying"))
	writeGeneration(t, ns, cp, 1)
	waitEtcd(t, ns, cp, namesGeneration(1))
	readyOn(t, ns, cp, 1)
	key := client.ObjectKey{Namespace: ns, Name: cp + "-etcd-backup"}

	first := getCopy(t, ns, cp)
	if err := k8s.Delete(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	copied := &corev1.Secret{}
	eventually(t, func() error {
		if err := k8s.Get(t.Context(), key, copied); err != nil {
			return err
		}
		if copied.UID == first.UID {
			return errors.New("the deleted copy is still there")
		}
		return nil
	})

	held := client.MergeFrom(copied.DeepCopy())
	copied.Finalizers = []string{"brygga.segla.io/test"}
	if err := k8s.Patch(t.Context(), copied, held); err != nil {
		t.Fatal(err)
	}
	source.Data["secretAccessKey"] = []byte("rotated")
	if err := k8s.Update(t.Context(), source); err != nil {
		t.Fatal(err)
	}
	if err := k8s.Delete(t.Context(), copied); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() error {
		if err := k8s.Get(t.Context(), key, copied); err != nil {
			return err
		}
		if copied.DeletionTimestamp.IsZero() || string(copied.Data["secretAccessKey"]) != "rotated" {
			return fmt.Errorf("the terminating copy holds %q, deletionTimestamp %v", copied.Data["secretAccessKey"], copied.DeletionTimestamp)
		}
		return nil
	})
	waitCondition(t, ns, cp, metav1.ConditionTrue, bryggav1alpha1.ReasonReady, "ready on certificate generation 1, 1 of 1 replicas")
	released := client.MergeFrom(copied.DeepCopy())
	copied.Finalizers = nil
	if err := k8s.Patch(t.Context(), copied, released); err != nil {
		t.Fatal(err)
	}
}

func TestPredecessorsCopy(t *testing.T) {
	ns, credentialsNamespace := namespace(t), namespace(t)
	const cp = "successor"
	create(t, credentials(credentialsNamespace))
	createClass(t, fullClass("succeeding", credentialsNamespace))
	leftover := func() *corev1.Secret {
		return &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name: cp + "-etcd-backup", Namespace: ns, Labels: map[string]string{bryggav1alpha1.ControlPlaneLabel: cp},
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: "brygga.segla.io/v1alpha1", Kind: "ControlPlane", Name: cp, UID: "11111111-2222-3333-4444-555555555555",
					Controller: new(true), BlockOwnerDeletion: new(true),
				}},
			},
			Data: map[string][]byte{"accessKeyID": []byte("the predecessor's")},
		}
	}
	predecessors := leftover()
	create(t, predecessors)
	create(t, controlPlane(ns, cp, "succeeding"))
	writeGeneration(t, ns, cp, 1)
	const controlledBy = "successor-etcd-backup is controlled by ControlPlane successor with UID 11111111-2222-3333-4444-555555555555"
	waitCondition(t, ns, cp, metav1.ConditionFalse, bryggav1alpha1.ReasonProvisioning, "DatastoreClass succeeding cannot be applied yet: "+controlledBy)
	settle(t, ns, cp)
	if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: cp + "-etcd"}, &druidv1alpha1.Etcd{}); !apierrors.IsNotFound(err) {
		t.Fatalf("want no Etcd while a predecessor's copy stands, got %v", err)
	}
	if again := getCopy(t, ns, cp); again.ResourceVersion != predecessors.ResourceVersion {
		t.Fatalf("the predecessor's copy moved from resourceVersion %s to %s", predecessors.ResourceVersion, again.ResourceVersion)
	}

	if err := k8s.Delete(t.Context(), predecessors); err != nil {
		t.Fatal(err)
	}
	waitEtcd(t, ns, cp, namesGeneration(1))
	if owners := getCopy(t, ns, cp).OwnerReferences; len(owners) != 1 || owners[0].UID != getControlPlane(t, ns, cp).UID {
		t.Fatalf("the copy's owners are %+v, want the ControlPlane", owners)
	}

	setPaused(t, ns, cp, true)
	if err := k8s.Delete(t.Context(), getCopy(t, ns, cp)); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() error {
		return gone(k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: cp + "-etcd-backup"}, &corev1.Secret{}))
	})
	predecessors = leftover()
	create(t, predecessors)
	setPaused(t, ns, cp, false)
	waitCondition(t, ns, cp, metav1.ConditionFalse, bryggav1alpha1.ReasonProvisioning, "; DatastoreClass succeeding is held: "+controlledBy)
	writeGeneration(t, ns, cp, 2)
	waitEtcd(t, ns, cp, namesGeneration(2))
	if again := getCopy(t, ns, cp); again.ResourceVersion != predecessors.ResourceVersion {
		t.Errorf("the predecessor's copy moved from resourceVersion %s to %s", predecessors.ResourceVersion, again.ResourceVersion)
	}
}

func TestOwnerlessCopyIsAdopted(t *testing.T) {
	ns, credentialsNamespace := namespace(t), namespace(t)
	const cp = "adopter"
	create(t, credentials(credentialsNamespace))
	createClass(t, fullClass("adopting", credentialsNamespace))
	create(t, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: cp + "-etcd-backup", Namespace: ns, Labels: map[string]string{bryggav1alpha1.ControlPlaneLabel: cp}}})
	create(t, controlPlane(ns, cp, "adopting"))
	writeGeneration(t, ns, cp, 1)
	waitEtcd(t, ns, cp, namesGeneration(1))
	copied := getCopy(t, ns, cp)
	if owners := copied.OwnerReferences; len(owners) != 1 || owners[0].UID != getControlPlane(t, ns, cp).UID || !ptr.Deref(owners[0].Controller, false) {
		t.Errorf("the copy's owners are %+v, want the ControlPlane as its controller", owners)
	}
	if got := string(copied.Data["secretAccessKey"]); got != "spike-secret-key" {
		t.Errorf("the adopted copy holds secretAccessKey %q, want the class Secret's", got)
	}
}

func getCopy(t *testing.T, ns, cp string) *corev1.Secret {
	t.Helper()
	secret := &corev1.Secret{}
	if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: cp + "-etcd-backup"}, secret); err != nil {
		t.Fatal(err)
	}
	return secret
}

func certificateVolumes(cp string, generation int) []corev1.Volume {
	var volumes []corev1.Volume
	for _, certificate := range bryggav1alpha1.EtcdCertificateRoles() {
		volumes = append(volumes, corev1.Volume{Name: string(certificate), VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: bryggav1alpha1.EtcdCertificateSecretName(cp, certificate, generation)}}})
	}
	return volumes
}

func runStatefulSet(t *testing.T, ns, cp string, generation int) {
	t.Helper()
	sts := &appsv1.StatefulSet{}
	err := k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: cp + "-etcd"}, sts)
	switch {
	case apierrors.IsNotFound(err):
		labels := druidv1alpha1.GetDefaultLabels(metav1.ObjectMeta{Name: cp + "-etcd"})
		create(t, &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Name: cp + "-etcd", Namespace: ns, Labels: labels},
			Spec: appsv1.StatefulSetSpec{
				Replicas:    new(int32(1)),
				Selector:    &metav1.LabelSelector{MatchLabels: labels},
				ServiceName: cp + "-etcd-peer",
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: labels},
					Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "etcd", Image: "etcd"}}, Volumes: certificateVolumes(cp, generation)},
				},
			},
		})
	case err != nil:
		t.Fatal(err)
	default:
		sts.Spec.Template.Spec.Volumes = certificateVolumes(cp, generation)
		if err := k8s.Update(t.Context(), sts); err != nil {
			t.Fatal(err)
		}
	}
	setStatefulSetStatus(t, ns, cp, nil)
}

func setStatefulSetStatus(t *testing.T, ns, cp string, mutate func(*appsv1.StatefulSetStatus)) {
	t.Helper()
	sts := &appsv1.StatefulSet{}
	if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: cp + "-etcd"}, sts); err != nil {
		t.Fatal(err)
	}
	sts.Status = appsv1.StatefulSetStatus{ObservedGeneration: sts.Generation, Replicas: 1, ReadyReplicas: 1, CurrentReplicas: 1, UpdatedReplicas: 1, CurrentRevision: "rev", UpdateRevision: "rev"}
	if mutate != nil {
		mutate(&sts.Status)
	}
	if err := k8s.Status().Update(t.Context(), sts); err != nil {
		t.Fatal(err)
	}
}

func setEtcdStatus(t *testing.T, ns, cp string, mutate func(*druidv1alpha1.EtcdStatus)) {
	t.Helper()
	etcd := getEtcd(t, ns, cp)
	etcd.Status = druidv1alpha1.EtcdStatus{
		ObservedGeneration: new(etcd.Generation),
		Ready:              new(true),
		Replicas:           1,
		ReadyReplicas:      1,
		CurrentReplicas:    1,
		Conditions: []druidv1alpha1.Condition{{
			Type: druidv1alpha1.ConditionTypeAllMembersUpdated, Status: druidv1alpha1.ConditionTrue, Reason: "AllMembersUpdated",
			Message: "All members reflect latest desired spec", LastTransitionTime: metav1.Now(), LastUpdateTime: metav1.Now(),
		}},
		Members: []druidv1alpha1.EtcdMemberStatus{{
			Name: cp + "-etcd-0", ID: new("8e9e05c52164694d"), Role: new(druidv1alpha1.EtcdRoleLeader),
			Status: druidv1alpha1.EtcdMemberStatusReady, Reason: "LeaseSucceeded", LastTransitionTime: metav1.Now(),
		}},
	}
	if mutate != nil {
		mutate(&etcd.Status)
	}
	if err := k8s.Status().Update(t.Context(), etcd); err != nil {
		t.Fatal(err)
	}
}

func TestUnsupported(t *testing.T) {
	ns := namespace(t)
	shared := controlPlane(ns, "shared", "")
	shared.Spec.Datastore = bryggav1alpha1.DatastoreChoice{Shared: &bryggav1alpha1.SharedDatastore{Name: "pool"}}
	create(t, shared)
	waitCondition(t, ns, "shared", metav1.ConditionFalse, bryggav1alpha1.ReasonUnsupported, "spec.datastore.shared is not supported yet")

	createClass(t, newClass("unsupported", "Retain"))
	ha := controlPlane(ns, "ha", "unsupported")
	ha.Spec.HighAvailability = &bryggav1alpha1.HighAvailability{FailureTolerance: "Node"}
	create(t, ha)
	writeGeneration(t, ns, "ha", 1)
	waitEtcd(t, ns, "ha", nil)
	runStatefulSet(t, ns, "ha", 1)
	setEtcdStatus(t, ns, "ha", nil)
	waitCondition(t, ns, "ha", metav1.ConditionFalse, bryggav1alpha1.ReasonUnsupported, "spec.highAvailability is not supported yet: the etcd runs one member")
}

func TestDatastoreReadyFollowsEtcdStatus(t *testing.T) {
	ns := namespace(t)
	const cp = "reported"
	createClass(t, newClass("reported", "Retain"))
	create(t, controlPlane(ns, cp, "reported"))
	writeGeneration(t, ns, cp, 1)
	waitEtcd(t, ns, cp, nil)
	waitCondition(t, ns, cp, metav1.ConditionFalse, bryggav1alpha1.ReasonProvisioning, "etcd-druid has not observed generation 1 of Etcd reported-etcd, observedGeneration 0")

	runStatefulSet(t, ns, cp, 1)
	setEtcdStatus(t, ns, cp, nil)
	waitCondition(t, ns, cp, metav1.ConditionTrue, bryggav1alpha1.ReasonReady, "etcd-druid reports Etcd reported-etcd ready on certificate generation 1, 1 of 1 replicas")
	if got, want := ptr.Deref(getControlPlane(t, ns, cp).Status.Datastore, bryggav1alpha1.DatastoreStatus{}), (bryggav1alpha1.DatastoreStatus{
		Endpoints: []bryggav1alpha1.HostPort{{Host: "reported-etcd-client." + ns + ".svc", Port: 2379}},
		Class:     &bryggav1alpha1.DatastoreClassStatus{Name: "reported", Generation: 1},
		Members:   bryggav1alpha1.Count{Total: 1, Ready: 1},
	}); !reflect.DeepEqual(got, want) {
		t.Errorf("status.datastore %+v, want %+v", got, want)
	}

	rows := []struct {
		name    string
		etcd    func(*druidv1alpha1.EtcdStatus)
		sts     func(*appsv1.StatefulSetStatus)
		message string
	}{
		{"generation not observed", func(s *druidv1alpha1.EtcdStatus) { s.ObservedGeneration = new(*s.ObservedGeneration - 1) }, nil,
			"etcd-druid has not observed generation"},
		{"AllMembersUpdated False", func(s *druidv1alpha1.EtcdStatus) {
			s.Conditions[0].Status, s.Conditions[0].Reason, s.Conditions[0].Message = druidv1alpha1.ConditionFalse, "NotAllMembersUpdated", "At least one member is not yet updated"
		}, nil, "etcd-druid reports AllMembersUpdated=False on Etcd reported-etcd: At least one member is not yet updated"},
		{"AllMembersUpdated absent", func(s *druidv1alpha1.EtcdStatus) { s.Conditions = nil }, nil,
			"etcd-druid reports no AllMembersUpdated condition on Etcd reported-etcd"},
		{"not ready", func(s *druidv1alpha1.EtcdStatus) { s.Ready, s.ReadyReplicas = new(false), 0 }, nil,
			"etcd-druid reports Etcd reported-etcd not ready, 0 of 1 replicas"},
		{"StatefulSet generation not observed", nil, func(s *appsv1.StatefulSetStatus) { s.ObservedGeneration-- },
			"the StatefulSet controller has not observed generation 1 of StatefulSet reported-etcd, observedGeneration 0"},
		{"StatefulSet not updated", nil, func(s *appsv1.StatefulSetStatus) { s.UpdatedReplicas = 0 },
			"StatefulSet reported-etcd: 0 of 1 replicas updated, 1 ready"},
		{"StatefulSet not ready", nil, func(s *appsv1.StatefulSetStatus) { s.ReadyReplicas = 0 },
			"StatefulSet reported-etcd: 1 of 1 replicas updated, 0 ready"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			setEtcdStatus(t, ns, cp, row.etcd)
			setStatefulSetStatus(t, ns, cp, row.sts)
			settle(t, ns, cp)
			waitCondition(t, ns, cp, metav1.ConditionFalse, bryggav1alpha1.ReasonProvisioning, row.message)
			setEtcdStatus(t, ns, cp, nil)
			setStatefulSetStatus(t, ns, cp, nil)
			settle(t, ns, cp)
			waitCondition(t, ns, cp, metav1.ConditionTrue, bryggav1alpha1.ReasonReady, "ready on certificate generation 1")
		})
	}
	t.Run("last errors alone", func(t *testing.T) {
		setEtcdStatus(t, ns, cp, func(s *druidv1alpha1.EtcdStatus) {
			s.LastErrors = []druidapicommon.LastError{{Code: "ERR_SYNC_CLIENT_SERVICE", Description: "the server could not answer", ObservedAt: metav1.Now()}}
		})
		settle(t, ns, cp)
		waitCondition(t, ns, cp, metav1.ConditionTrue, bryggav1alpha1.ReasonReady, "ready on certificate generation 1")
		setEtcdStatus(t, ns, cp, nil)
	})

	writeGeneration(t, ns, cp, 2)
	waitEtcd(t, ns, cp, namesGeneration(2))
	setEtcdStatus(t, ns, cp, nil)
	settle(t, ns, cp)
	waitCondition(t, ns, cp, metav1.ConditionFalse, bryggav1alpha1.ReasonProvisioning, "StatefulSet reported-etcd does not mount certificate generation 2 yet")
	runStatefulSet(t, ns, cp, 2)
	settle(t, ns, cp)
	waitCondition(t, ns, cp, metav1.ConditionTrue, bryggav1alpha1.ReasonReady, "etcd-druid reports Etcd reported-etcd ready on certificate generation 2, 1 of 1 replicas")

	if err := k8s.Delete(t.Context(), &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: cp + "-etcd"}}); err != nil {
		t.Fatal(err)
	}
	settle(t, ns, cp)
	waitCondition(t, ns, cp, metav1.ConditionFalse, bryggav1alpha1.ReasonProvisioning, "StatefulSet reported-etcd does not exist")

	etcd := getEtcd(t, ns, cp)
	held := client.MergeFrom(etcd.DeepCopy())
	etcd.Finalizers = append(etcd.Finalizers, "brygga.segla.io/test")
	if err := k8s.Patch(t.Context(), etcd, held); err != nil {
		t.Fatal(err)
	}
	if err := k8s.Delete(t.Context(), etcd); err != nil {
		t.Fatal(err)
	}
	waitCondition(t, ns, cp, metav1.ConditionFalse, bryggav1alpha1.ReasonProvisioning, "Etcd reported-etcd is being deleted")
	released := client.MergeFrom(etcd.DeepCopy())
	etcd.Finalizers = nil
	if err := k8s.Patch(t.Context(), etcd, released); err != nil {
		t.Fatal(err)
	}
}

func claim(ns, name string, labels map[string]string) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Labels: labels},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources:   corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")}},
		},
	}
}

func getClaim(t *testing.T, ns, name string) *corev1.PersistentVolumeClaim {
	t.Helper()
	pvc := &corev1.PersistentVolumeClaim{}
	if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: name}, pvc); err != nil {
		t.Fatal(err)
	}
	return pvc
}

func removeClaim(t *testing.T, ns, name string) {
	t.Helper()
	pvc := getClaim(t, ns, name)
	if err := k8s.Delete(t.Context(), pvc); err != nil {
		t.Fatal(err)
	}
	pvc = getClaim(t, ns, name)
	patch := client.MergeFrom(pvc.DeepCopy())
	pvc.Finalizers = nil
	if err := k8s.Patch(t.Context(), pvc, patch); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() error {
		return gone(k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: name}, &corev1.PersistentVolumeClaim{}))
	})
}

func gone(err error) error {
	switch {
	case err == nil:
		return errors.New("still there")
	case !apierrors.IsNotFound(err):
		return err
	}
	return nil
}

func untouched(t *testing.T, ns, name string) {
	t.Helper()
	if pvc := getClaim(t, ns, name); !pvc.DeletionTimestamp.IsZero() {
		t.Errorf("claim %s is being deleted", name)
	}
}

func deleted(t *testing.T, ns, name string) {
	t.Helper()
	pvc := &corev1.PersistentVolumeClaim{}
	if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: name}, pvc); !apierrors.IsNotFound(err) && (err != nil || pvc.DeletionTimestamp.IsZero()) {
		t.Errorf("want claim %s deleted, got %v, deletionTimestamp %v", name, err, pvc.DeletionTimestamp)
	}
}

func TestVolumesExist(t *testing.T) {
	ns := namespace(t)
	createClass(t, newClass("reused", "Delete"))
	etcdLabels := druidv1alpha1.GetDefaultLabels(metav1.ObjectMeta{Name: "dev-etcd"})
	previous := maps.Clone(etcdLabels)
	previous[bryggav1alpha1.ControlPlaneUIDLabel] = "11111111-2222-3333-4444-555555555555"
	create(t, claim(ns, "dev-etcd-dev-etcd-0", nil))
	create(t, claim(ns, "dev-etcd-dev-etcd-1", previous))
	create(t, claim(ns, "dev-etcd-scratch", etcdLabels))
	create(t, controlPlane(ns, "dev", "reused"))
	writeGeneration(t, ns, "dev", 1)
	waitCondition(t, ns, "dev", metav1.ConditionFalse, bryggav1alpha1.ReasonVolumesExist,
		"dev-etcd-dev-etcd-0, dev-etcd-dev-etcd-1 of another ControlPlane stand where the etcd of dev would be created: delete them to start empty, or keep them, and this ControlPlane waits")
	if condition := meta.FindStatusCondition(getControlPlane(t, ns, "dev").Status.Conditions, bryggav1alpha1.ConditionDatastoreReady); strings.Contains(condition.Message, "dev-etcd-scratch") {
		t.Errorf("a claim the StatefulSet does not attach blocks the Etcd: %s", condition.Message)
	}
	if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: "dev-etcd"}, &druidv1alpha1.Etcd{}); !apierrors.IsNotFound(err) {
		t.Fatalf("want no Etcd on another ControlPlane's claims, got %v", err)
	}

	removeClaim(t, ns, "dev-etcd-dev-etcd-0")
	removeClaim(t, ns, "dev-etcd-dev-etcd-1")
	settle(t, ns, "dev")
	waitEtcd(t, ns, "dev", nil)
	cp := getControlPlane(t, ns, "dev")

	create(t, claim(ns, "dev-etcd-dev-etcd-0", nil))
	settle(t, ns, "dev")
	pvc := getClaim(t, ns, "dev-etcd-dev-etcd-0")
	if pvc.Labels[bryggav1alpha1.ControlPlaneUIDLabel] != string(cp.UID) || pvc.Annotations[bryggav1alpha1.DatastoreClassAnnotation] != "reused" {
		t.Errorf("the StatefulSet's claim carries labels %v and annotations %v, want the ControlPlane's UID %s and the class reused", pvc.Labels, pvc.Annotations, cp.UID)
	}
	if pvc := getClaim(t, ns, "dev-etcd-scratch"); pvc.Labels[bryggav1alpha1.ControlPlaneUIDLabel] != "" {
		t.Errorf("a claim the StatefulSet does not attach was labelled: %v", pvc.Labels)
	}
}

func TestForeignEtcd(t *testing.T) {
	createClass(t, newClass("foreign", "Delete"))
	rows := []struct {
		name, cp string
		labels   map[string]string
		owners   []metav1.OwnerReference
		message  string
	}{
		{"no controller", "orphaned", nil, nil, "Etcd orphaned-etcd is not this ControlPlane's, it has no controller: delete it and its claims to start empty, or keep them, and this ControlPlane waits"},
		{"another ControlPlane", "succeeded", map[string]string{bryggav1alpha1.ControlPlaneLabel: "succeeded"}, []metav1.OwnerReference{{
			APIVersion: "brygga.segla.io/v1alpha1", Kind: "ControlPlane", Name: "succeeded", UID: "11111111-2222-3333-4444-555555555555",
			Controller: new(true), BlockOwnerDeletion: new(true),
		}}, "Etcd succeeded-etcd is not this ControlPlane's, its controller is ControlPlane succeeded with UID 11111111-2222-3333-4444-555555555555"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			ns := namespace(t)
			etcd := &druidv1alpha1.Etcd{
				ObjectMeta: metav1.ObjectMeta{Name: row.cp + "-etcd", Namespace: ns, Labels: row.labels, OwnerReferences: row.owners},
				Spec:       druidv1alpha1.EtcdSpec{Replicas: 1, Labels: map[string]string{"app": "etcd"}},
			}
			create(t, etcd)
			create(t, controlPlane(ns, row.cp, "foreign"))
			writeGeneration(t, ns, row.cp, 1)
			waitCondition(t, ns, row.cp, metav1.ConditionFalse, bryggav1alpha1.ReasonVolumesExist, row.message)
			settle(t, ns, row.cp)
			if datastore := getControlPlane(t, ns, row.cp).Status.Datastore; datastore != nil {
				t.Errorf("status.datastore %+v describes an Etcd of another ControlPlane", datastore)
			}
			if again := getEtcd(t, ns, row.cp); again.ResourceVersion != etcd.ResourceVersion {
				t.Errorf("the Etcd moved from resourceVersion %s to %s: owners %+v", etcd.ResourceVersion, again.ResourceVersion, again.OwnerReferences)
			}

			if err := k8s.Delete(t.Context(), getControlPlane(t, ns, row.cp)); err != nil {
				t.Fatal(err)
			}
			eventually(t, func() error {
				return gone(k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: row.cp}, &bryggav1alpha1.ControlPlane{}))
			})
			if again := getEtcd(t, ns, row.cp); again.ResourceVersion != etcd.ResourceVersion || !again.DeletionTimestamp.IsZero() {
				t.Errorf("the deletion touched the Etcd: resourceVersion %s to %s, deletionTimestamp %v", etcd.ResourceVersion, again.ResourceVersion, again.DeletionTimestamp)
			}
		})
	}
}

func TestStorageAtCreation(t *testing.T) {
	ns := namespace(t)
	createClass(t, newClass("growing", "Retain"))
	create(t, controlPlane(ns, "grown", "growing"))
	writeGeneration(t, ns, "grown", 1)
	waitEtcd(t, ns, "grown", nil)

	class := &bryggav1alpha1.DatastoreClass{}
	if err := k8s.Get(t.Context(), client.ObjectKey{Name: "growing"}, class); err != nil {
		t.Fatal(err)
	}
	class.Spec.Storage = bryggav1alpha1.EtcdStorage{ClassName: "fast", Size: resource.MustParse("2Gi"), ReclaimPolicy: "Retain"}
	class.Spec.Etcd = &bryggav1alpha1.EtcdSettings{QuotaBackendBytes: new(resource.MustParse("3Gi"))}
	if err := k8s.Update(t.Context(), class); err != nil {
		t.Fatal(err)
	}
	etcd := waitEtcd(t, ns, "grown", func(etcd *druidv1alpha1.Etcd) error {
		if quota := quantity(etcd.Spec.Etcd.Quota); quota != "3Gi" {
			return fmt.Errorf("quota %s, the class edit has not reached the Etcd", quota)
		}
		return nil
	})
	if etcd.Spec.StorageClass != nil || quantity(etcd.Spec.StorageCapacity) != "1Gi" {
		t.Errorf("storageClass %v, storageCapacity %s: storage changed after creation", etcd.Spec.StorageClass, quantity(etcd.Spec.StorageCapacity))
	}
	if got := etcd.Annotations[bryggav1alpha1.DatastoreClassGenerationAnnotation]; got != "2" {
		t.Errorf("class generation %s recorded, want 2", got)
	}
}

func TestRecordedClass(t *testing.T) {
	ns := namespace(t)
	first := newClass("default-first", "Retain")
	first.Annotations = map[string]string{bryggav1alpha1.DefaultClassAnnotation: "true"}
	first.Spec.Storage.Size = resource.MustParse("3Gi")
	createClass(t, first)
	create(t, controlPlane(ns, "dev", ""))
	writeGeneration(t, ns, "dev", 1)
	etcd := waitEtcd(t, ns, "dev", nil)
	if got := etcd.Annotations[bryggav1alpha1.DatastoreClassAnnotation]; got != "default-first" {
		t.Fatalf("class %s recorded, want default-first", got)
	}
	create(t, claim(ns, "dev-etcd-dev-etcd-0", druidv1alpha1.GetDefaultLabels(etcd.ObjectMeta)))
	settle(t, ns, "dev")

	second := newClass("default-second", "Delete")
	second.Annotations = map[string]string{bryggav1alpha1.DefaultClassAnnotation: "true"}
	second.Spec.Storage.Size = resource.MustParse("5Gi")
	createClass(t, second)
	if err := k8s.Get(t.Context(), client.ObjectKey{Name: "default-first"}, first); err != nil {
		t.Fatal(err)
	}
	delete(first.Annotations, bryggav1alpha1.DefaultClassAnnotation)
	if err := k8s.Update(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	settle(t, ns, "dev")
	etcd = getEtcd(t, ns, "dev")
	if etcd.Annotations[bryggav1alpha1.DatastoreClassAnnotation] != "default-first" || quantity(etcd.Spec.StorageCapacity) != "3Gi" {
		t.Errorf("the Etcd follows class %s with %s, want default-first with 3Gi after the default moved", etcd.Annotations[bryggav1alpha1.DatastoreClassAnnotation], quantity(etcd.Spec.StorageCapacity))
	}
	if pvc := getClaim(t, ns, "dev-etcd-dev-etcd-0"); pvc.Annotations[bryggav1alpha1.DatastoreClassAnnotation] != "default-first" {
		t.Errorf("the claim records class %s, want default-first", pvc.Annotations[bryggav1alpha1.DatastoreClassAnnotation])
	}

	if err := k8s.Delete(t.Context(), getControlPlane(t, ns, "dev")); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() error {
		return gone(k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: "dev"}, &bryggav1alpha1.ControlPlane{}))
	})
	untouched(t, ns, "dev-etcd-dev-etcd-0")
}

func reconcileNow(t *testing.T, ns, name string) time.Duration {
	t.Helper()
	result, err := (&datastore.Reconciler{Client: k8s, Reader: k8s}).Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: name}})
	if err != nil {
		t.Fatal(err)
	}
	return result.RequeueAfter
}

func TestClassSurvivesTheEtcd(t *testing.T) {
	ns := namespace(t)
	first := newClass("survivor-first", "Retain")
	first.Annotations = map[string]string{bryggav1alpha1.DefaultClassAnnotation: "true"}
	first.Spec.Storage.Size = resource.MustParse("3Gi")
	createClass(t, first)
	create(t, controlPlane(ns, "dev", ""))
	writeGeneration(t, ns, "dev", 1)
	etcd := waitEtcd(t, ns, "dev", nil)
	create(t, claim(ns, "dev-etcd-dev-etcd-0", druidv1alpha1.GetDefaultLabels(etcd.ObjectMeta)))
	settle(t, ns, "dev")

	second := newClass("survivor-second", "Delete")
	second.Annotations = map[string]string{bryggav1alpha1.DefaultClassAnnotation: "true"}
	second.Spec.Storage.Size = resource.MustParse("5Gi")
	createClass(t, second)
	if err := k8s.Get(t.Context(), client.ObjectKey{Name: "survivor-first"}, first); err != nil {
		t.Fatal(err)
	}
	delete(first.Annotations, bryggav1alpha1.DefaultClassAnnotation)
	if err := k8s.Update(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	if err := k8s.Delete(t.Context(), etcd); err != nil {
		t.Fatal(err)
	}
	waitEtcd(t, ns, "dev", func(again *druidv1alpha1.Etcd) error {
		if again.UID == etcd.UID {
			return errors.New("the Etcd has not been created again yet")
		}
		if class, size := again.Annotations[bryggav1alpha1.DatastoreClassAnnotation], quantity(again.Spec.StorageCapacity); class != "survivor-first" || size != "3Gi" {
			return fmt.Errorf("the Etcd created again follows class %s with %s, want survivor-first, which its claims record, with 3Gi", class, size)
		}
		return nil
	})

	if err := k8s.Delete(t.Context(), newClass("survivor-first", "")); err != nil {
		t.Fatal(err)
	}
	create(t, controlPlane(ns, "probe", "survivor-first"))
	waitCondition(t, ns, "probe", metav1.ConditionFalse, bryggav1alpha1.ReasonDatastoreClassNotFound, "DatastoreClass survivor-first does not exist")
	if err := k8s.Delete(t.Context(), getEtcd(t, ns, "dev")); err != nil {
		t.Fatal(err)
	}
	waitCondition(t, ns, "dev", metav1.ConditionFalse, bryggav1alpha1.ReasonDatastoreClassNotFound,
		"DatastoreClass survivor-first, which the claims of this ControlPlane record, does not exist")
	if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: "dev-etcd"}, &druidv1alpha1.Etcd{}); !apierrors.IsNotFound(err) {
		t.Fatalf("want no Etcd while the class its claims record is gone, got %v", err)
	}
	if after := reconcileNow(t, ns, "dev"); after != time.Minute {
		t.Errorf("a class the claims record and that is gone requeues after %s, want a minute: the claims are outside the cache", after)
	}

	createClass(t, newClass("survivor-first", "Retain"))
	waitEtcd(t, ns, "dev", func(again *druidv1alpha1.Etcd) error {
		if class := again.Annotations[bryggav1alpha1.DatastoreClassAnnotation]; class != "survivor-first" {
			return fmt.Errorf("the Etcd follows class %s, want survivor-first", class)
		}
		return nil
	})
}

func withoutTLS(etcd *druidv1alpha1.Etcd) druidv1alpha1.EtcdSpec {
	spec := etcd.Spec.DeepCopy()
	spec.Etcd.ClientUrlTLS, spec.Etcd.PeerUrlTLS, spec.Backup.TLS = nil, nil, nil
	return *spec
}

func TestClassGone(t *testing.T) {
	ns, credentialsNamespace := namespace(t), namespace(t)
	const cp = "kept"
	create(t, credentials(credentialsNamespace))
	class := fullClass("vanishing", credentialsNamespace)
	class.Spec.Storage.ReclaimPolicy = "Delete"
	createClass(t, class)
	create(t, controlPlane(ns, cp, "vanishing"))
	writeGeneration(t, ns, cp, 1)
	waitEtcd(t, ns, cp, namesGeneration(1))
	runStatefulSet(t, ns, cp, 1)
	setEtcdStatus(t, ns, cp, nil)
	waitCondition(t, ns, cp, metav1.ConditionTrue, bryggav1alpha1.ReasonReady, "ready on certificate generation 1")
	create(t, claim(ns, "kept-etcd-kept-etcd-0", nil))
	settle(t, ns, cp)
	before := getEtcd(t, ns, cp)

	if err := k8s.Delete(t.Context(), newClass("vanishing", "")); err != nil {
		t.Fatal(err)
	}
	settle(t, ns, cp)
	waitCondition(t, ns, cp, metav1.ConditionTrue, bryggav1alpha1.ReasonReady, "ready on certificate generation 1")

	writeGeneration(t, ns, cp, 2)
	after := waitEtcd(t, ns, cp, namesGeneration(2))
	if got, want := withoutTLS(after), withoutTLS(before); !reflect.DeepEqual(got, want) {
		t.Errorf("the Etcd's fields from the gone class moved:\n got %+v\nwant %+v", got, want)
	}
	for _, key := range []string{bryggav1alpha1.DatastoreClassAnnotation, bryggav1alpha1.DatastoreClassGenerationAnnotation} {
		if after.Annotations[key] != before.Annotations[key] {
			t.Errorf("annotation %s moved from %q to %q", key, before.Annotations[key], after.Annotations[key])
		}
	}
	runStatefulSet(t, ns, cp, 2)
	setEtcdStatus(t, ns, cp, nil)
	waitCondition(t, ns, cp, metav1.ConditionTrue, bryggav1alpha1.ReasonReady, "etcd-druid reports Etcd kept-etcd ready on certificate generation 2")

	if err := k8s.Delete(t.Context(), getControlPlane(t, ns, cp)); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() error {
		return gone(k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: cp}, &bryggav1alpha1.ControlPlane{}))
	})
	untouched(t, ns, "kept-etcd-kept-etcd-0")
}

func TestDeletionBeforeLabelling(t *testing.T) {
	ns := namespace(t)
	createClass(t, newClass("unlabelled", "Delete"))
	create(t, controlPlane(ns, "dev", "unlabelled"))
	writeGeneration(t, ns, "dev", 1)
	waitEtcd(t, ns, "dev", nil)
	settle(t, ns, "dev")

	create(t, claim(ns, "dev-etcd-dev-etcd-0", druidv1alpha1.GetDefaultLabels(metav1.ObjectMeta{Name: "dev-etcd"})))
	if pvc := getClaim(t, ns, "dev-etcd-dev-etcd-0"); pvc.Labels[bryggav1alpha1.ControlPlaneUIDLabel] != "" {
		t.Fatalf("a pass labelled the claim before the deletion: %v", pvc.Labels)
	}
	if err := k8s.Delete(t.Context(), getControlPlane(t, ns, "dev")); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() error {
		return gone(k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: "dev"}, &bryggav1alpha1.ControlPlane{}))
	})
	deleted(t, ns, "dev-etcd-dev-etcd-0")
}

func TestDeletion(t *testing.T) {
	for _, policy := range []string{"Delete", "Retain"} {
		t.Run(policy, func(t *testing.T) {
			ns := namespace(t)
			className := "reclaim-" + strings.ToLower(policy)
			createClass(t, newClass(className, policy))
			create(t, controlPlane(ns, "dev", className))
			writeGeneration(t, ns, "dev", 1)
			etcd := waitEtcd(t, ns, "dev", nil)
			cp := getControlPlane(t, ns, "dev")

			etcdLabels := druidv1alpha1.GetDefaultLabels(etcd.ObjectMeta)
			previous := maps.Clone(etcdLabels)
			previous[bryggav1alpha1.ControlPlaneUIDLabel] = "11111111-2222-3333-4444-555555555555"
			create(t, claim(ns, "dev-etcd-dev-etcd-0", etcdLabels))
			create(t, claim(ns, "dev-etcd-dev-etcd-1", previous))
			create(t, claim(ns, "someone-elses", nil))
			settle(t, ns, "dev")
			if pvc := getClaim(t, ns, "dev-etcd-dev-etcd-0"); pvc.Labels[bryggav1alpha1.ControlPlaneUIDLabel] != string(cp.UID) || pvc.Annotations[bryggav1alpha1.DatastoreClassAnnotation] != className {
				t.Fatalf("the StatefulSet's claim carries labels %v and annotations %v, want the ControlPlane's UID %s and the class %s", pvc.Labels, pvc.Annotations, cp.UID, className)
			}

			held := client.MergeFrom(etcd.DeepCopy())
			etcd.Finalizers = append(etcd.Finalizers, "brygga.segla.io/test")
			if err := k8s.Patch(t.Context(), etcd, held); err != nil {
				t.Fatal(err)
			}
			if err := k8s.Delete(t.Context(), cp); err != nil {
				t.Fatal(err)
			}
			eventually(t, func() error {
				if etcd = getEtcd(t, ns, "dev"); etcd.DeletionTimestamp.IsZero() {
					return errors.New("the Etcd is not being deleted")
				}
				return nil
			})
			if !controllerutil.ContainsFinalizer(getControlPlane(t, ns, "dev"), bryggav1alpha1.DatastoreFinalizer) {
				t.Fatal("the finalizer left before the Etcd was gone")
			}
			waitCondition(t, ns, "dev", metav1.ConditionFalse, bryggav1alpha1.ReasonDeleting, "the ControlPlane is being deleted")
			untouched(t, ns, "dev-etcd-dev-etcd-0")
			untouched(t, ns, "dev-etcd-dev-etcd-1")

			released := client.MergeFrom(etcd.DeepCopy())
			etcd.Finalizers = nil
			if err := k8s.Patch(t.Context(), etcd, released); err != nil {
				t.Fatal(err)
			}
			eventually(t, func() error {
				return gone(k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: "dev"}, &bryggav1alpha1.ControlPlane{}))
			})
			if err := gone(k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: "dev-etcd"}, &druidv1alpha1.Etcd{})); err != nil {
				t.Errorf("the Etcd: %v", err)
			}
			if policy == "Delete" {
				deleted(t, ns, "dev-etcd-dev-etcd-0")
			} else {
				untouched(t, ns, "dev-etcd-dev-etcd-0")
			}
			untouched(t, ns, "dev-etcd-dev-etcd-1")
			untouched(t, ns, "someone-elses")
		})
	}
}
