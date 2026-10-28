// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package datastore_test

import (
	"bytes"
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
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	networkingv1ac "k8s.io/client-go/applyconfigurations/networking/v1"
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

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/controller/datastore"
	"go.lodia.io/lodia/internal/controller/datastore/datastoretest"
	"go.lodia.io/lodia/internal/operator"
)

var k8s client.WithWatch

const druidNamespace = "druid-system"

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
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "..", "api", "crds")},
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
	for _, add := range []func(*runtime.Scheme) error{clientgoscheme.AddToScheme, lodiav1alpha1.AddToScheme, druidv1alpha1.AddToScheme} {
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
	if err := (&datastore.Reconciler{Client: mgr.GetClient(), Reader: mgr.GetAPIReader(), EtcdDruidNamespace: druidNamespace}).SetupWithManager(mgr, 1); err != nil {
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
	const user = "lodia-datastore"
	ctx := context.Background()
	manifest, err := os.ReadFile(filepath.Join("..", "..", "..", "charts", "lodia", "files", "rbac", "datastore.yaml"))
	if err != nil {
		return nil, err
	}
	role := &rbacv1.ClusterRole{}
	if err := yaml.UnmarshalStrict(manifest, role); err != nil {
		return nil, err
	}
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
	err = wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		review := &authorizationv1.SubjectAccessReview{Spec: authorizationv1.SubjectAccessReviewSpec{
			User:               user,
			ResourceAttributes: &authorizationv1.ResourceAttributes{Group: lodiav1alpha1.GroupVersion.Group, Resource: "controlplanes", Verb: "watch"},
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

func createClass(t *testing.T, class *lodiav1alpha1.DatastoreClass) {
	t.Helper()
	create(t, class)
	t.Cleanup(func() {
		if err := client.IgnoreNotFound(k8s.Delete(context.Background(), class)); err != nil {
			t.Error(err)
		}
	})
}

func newClass(name string, reclaimPolicy string) *lodiav1alpha1.DatastoreClass {
	return &lodiav1alpha1.DatastoreClass{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       lodiav1alpha1.DatastoreClassSpec{Storage: lodiav1alpha1.EtcdStorage{Size: resource.MustParse("1Gi"), ReclaimPolicy: reclaimPolicy}},
	}
}

func fullClass(name, credentialsNamespace string) *lodiav1alpha1.DatastoreClass {
	return &lodiav1alpha1.DatastoreClass{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: lodiav1alpha1.DatastoreClassSpec{
			Etcd: &lodiav1alpha1.EtcdSettings{
				QuotaBackendBytes: new(resource.MustParse("2Gi")),
				AutoCompaction:    &lodiav1alpha1.AutoCompaction{Mode: "revision", Retention: "1000"},
				Defragmentation:   &lodiav1alpha1.Defragmentation{Schedule: "0 3 * * *"},
				Resources: &corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("30m"), corev1.ResourceMemory: resource.MustParse("150M")},
					Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("2Gi")},
				},
			},
			Storage: lodiav1alpha1.EtcdStorage{ClassName: "fast", Size: resource.MustParse("20Gi"), ReclaimPolicy: "Retain"},
			Backup: &lodiav1alpha1.EtcdBackup{
				Store: lodiav1alpha1.BackupStore{
					Provider: "S3", Bucket: "lodia-backups", Prefix: "prod", Region: "eu-central-1", Endpoint: "https://s3.example.test",
					SecretRef: lodiav1alpha1.SecretReference{Name: "s3", Namespace: credentialsNamespace},
				},
				FullSnapshotSchedule: "0 2 * * *",
				DeltaSnapshotPeriod:  &metav1.Duration{Duration: 5 * time.Minute},
				Retention:            &lodiav1alpha1.BackupRetention{FullSnapshots: 30},
			},
			Placement: &lodiav1alpha1.Placement{
				NodeSelector:      map[string]string{"node-role.example.com/control-planes": "true"},
				Tolerations:       []corev1.Toleration{{Key: "control-planes", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule}},
				PriorityClassName: "control-plane",
			},
		},
	}
}

func controlPlane(ns, name, className string) *lodiav1alpha1.ControlPlane {
	return &lodiav1alpha1.ControlPlane{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: lodiav1alpha1.ControlPlaneSpec{
			Version:   "v1.37.1",
			Datastore: lodiav1alpha1.DatastoreChoice{Dedicated: &lodiav1alpha1.DedicatedDatastore{ClassName: className}},
			Endpoint:  lodiav1alpha1.Endpoint{LoadBalancer: &lodiav1alpha1.LoadBalancerEndpoint{}},
			Network:   lodiav1alpha1.Network{ServiceCIDRs: []lodiav1alpha1.CIDR{"10.96.0.0/16"}, PodCIDRs: []lodiav1alpha1.CIDR{"10.244.0.0/16"}},
		},
	}
}

func credentials(ns string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "s3", Namespace: ns},
		Data:       map[string][]byte{"accessKeyID": []byte("spike-access"), "secretAccessKey": []byte("spike-secret-key"), "s3ForcePathStyle": []byte("true")},
	}
}

func writeGeneration(t *testing.T, ns, cp string, generation int, only ...lodiav1alpha1.EtcdCertificateRole) {
	t.Helper()
	secrets, err := datastoretest.Generation(getControlPlane(t, ns, cp), generation)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range secrets {
		if len(only) > 0 && !slices.ContainsFunc(only, func(certificate lodiav1alpha1.EtcdCertificateRole) bool {
			return secret.Name == lodiav1alpha1.EtcdCertificateSecretName(cp, certificate, generation)
		}) {
			continue
		}
		create(t, secret)
	}
}

func getControlPlane(t *testing.T, ns, name string) *lodiav1alpha1.ControlPlane {
	t.Helper()
	cp := &lodiav1alpha1.ControlPlane{}
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
		cp := &lodiav1alpha1.ControlPlane{}
		if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: name}, cp); err != nil {
			return err
		}
		condition := meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionDatastoreReady)
		switch {
		case condition == nil:
			return fmt.Errorf("no %s condition", lodiav1alpha1.ConditionDatastoreReady)
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
		condition := meta.FindStatusCondition(current.Status.Conditions, lodiav1alpha1.ConditionDatastoreReady)
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
		if !controllerutil.ContainsFinalizer(getControlPlane(t, ns, "dev"), lodiav1alpha1.DatastoreFinalizer) {
			return fmt.Errorf("the finalizer %s is not on the ControlPlane", lodiav1alpha1.DatastoreFinalizer)
		}
		return nil
	})
	waitCondition(t, ns, "dev", metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForCertificates, "waiting for a complete generation of the etcd certificates")
	if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: "dev-etcd"}, &druidv1alpha1.Etcd{}); !apierrors.IsNotFound(err) {
		t.Fatalf("want no Etcd before the first certificate generation, got %v", err)
	}

	writeGeneration(t, ns, "dev", 1)
	etcd := waitEtcd(t, ns, "dev", namesGeneration(1))
	var cp *lodiav1alpha1.ControlPlane
	eventually(t, func() error {
		if cp = getControlPlane(t, ns, "dev"); cp.Status.Datastore == nil {
			return errors.New("status.datastore is not written yet")
		}
		return nil
	})
	tenantNamespace := &corev1.Namespace{}
	if err := k8s.Get(t.Context(), client.ObjectKey{Name: ns}, tenantNamespace); err != nil {
		t.Fatal(err)
	}
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
		{"backup.store.bucket → spec.backup.store.container", ptr.Deref(store.Container, ""), "lodia-backups"},
		{"backup.store.prefix → spec.backup.store.prefix, the namespace's and the ControlPlane's UIDs below the class's prefix", store.Prefix, "prod/" + string(tenantNamespace.UID) + "/controlplanes/" + string(cp.UID)},
		{"backup.store.endpoint → spec.backup.store.endpointOverride", ptr.Deref(store.EndpointOverride, ""), "https://s3.example.test"},
		{"backup.store.secretRef → spec.backup.store.secretRef, the copy in the namespace", ptr.Deref(store.SecretRef, corev1.SecretReference{}).Name, "dev-etcd-backup"},
		{"backup.fullSnapshotSchedule → spec.backup.fullSnapshotSchedule", ptr.Deref(etcd.Spec.Backup.FullSnapshotSchedule, ""), "0 2 * * *"},
		{"backup.deltaSnapshotPeriod → spec.backup.deltaSnapshotPeriod", ptr.Deref(etcd.Spec.Backup.DeltaSnapshotPeriod, metav1.Duration{}).Duration, 5 * time.Minute},
		{"backup.retention.fullSnapshots → spec.backup.garbageCollectionPolicy", ptr.Deref(etcd.Spec.Backup.GarbageCollectionPolicy, ""), druidv1alpha1.GarbageCollectionPolicy("LimitBased")},
		{"backup.retention.fullSnapshots → spec.backup.maxBackupsLimitBasedGC", ptr.Deref(etcd.Spec.Backup.MaxBackupsLimitBasedGC, 0), int32(30)},
		{"placement.nodeSelector → spec.schedulingConstraints.affinity", etcd.Spec.SchedulingConstraints.Affinity, &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{
				{Key: "node-role.example.com/control-planes", Operator: corev1.NodeSelectorOpIn, Values: []string{"true"}},
			}}}},
		}}},
		{"placement.tolerations → spec.schedulingConstraints.tolerations", etcd.Spec.SchedulingConstraints.Tolerations,
			[]corev1.Toleration{{Key: "control-planes", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule}}},
		{"placement.priorityClassName → spec.priorityClassName", ptr.Deref(etcd.Spec.PriorityClassName, ""), "control-plane"},
		{"generation 1 → spec.etcd.clientUrlTls", tls(etcd.Spec.Etcd.ClientUrlTLS), "ca dev-etcd-ca-1/" + lodiav1alpha1.CACertKey + " server dev-etcd-server-1 client dev-etcd-client-1"},
		{"generation 1 → spec.etcd.peerUrlTls", peerTLS(etcd), "ca dev-etcd-ca-1/" + lodiav1alpha1.CACertKey + " server dev-etcd-peer-1 client "},
		{"generation 1 → spec.backup.tls", tls(etcd.Spec.Backup.TLS), "ca dev-etcd-ca-1/" + lodiav1alpha1.CACertKey + " server dev-etcd-backup-restore-1 client dev-etcd-client-1"},
		{"the ControlPlane → metadata.labels", etcd.Labels[lodiav1alpha1.ControlPlaneLabel], "dev"},
		{"the ControlPlane → spec.labels", etcd.Spec.Labels[lodiav1alpha1.ControlPlaneLabel], "dev"},
		{"the ControlPlane → metadata.ownerReferences", etcd.OwnerReferences, []metav1.OwnerReference{{
			APIVersion: "lodia.io/v1alpha1", Kind: "ControlPlane", Name: "dev", UID: cp.UID, Controller: new(true), BlockOwnerDeletion: new(true),
		}}},
		{"the Etcd → status.datastore", ptr.Deref(cp.Status.Datastore, lodiav1alpha1.DatastoreStatus{}), lodiav1alpha1.DatastoreStatus{
			Endpoints: []lodiav1alpha1.HostPort{{Host: "dev-etcd-client." + ns + ".svc", Port: 2379}},
			Class:     &lodiav1alpha1.DatastoreClassStatus{Name: "mapped", Generation: 1},
			Members:   lodiav1alpha1.RolloutCount{Count: lodiav1alpha1.Count{Total: 1}},
		}},
		{"the class → metadata.annotations[" + lodiav1alpha1.DatastoreClassAnnotation + "]", etcd.Annotations[lodiav1alpha1.DatastoreClassAnnotation], "mapped"},
		{"the class's generation → metadata.annotations[" + lodiav1alpha1.DatastoreClassGenerationAnnotation + "]", etcd.Annotations[lodiav1alpha1.DatastoreClassGenerationAnnotation], "1"},
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
	events, err := k8s.Watch(t.Context(), &lodiav1alpha1.ControlPlaneList{}, client.InNamespace(ns))
	if err != nil {
		t.Fatal(err)
	}
	defer events.Stop()
	create(t, controlPlane(ns, "dev", "state"))
	var first *metav1.Condition
	for first == nil {
		select {
		case event := <-events.ResultChan():
			if cp, ok := event.Object.(*lodiav1alpha1.ControlPlane); ok && event.Type == watch.Modified {
				first = meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionDatastoreReady)
			}
		case <-time.After(30 * time.Second):
			t.Fatal("no DatastoreReady condition was written")
		}
	}
	if first.Status != metav1.ConditionUnknown || first.Reason != lodiav1alpha1.ReasonReconciling {
		t.Errorf("the first DatastoreReady is %s/%s %q, want Unknown/%s", first.Status, first.Reason, first.Message, lodiav1alpha1.ReasonReconciling)
	}

	writeGeneration(t, ns, "dev", 1)
	waitCondition(t, ns, "dev", metav1.ConditionFalse, lodiav1alpha1.ReasonProvisioning, "etcd-druid has not observed generation 1 of Etcd dev-etcd")
	var before *lodiav1alpha1.ControlPlane
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
		condition := meta.FindStatusCondition(after.Status.Conditions, lodiav1alpha1.ConditionDatastoreReady)
		t.Errorf("a pass whose Etcd read fails moved the ControlPlane from resourceVersion %s to %s: DatastoreReady %s/%s %q, status.datastore %+v",
			before.ResourceVersion, after.ResourceVersion, condition.Status, condition.Reason, condition.Message, after.Status.Datastore)
	}
}

func TestEtcdRefused(t *testing.T) {
	ns := namespace(t)
	createClass(t, newClass("refused", "Retain"))
	binding := refuseEtcds(t, ns)

	create(t, controlPlane(ns, "dev", "refused"))
	writeGeneration(t, ns, "dev", 1)
	waitCondition(t, ns, "dev", metav1.ConditionFalse, lodiav1alpha1.ReasonRefused,
		"the API server refuses Etcd dev-etcd on certificate generation 1 with the fields of DatastoreClass refused generation 1; the reason is in Lodia's log")
	if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: "dev-etcd"}, &druidv1alpha1.Etcd{}); !apierrors.IsNotFound(err) {
		t.Fatalf("want no Etcd while the API server refuses it, got %v", err)
	}

	if err := k8s.Delete(t.Context(), binding); err != nil {
		t.Fatal(err)
	}
	waitEtcd(t, ns, "dev", namesGeneration(1))
	waitCondition(t, ns, "dev", metav1.ConditionFalse, lodiav1alpha1.ReasonProvisioning, "etcd-druid has not observed generation 1 of Etcd dev-etcd")
}

func refuseEtcds(t *testing.T, ns string) *admissionregistrationv1.ValidatingAdmissionPolicyBinding {
	t.Helper()
	return refuse(t, ns, admissionregistrationv1.Rule{APIGroups: []string{druidv1alpha1.SchemeGroupVersion.Group}, APIVersions: []string{"v1alpha1"}, Resources: []string{"etcds"}}, func() client.Object {
		return &druidv1alpha1.Etcd{ObjectMeta: metav1.ObjectMeta{Name: "probe", Namespace: ns}, Spec: druidv1alpha1.EtcdSpec{Replicas: 1, Labels: map[string]string{"app": "etcd"}}}
	})
}

func refuse(t *testing.T, ns string, rule admissionregistrationv1.Rule, probe func() client.Object) *admissionregistrationv1.ValidatingAdmissionPolicyBinding {
	t.Helper()
	resource := rule.Resources[0]
	policy := &admissionregistrationv1.ValidatingAdmissionPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "refuse-" + resource + "-in-" + ns},
		Spec: admissionregistrationv1.ValidatingAdmissionPolicySpec{
			FailurePolicy: new(admissionregistrationv1.Fail),
			MatchConstraints: &admissionregistrationv1.MatchResources{ResourceRules: []admissionregistrationv1.NamedRuleWithOperations{{
				RuleWithOperations: admissionregistrationv1.RuleWithOperations{
					Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Create, admissionregistrationv1.Update},
					Rule:       rule,
				},
			}}},
			Validations: []admissionregistrationv1.Validation{{Expression: "object.metadata.namespace != '" + ns + "'", Message: "no " + resource + " in " + ns}},
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
		if err := k8s.Create(t.Context(), probe(), client.DryRunAll); err == nil {
			return errors.New("the policy does not refuse " + resource + " yet")
		}
		return nil
	})
	return binding
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
		if controllerutil.ContainsFinalizer(cp, lodiav1alpha1.DatastoreFinalizer) || meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionDatastoreReady) != nil {
			return fmt.Errorf("a paused ControlPlane was reconciled: finalizers %v, conditions %+v", cp.Finalizers, cp.Status.Conditions)
		}
		if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: "dev-etcd"}, &druidv1alpha1.Etcd{}); !apierrors.IsNotFound(err) {
			return fmt.Errorf("want no Etcd for a paused ControlPlane, got %w", err)
		}
		return nil
	})

	setPaused(t, ns, "dev", false)
	waitEtcd(t, ns, "dev", namesGeneration(1))
	waitCondition(t, ns, "dev", metav1.ConditionFalse, lodiav1alpha1.ReasonProvisioning, "etcd-druid has not observed generation 1 of Etcd dev-etcd")
	if !controllerutil.ContainsFinalizer(getControlPlane(t, ns, "dev"), lodiav1alpha1.DatastoreFinalizer) {
		t.Fatal("the finalizer is not on the unpaused ControlPlane")
	}

	setPaused(t, ns, "dev", true)
	writeGeneration(t, ns, "dev", 2)
	if err := k8s.Delete(t.Context(), getControlPlane(t, ns, "dev")); err != nil {
		t.Fatal(err)
	}
	consistently(t, func() error {
		cp := getControlPlane(t, ns, "dev")
		if !controllerutil.ContainsFinalizer(cp, lodiav1alpha1.DatastoreFinalizer) {
			return errors.New("the finalizer of a paused ControlPlane was released")
		}
		if condition := meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionDatastoreReady); condition.Reason == lodiav1alpha1.ReasonDeleting {
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
		return gone(k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: "dev"}, &lodiav1alpha1.ControlPlane{}))
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

	writeGeneration(t, ns, "dev", 2, lodiav1alpha1.EtcdCARole, lodiav1alpha1.EtcdServerRole, lodiav1alpha1.EtcdPeerRole, lodiav1alpha1.EtcdBackupRestoreRole)
	settle(t, ns, "dev")
	if err := namesGeneration(1)(getEtcd(t, ns, "dev")); err != nil {
		t.Fatalf("four of five Secrets of generation 2: %v", err)
	}

	secrets, err := datastoretest.Generation(getControlPlane(t, ns, "dev"), 2)
	if err != nil {
		t.Fatal(err)
	}
	terminating := secrets[slices.IndexFunc(secrets, func(s *corev1.Secret) bool {
		return s.Name == lodiav1alpha1.EtcdCertificateSecretName("dev", lodiav1alpha1.EtcdClientRole, 2)
	})]
	label := terminating.Labels[lodiav1alpha1.EtcdCertificatesLabel]
	delete(terminating.Labels, lodiav1alpha1.EtcdCertificatesLabel)
	terminating.Finalizers = []string{"lodia.io/test"}
	create(t, terminating)
	if err := k8s.Delete(t.Context(), terminating); err != nil {
		t.Fatal(err)
	}
	if err := k8s.Get(t.Context(), client.ObjectKeyFromObject(terminating), terminating); err != nil {
		t.Fatal(err)
	}
	patch := client.MergeFrom(terminating.DeepCopy())
	terminating.Labels[lodiav1alpha1.EtcdCertificatesLabel] = label
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

	writeGeneration(t, ns, "dev", 2, lodiav1alpha1.EtcdClientRole)
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
	if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: lodiav1alpha1.EtcdCertificateSecretName("dev", lodiav1alpha1.EtcdClientRole, 2)}, held); err != nil {
		t.Fatal(err)
	}
	patch := client.MergeFrom(held.DeepCopy())
	held.Finalizers = []string{"lodia.io/test"}
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
		APIVersion: "lodia.io/v1alpha1", Kind: "ControlPlane", Name: "dev", UID: "11111111-2222-3333-4444-555555555555",
		Controller: new(true), BlockOwnerDeletion: new(true),
	}})
	writeWith(2, nil)
	settle(t, ns, "dev")
	waitCondition(t, ns, "dev", metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForCertificates, "waiting for a complete generation of the etcd certificates")
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

	for _, certificate := range lodiav1alpha1.EtcdCertificateRoles() {
		if err := k8s.Delete(t.Context(), &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: lodiav1alpha1.EtcdCertificateSecretName(cp, certificate, 2)}}); err != nil {
			t.Fatal(err)
		}
	}
	waitCondition(t, ns, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonProvisioning,
		"Secrets Etcd mounted-etcd mounts do not exist: mounted-etcd-backup-restore-2, mounted-etcd-ca-2, mounted-etcd-client-2, mounted-etcd-peer-2, mounted-etcd-server-2")
	if err := namesGeneration(2)(getEtcd(t, ns, cp)); err != nil {
		t.Fatalf("the Etcd left the generation it names for an older one: %v", err)
	}

	writeGeneration(t, ns, cp, 3)
	waitEtcd(t, ns, cp, namesGeneration(3))
	readyOn(t, ns, cp, 3)
	terminating := &corev1.Secret{}
	if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: lodiav1alpha1.EtcdCertificateSecretName(cp, lodiav1alpha1.EtcdCARole, 3)}, terminating); err != nil {
		t.Fatal(err)
	}
	held := client.MergeFrom(terminating.DeepCopy())
	terminating.Finalizers = []string{"lodia.io/test"}
	if err := k8s.Patch(t.Context(), terminating, held); err != nil {
		t.Fatal(err)
	}
	if err := k8s.Delete(t.Context(), terminating); err != nil {
		t.Fatal(err)
	}
	settle(t, ns, cp)
	waitCondition(t, ns, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, "has quorum, 1 of 1 members ready")
	released := client.MergeFrom(terminating.DeepCopy())
	terminating.Finalizers = nil
	if err := k8s.Patch(t.Context(), terminating, released); err != nil {
		t.Fatal(err)
	}
}

func TestDatastoreClassNotFound(t *testing.T) {
	ns := namespace(t)
	create(t, controlPlane(ns, "named", "absent"))
	waitCondition(t, ns, "named", metav1.ConditionFalse, lodiav1alpha1.ReasonDatastoreClassNotFound, "DatastoreClass absent does not exist")

	create(t, controlPlane(ns, "unnamed", ""))
	waitCondition(t, ns, "unnamed", metav1.ConditionFalse, lodiav1alpha1.ReasonDatastoreClassNotFound, "no DatastoreClass is annotated")

	for _, name := range []string{"default-a", "default-b"} {
		class := newClass(name, "Retain")
		class.Annotations = map[string]string{lodiav1alpha1.DefaultClassAnnotation: "true"}
		class.Spec.Storage.Size = resource.MustParse("3Gi")
		createClass(t, class)
	}
	waitCondition(t, ns, "unnamed", metav1.ConditionFalse, lodiav1alpha1.ReasonDatastoreClassAmbiguous, "2 DatastoreClasses are annotated")

	if err := k8s.Delete(t.Context(), newClass("default-b", "")); err != nil {
		t.Fatal(err)
	}
	waitCondition(t, ns, "unnamed", metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForCertificates, "waiting for a complete generation")
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
	waitCondition(t, ns, "backed", metav1.ConditionFalse, lodiav1alpha1.ReasonProvisioning, "DatastoreClass backed-up cannot be applied yet: its credentials Secret "+credentialsNamespace+"/s3 is missing")

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
	if copied.Labels[lodiav1alpha1.ControlPlaneLabel] != "backed" {
		t.Errorf("labels %v lack %s=dev", copied.Labels, lodiav1alpha1.ControlPlaneLabel)
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

const snapshotParser = "https://github.com/gardener/etcd-backup-restore/blob/v0.45.1/pkg/snapstore/snapshot.go#L34-L59"

func snapshotPrefix(key string) string {
	for _, version := range []string{"v1/", "v2/"} {
		if i := strings.LastIndex(key, version); i >= 0 {
			return key[:i+len(version)]
		}
	}
	return ""
}

func TestBackupPrefixHoldsNoName(t *testing.T) {
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "team-v1"}}
	create(t, ns)
	credentialsNamespace := namespace(t)
	create(t, credentials(credentialsNamespace))
	createClass(t, fullClass("nameless", credentialsNamespace))
	create(t, controlPlane(ns.Name, "v1", "nameless"))
	writeGeneration(t, ns.Name, "v1", 1)
	store := ptr.Deref(waitEtcd(t, ns.Name, "v1", nil).Spec.Backup.Store, druidv1alpha1.StoreSpec{})
	cp := getControlPlane(t, ns.Name, "v1")
	if want := "prod/" + string(ns.UID) + "/controlplanes/" + string(cp.UID); store.Prefix != want {
		t.Fatalf("ControlPlane v1 in namespace team-v1 has the backup prefix %q, want %q", store.Prefix, want)
	}
	for _, c := range []struct{ key, prefix string }{
		{store.Prefix + "/v2/Full-00000000-00000052-1761500000.gz", store.Prefix + "/v2/"},
		{store.Prefix + "/v2/Incr-00000053-00000071-1761500010.gz", store.Prefix + "/v2/"},
		{"prod/team-v1/v1/" + string(cp.UID) + "/v2/Full-00000000-00000052-1761500000.gz", "prod/team-v1/v1/"},
	} {
		if got := snapshotPrefix(c.key); got != c.prefix {
			t.Errorf("etcd-backup-restore v0.45.1 reads %s under %q, want %q (%s)", c.key, got, c.prefix, snapshotParser)
		}
	}

	settle(t, ns.Name, "v1")
	before := getControlPlane(t, ns.Name, "v1")
	injected := errors.New("injected: the API server did not answer")
	failing := interceptor.NewClient(k8s, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*corev1.Namespace); ok {
				return injected
			}
			return c.Get(ctx, key, obj, opts...)
		},
	})
	reconciler := &datastore.Reconciler{Client: failing, Reader: failing, EtcdDruidNamespace: druidNamespace}
	if _, err := reconciler.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns.Name, Name: "v1"}}); !errors.Is(err, injected) {
		t.Fatalf("a pass whose Namespace read fails returned %v, want the read's error", err)
	}
	if after := getControlPlane(t, ns.Name, "v1"); after.ResourceVersion != before.ResourceVersion {
		t.Errorf("a pass whose Namespace read fails moved the ControlPlane from resourceVersion %s to %s", before.ResourceVersion, after.ResourceVersion)
	}
}

func readyOn(t *testing.T, ns, cp string, generation int) {
	t.Helper()
	runStatefulSet(t, ns, cp, generation)
	setEtcdStatus(t, ns, cp, nil)
	waitCondition(t, ns, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, "has quorum, 1 of 1 members ready")
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
	class := &lodiav1alpha1.DatastoreClass{}
	if err := k8s.Get(t.Context(), client.ObjectKey{Name: "holding"}, class); err != nil {
		t.Fatal(err)
	}
	class.Spec.Etcd.QuotaBackendBytes = new(resource.MustParse("3Gi"))
	if err := k8s.Update(t.Context(), class); err != nil {
		t.Fatal(err)
	}
	writeGeneration(t, ns, cp, 2)
	etcd := waitEtcd(t, ns, cp, namesGeneration(2))
	if quota, recorded := quantity(etcd.Spec.Etcd.Quota), etcd.Annotations[lodiav1alpha1.DatastoreClassGenerationAnnotation]; quota != "2Gi" || recorded != "1" {
		t.Errorf("quota %s from class generation %s: the held class's edit reached the Etcd", quota, recorded)
	}
	readyOn(t, ns, cp, 2)
	waitCondition(t, ns, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady,
		"has quorum, 1 of 1 members ready; DatastoreClass holding is held: its credentials Secret "+credentialsNamespace+"/s3 is missing")
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
	waitCondition(t, ns, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, "has quorum, 1 of 1 members ready")
	if c := meta.FindStatusCondition(getControlPlane(t, ns, cp).Status.Conditions, lodiav1alpha1.ConditionDatastoreReady); strings.Contains(c.Message, "is held") {
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
	copied.Finalizers = []string{"lodia.io/test"}
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
	waitCondition(t, ns, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, "has quorum, 1 of 1 members ready")
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
				Name: cp + "-etcd-backup", Namespace: ns, Labels: map[string]string{lodiav1alpha1.ControlPlaneLabel: cp},
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: "lodia.io/v1alpha1", Kind: "ControlPlane", Name: cp, UID: "11111111-2222-3333-4444-555555555555",
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
	waitCondition(t, ns, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonProvisioning, "DatastoreClass succeeding cannot be applied yet: "+controlledBy)
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
	waitCondition(t, ns, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonProvisioning, "; DatastoreClass succeeding is held: "+controlledBy)
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
	create(t, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: cp + "-etcd-backup", Namespace: ns, Labels: map[string]string{lodiav1alpha1.ControlPlaneLabel: cp}}})
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
	for _, certificate := range lodiav1alpha1.EtcdCertificateRoles() {
		volumes = append(volumes, corev1.Volume{Name: string(certificate), VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: lodiav1alpha1.EtcdCertificateSecretName(cp, certificate, generation)}}})
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
	sts.Status = appsv1.StatefulSetStatus{ObservedGeneration: sts.Generation, Replicas: 1, ReadyReplicas: 1, AvailableReplicas: 1, CurrentReplicas: 1, UpdatedReplicas: 1, CurrentRevision: "rev", UpdateRevision: "rev"}
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
	shared.Spec.Datastore = lodiav1alpha1.DatastoreChoice{Shared: &lodiav1alpha1.SharedDatastore{Name: "pool"}}
	create(t, shared)
	waitCondition(t, ns, "shared", metav1.ConditionFalse, lodiav1alpha1.ReasonUnsupported, "spec.datastore.shared is not supported yet")
	if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: "shared-etcd"}, &networkingv1.NetworkPolicy{}); !apierrors.IsNotFound(err) {
		t.Errorf("want no NetworkPolicy for a shared store, got %v", err)
	}
}

func TestDatastoreReadyIsQuorum(t *testing.T) {
	ns := namespace(t)
	const cp = "reported"
	createClass(t, newClass("reported", "Retain"))
	create(t, controlPlane(ns, cp, "reported"))
	writeGeneration(t, ns, cp, 1)
	waitEtcd(t, ns, cp, nil)
	waitCondition(t, ns, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonProvisioning, "etcd-druid has not observed generation 1 of Etcd reported-etcd, observedGeneration 0")
	setEtcdStatus(t, ns, cp, func(s *druidv1alpha1.EtcdStatus) { s.Ready, s.ReadyReplicas = new(false), 0 })
	waitCondition(t, ns, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonProvisioning, "Etcd reported-etcd has 0 of 1 members ready")
	if etcd := getEtcd(t, ns, cp); metav1.HasAnnotation(etcd.ObjectMeta, lodiav1alpha1.EtcdFormedAnnotation) {
		t.Errorf("an etcd that never had a ready member is annotated %v", etcd.Annotations)
	}

	runStatefulSet(t, ns, cp, 1)
	setEtcdStatus(t, ns, cp, nil)
	waitCondition(t, ns, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, "Etcd reported-etcd has quorum, 1 of 1 members ready")
	waitEtcd(t, ns, cp, func(etcd *druidv1alpha1.Etcd) error {
		if etcd.Annotations[lodiav1alpha1.EtcdFormedAnnotation] != string(etcd.UID) {
			return fmt.Errorf("an etcd with a ready member is annotated %v, want %s with its UID %s", etcd.Annotations, lodiav1alpha1.EtcdFormedAnnotation, etcd.UID)
		}
		return nil
	})
	if got, want := ptr.Deref(getControlPlane(t, ns, cp).Status.Datastore, lodiav1alpha1.DatastoreStatus{}), (lodiav1alpha1.DatastoreStatus{
		Endpoints: []lodiav1alpha1.HostPort{{Host: "reported-etcd-client." + ns + ".svc", Port: 2379}},
		Class:     &lodiav1alpha1.DatastoreClassStatus{Name: "reported", Generation: 1},
		Members:   lodiav1alpha1.RolloutCount{Count: lodiav1alpha1.Count{Total: 1, Ready: 1}, Updated: new(int32(1))},
	}); !reflect.DeepEqual(got, want) {
		t.Errorf("status.datastore %+v, want %+v", got, want)
	}

	rows := []struct {
		name    string
		etcd    func(*druidv1alpha1.EtcdStatus)
		sts     func(*appsv1.StatefulSetStatus)
		updated string
	}{
		{"a generation not observed", func(s *druidv1alpha1.EtcdStatus) { s.ObservedGeneration = new(*s.ObservedGeneration - 1) }, nil, "absent"},
		{"AllMembersUpdated False", func(s *druidv1alpha1.EtcdStatus) {
			s.Conditions[0].Status, s.Conditions[0].Reason, s.Conditions[0].Message = druidv1alpha1.ConditionFalse, "NotAllMembersUpdated", "At least one member is not yet updated"
		}, nil, "1"},
		{"last errors", func(s *druidv1alpha1.EtcdStatus) {
			s.LastErrors = []druidapicommon.LastError{{Code: "ERR_SYNC_CLIENT_SERVICE", Description: "the server could not answer", ObservedAt: metav1.Now()}}
		}, nil, "1"},
		{"a StatefulSet generation not observed", nil, func(s *appsv1.StatefulSetStatus) { s.ObservedGeneration-- }, "absent"},
		{"the member rolled, not ready yet", nil, func(s *appsv1.StatefulSetStatus) { s.AvailableReplicas = 0 }, "0"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			setEtcdStatus(t, ns, cp, row.etcd)
			setStatefulSetStatus(t, ns, cp, row.sts)
			settle(t, ns, cp)
			waitCondition(t, ns, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, "Etcd reported-etcd has quorum, 1 of 1 members ready")
			eventually(t, func() error {
				updated := "absent"
				if u := getControlPlane(t, ns, cp).Status.Datastore.Members.Updated; u != nil {
					updated = strconv.Itoa(int(*u))
				}
				if updated != row.updated {
					return fmt.Errorf("status.datastore.members.updated is %s, want %s", updated, row.updated)
				}
				return nil
			})
			setEtcdStatus(t, ns, cp, nil)
			setStatefulSetStatus(t, ns, cp, nil)
		})
	}

	setEtcdStatus(t, ns, cp, func(s *druidv1alpha1.EtcdStatus) { s.Ready, s.ReadyReplicas = new(false), 0 })
	waitCondition(t, ns, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonQuorumLost, "Etcd reported-etcd has 0 of 1 members ready")
	setEtcdStatus(t, ns, cp, nil)
	waitCondition(t, ns, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, "Etcd reported-etcd has quorum, 1 of 1 members ready")

	etcd := getEtcd(t, ns, cp)
	held := client.MergeFrom(etcd.DeepCopy())
	etcd.Finalizers = append(etcd.Finalizers, "lodia.io/test")
	if err := k8s.Patch(t.Context(), etcd, held); err != nil {
		t.Fatal(err)
	}
	if err := k8s.Delete(t.Context(), etcd); err != nil {
		t.Fatal(err)
	}
	waitCondition(t, ns, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonProvisioning, "Etcd reported-etcd is being deleted")
	released := client.MergeFrom(etcd.DeepCopy())
	etcd.Finalizers = nil
	if err := k8s.Patch(t.Context(), etcd, released); err != nil {
		t.Fatal(err)
	}
	recreated := waitEtcd(t, ns, cp, func(e *druidv1alpha1.Etcd) error {
		if e.UID == etcd.UID {
			return errors.New("the deleted Etcd is still there")
		}
		return nil
	})
	if recreated.Annotations[lodiav1alpha1.EtcdFormedAnnotation] == string(recreated.UID) {
		t.Errorf("the recreated Etcd starts annotated %v", recreated.Annotations)
	}
	waitCondition(t, ns, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonProvisioning, "etcd-druid has not observed generation 1 of Etcd reported-etcd")

	copied := client.MergeFrom(recreated.DeepCopy())
	metav1.SetMetaDataAnnotation(&recreated.ObjectMeta, lodiav1alpha1.EtcdFormedAnnotation, string(etcd.UID))
	if err := k8s.Patch(t.Context(), recreated, copied); err != nil {
		t.Fatal(err)
	}
	setEtcdStatus(t, ns, cp, func(s *druidv1alpha1.EtcdStatus) { s.Ready, s.ReadyReplicas = new(false), 0 })
	waitCondition(t, ns, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonProvisioning, "Etcd reported-etcd has 0 of 1 members ready")
}

func TestThreeMembersWithHighAvailability(t *testing.T) {
	ns := namespace(t)
	const cp = "ha"
	createClass(t, newClass("ha", "Retain"))
	ha := controlPlane(ns, cp, "ha")
	ha.Spec.HighAvailability = &lodiav1alpha1.HighAvailability{FailureTolerance: "Zone"}
	create(t, ha)
	writeGeneration(t, ns, cp, 1)
	etcd := waitEtcd(t, ns, cp, nil)
	members := map[string]string{"app.kubernetes.io/managed-by": "etcd-druid", "app.kubernetes.io/part-of": cp + "-etcd", "app.kubernetes.io/name": cp + "-etcd"}
	var spread []string
	for _, c := range etcd.Spec.SchedulingConstraints.TopologySpreadConstraints {
		spread = append(spread, fmt.Sprintf("%s %d %s %d %s %v %v", c.TopologyKey, c.MaxSkew, c.WhenUnsatisfiable, ptr.Deref(c.MinDomains, 0), ptr.Deref(c.NodeTaintsPolicy, ""), c.LabelSelector.MatchLabels, c.MatchLabelKeys))
	}
	if want := []string{
		fmt.Sprintf("kubernetes.io/hostname 1 DoNotSchedule 3 Honor %v []", members),
		fmt.Sprintf("topology.kubernetes.io/zone 1 DoNotSchedule 3 Honor %v []", members),
	}; etcd.Spec.Replicas != 3 || !slices.Equal(spread, want) {
		t.Errorf("the Etcd asks for %d members spread by %q, want 3 spread by %q", etcd.Spec.Replicas, spread, want)
	}
	if value, ok := etcd.Annotations["resources.druid.gardener.cloud/allow-unhealthy-pod-eviction"]; !ok || value != "" {
		t.Errorf("the Etcd's annotations are %v, want etcd-druid's allow-unhealthy-pod-eviction", etcd.Annotations)
	}

	runStatefulSet(t, ns, cp, 1)
	quorate := func(ready int32) {
		t.Helper()
		setEtcdStatus(t, ns, cp, func(s *druidv1alpha1.EtcdStatus) {
			s.Replicas, s.CurrentReplicas, s.ReadyReplicas, s.Ready = 3, 3, ready, new(ready == 3)
			s.Members = append(s.Members, druidv1alpha1.EtcdMemberStatus{Name: cp + "-etcd-1", Status: druidv1alpha1.EtcdMemberStatusReady, Reason: "LeaseSucceeded", LastTransitionTime: metav1.Now()},
				druidv1alpha1.EtcdMemberStatus{Name: cp + "-etcd-2", Status: druidv1alpha1.EtcdMemberStatusReady, Reason: "LeaseSucceeded", LastTransitionTime: metav1.Now()})
		})
	}
	setStatefulSetStatus(t, ns, cp, func(s *appsv1.StatefulSetStatus) {
		s.Replicas, s.ReadyReplicas, s.AvailableReplicas, s.CurrentReplicas, s.UpdatedReplicas = 3, 3, 3, 3, 3
	})
	quorate(0)
	waitCondition(t, ns, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonProvisioning, "Etcd ha-etcd has 0 of 3 members ready")
	quorate(3)
	waitCondition(t, ns, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, "Etcd ha-etcd has quorum, 3 of 3 members ready")
	quorate(2)
	waitCondition(t, ns, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, "Etcd ha-etcd has quorum, 2 of 3 members ready")
	quorate(1)
	waitCondition(t, ns, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, "Etcd ha-etcd has quorum, 1 of 3 members ready")
	quorate(0)
	waitCondition(t, ns, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonQuorumLost, "Etcd ha-etcd has 0 of 3 members ready")
	quorate(3)
	waitCondition(t, ns, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, "Etcd ha-etcd has quorum, 3 of 3 members ready")

	policy := getPolicy(t, ns, cp)
	if want := "[from 1 peers to [2379] from 1 peers to [2379 2380] from 1 peers to [2379]]"; describeIngress(policy) != want {
		t.Errorf("the NetworkPolicy of three members admits %s, want %s", describeIngress(policy), want)
	}

	update := func(change func(*lodiav1alpha1.ControlPlane)) {
		t.Helper()
		eventually(t, func() error {
			fresh := getControlPlane(t, ns, cp)
			change(fresh)
			return k8s.Update(t.Context(), fresh)
		})
	}
	setEtcdStatus(t, ns, cp, func(s *druidv1alpha1.EtcdStatus) {
		s.Replicas, s.CurrentReplicas, s.ReadyReplicas = 3, 3, 1
		s.Conditions = append(s.Conditions, druidv1alpha1.Condition{
			Type: druidv1alpha1.ConditionTypeScaleOperationComplete, Status: druidv1alpha1.ConditionFalse, Reason: druidv1alpha1.ScaleOperationReasonScalingOut,
			LastTransitionTime: metav1.Now(), LastUpdateTime: metav1.Now(),
		})
	})
	waitCondition(t, ns, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, "Etcd ha-etcd has quorum, 1 of 3 members ready, while etcd-druid scales it")
	update(func(fresh *lodiav1alpha1.ControlPlane) { fresh.Spec.HighAvailability = nil })
	settle(t, ns, cp)
	if etcd := getEtcd(t, ns, cp); etcd.Spec.Replicas != 3 {
		t.Errorf("the Etcd asks for %d members while etcd-druid scales it, want the 3 it scales to", etcd.Spec.Replicas)
	}
	setEtcdStatus(t, ns, cp, func(s *druidv1alpha1.EtcdStatus) { s.Replicas, s.CurrentReplicas, s.ReadyReplicas = 3, 3, 3 })
	waitEtcd(t, ns, cp, func(etcd *druidv1alpha1.Etcd) error {
		if _, ok := etcd.Annotations["resources.druid.gardener.cloud/allow-unhealthy-pod-eviction"]; etcd.Spec.Replicas != 1 || ok || len(etcd.Spec.SchedulingConstraints.TopologySpreadConstraints) != 0 {
			return fmt.Errorf("the Etcd asks for %d members, annotated %v, spread by %+v", etcd.Spec.Replicas, etcd.Annotations, etcd.Spec.SchedulingConstraints.TopologySpreadConstraints)
		}
		return nil
	})
}

func TestHighAvailabilityOnAFormedEtcd(t *testing.T) {
	ns := namespace(t)
	const cp = "growing"
	createClass(t, newClass("growing", "Retain"))
	create(t, controlPlane(ns, cp, "growing"))
	writeGeneration(t, ns, cp, 1)
	waitEtcd(t, ns, cp, nil)
	runStatefulSet(t, ns, cp, 1)
	druid := func(members, ready, pods int32, scaling string) {
		t.Helper()
		setStatefulSetStatus(t, ns, cp, func(s *appsv1.StatefulSetStatus) {
			s.Replicas, s.ReadyReplicas, s.AvailableReplicas, s.CurrentReplicas, s.UpdatedReplicas = pods, ready, ready, pods, pods
		})
		setEtcdStatus(t, ns, cp, func(s *druidv1alpha1.EtcdStatus) {
			s.Replicas, s.CurrentReplicas, s.ReadyReplicas, s.Ready = pods, pods, ready, new(ready == pods)
			for i := int32(1); i < members; i++ {
				s.Members = append(s.Members, druidv1alpha1.EtcdMemberStatus{
					Name: fmt.Sprintf("%s-etcd-%d", cp, i), Role: new(druidv1alpha1.EtcdRoleMember), Status: druidv1alpha1.EtcdMemberStatusReady, Reason: "LeaseSucceeded", LastTransitionTime: metav1.Now(),
				})
			}
			complete := druidv1alpha1.Condition{
				Type: druidv1alpha1.ConditionTypeScaleOperationComplete, Status: druidv1alpha1.ConditionTrue, Reason: druidv1alpha1.ScaleOperationReasonNoScaleOperation,
				LastTransitionTime: metav1.Now(), LastUpdateTime: metav1.Now(),
			}
			if scaling != "" {
				complete.Status, complete.Reason = druidv1alpha1.ConditionFalse, scaling
			}
			s.Conditions = append(s.Conditions, complete)
		})
	}
	druid(1, 1, 1, "")
	waitCondition(t, ns, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, "Etcd growing-etcd has quorum, 1 of 1 members ready")
	highAvailability := func(ha *lodiav1alpha1.HighAvailability) {
		t.Helper()
		eventually(t, func() error {
			fresh := getControlPlane(t, ns, cp)
			fresh.Spec.HighAvailability = ha
			return k8s.Update(t.Context(), fresh)
		})
	}

	highAvailability(&lodiav1alpha1.HighAvailability{FailureTolerance: "Node"})
	waitEtcd(t, ns, cp, func(etcd *druidv1alpha1.Etcd) error {
		if etcd.Spec.Replicas != 3 {
			return fmt.Errorf("the Etcd asks for %d members", etcd.Spec.Replicas)
		}
		return nil
	})
	for _, step := range []struct {
		name                 string
		members, ready, pods int32
		scaling, want        string
	}{
		{"etcd-druid starts the scale-out", 1, 1, 3, druidv1alpha1.ScaleOperationReasonScalingOut, "has quorum, 1 of 3 members ready, while etcd-druid scales it"},
		{"two learners joined, which fail the readiness read", 3, 1, 3, druidv1alpha1.ScaleOperationReasonScalingOut, "has quorum, 1 of 3 members ready, while etcd-druid scales it"},
		{"the scale-out is complete before the learners are promoted", 3, 1, 3, "", "has quorum, 1 of 3 members ready"},
		{"the learners are promoted", 3, 3, 3, "", "has quorum, 3 of 3 members ready"},
	} {
		t.Run("on: "+step.name, func(t *testing.T) {
			druid(step.members, step.ready, step.pods, step.scaling)
			eventually(t, func() error {
				c := meta.FindStatusCondition(getControlPlane(t, ns, cp).Status.Conditions, lodiav1alpha1.ConditionDatastoreReady)
				if want := "Etcd growing-etcd " + step.want; c == nil || c.Status != metav1.ConditionTrue || c.Reason != lodiav1alpha1.ReasonReady || c.Message != want {
					return fmt.Errorf("DatastoreReady is %+v, want True Ready %q", c, want)
				}
				return nil
			})
		})
	}

	highAvailability(nil)
	waitEtcd(t, ns, cp, func(etcd *druidv1alpha1.Etcd) error {
		if etcd.Spec.Replicas != 1 {
			return fmt.Errorf("the Etcd asks for %d members", etcd.Spec.Replicas)
		}
		return nil
	})
	for _, step := range []struct {
		name                 string
		members, ready, pods int32
		scaling              string
	}{
		{"etcd-druid starts the scale-in", 1, 3, 3, druidv1alpha1.ScaleOperationReasonScalingIn},
		{"it has removed the surplus members and their pods", 1, 1, 1, druidv1alpha1.ScaleOperationReasonScalingIn},
		{"its member list completes the scale-in", 1, 1, 1, ""},
	} {
		t.Run("off: "+step.name, func(t *testing.T) {
			druid(step.members, step.ready, step.pods, step.scaling)
			settle(t, ns, cp)
			waitCondition(t, ns, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, "has quorum")
			if etcd := getEtcd(t, ns, cp); etcd.Spec.Replicas != 1 {
				t.Errorf("the Etcd asks for %d members, want 1", etcd.Spec.Replicas)
			}
		})
	}
}

func TestEtcdDruidKeepsTheClientPortThroughAScaleIn(t *testing.T) {
	ns := namespace(t)
	const cp = "shrinking"
	createClass(t, newClass("shrinking", "Retain"))
	ha := controlPlane(ns, cp, "shrinking")
	ha.Spec.HighAvailability = &lodiav1alpha1.HighAvailability{FailureTolerance: "Node"}
	create(t, ha)
	writeGeneration(t, ns, cp, 1)
	waitEtcd(t, ns, cp, nil)
	runStatefulSet(t, ns, cp, 1)
	druid := func(members, ready, pods int32, scaling string) {
		t.Helper()
		setStatefulSetStatus(t, ns, cp, func(s *appsv1.StatefulSetStatus) {
			s.Replicas, s.ReadyReplicas, s.AvailableReplicas, s.CurrentReplicas, s.UpdatedReplicas = pods, ready, ready, pods, pods
		})
		setEtcdStatus(t, ns, cp, func(s *druidv1alpha1.EtcdStatus) {
			s.Replicas, s.CurrentReplicas, s.ReadyReplicas, s.Ready = pods, pods, ready, new(ready == pods)
			for i := int32(1); i < members; i++ {
				s.Members = append(s.Members, druidv1alpha1.EtcdMemberStatus{
					Name: fmt.Sprintf("%s-etcd-%d", cp, i), Status: druidv1alpha1.EtcdMemberStatusReady, Reason: "LeaseSucceeded", LastTransitionTime: metav1.Now(),
				})
			}
			complete := druidv1alpha1.Condition{
				Type: druidv1alpha1.ConditionTypeScaleOperationComplete, Status: druidv1alpha1.ConditionTrue, Reason: druidv1alpha1.ScaleOperationReasonNoScaleOperation,
				LastTransitionTime: metav1.Now(), LastUpdateTime: metav1.Now(),
			}
			if scaling != "" {
				complete.Status, complete.Reason = druidv1alpha1.ConditionFalse, scaling
			}
			s.Conditions = append(s.Conditions, complete)
		})
	}
	druid(3, 3, 3, "")
	waitCondition(t, ns, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, "has quorum")

	eventually(t, func() error {
		fresh := getControlPlane(t, ns, cp)
		fresh.Spec.HighAvailability = nil
		return k8s.Update(t.Context(), fresh)
	})
	waitEtcd(t, ns, cp, func(etcd *druidv1alpha1.Etcd) error {
		if etcd.Spec.Replicas != 1 {
			return fmt.Errorf("the Etcd asks for %d members", etcd.Spec.Replicas)
		}
		return nil
	})
	for _, step := range []struct {
		name                 string
		members, ready, pods int32
		scaling              string
	}{
		{"etcd-druid starts the scale-in", 3, 3, 3, druidv1alpha1.ScaleOperationReasonScalingIn},
		{"its status lists only the member the spec asks for", 1, 3, 3, druidv1alpha1.ScaleOperationReasonScalingIn},
		{"it has removed the surplus members and their pods", 1, 1, 1, druidv1alpha1.ScaleOperationReasonScalingIn},
		{"its member list completes the scale-in", 1, 1, 1, ""},
	} {
		t.Run(step.name, func(t *testing.T) {
			druid(step.members, step.ready, step.pods, step.scaling)
			settle(t, ns, cp)
			waitCondition(t, ns, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, "has quorum")
			sameSpec(t, getPolicy(t, ns, cp).Spec, policySpec(cp, false))
			if m := getControlPlane(t, ns, cp).Status.Datastore.Members; m.Ready > m.Total {
				t.Errorf("status.datastore.members counts %d ready of %d", m.Ready, m.Total)
			}
			if etcd := getEtcd(t, ns, cp); etcd.Spec.Replicas != 1 {
				t.Errorf("the Etcd asks for %d members, want the 1 etcd-druid scales it to", etcd.Spec.Replicas)
			}
		})
	}
}

func TestAddingHighAvailabilityWaitsForASettledEtcd(t *testing.T) {
	ns := namespace(t)
	const cp = "toggled"
	createClass(t, newClass("toggled", "Retain"))
	create(t, controlPlane(ns, cp, "toggled"))
	writeGeneration(t, ns, cp, 1)
	waitEtcd(t, ns, cp, nil)
	runStatefulSet(t, ns, cp, 1)
	setEtcdStatus(t, ns, cp, nil)
	waitCondition(t, ns, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, "has quorum, 1 of 1 members ready")

	t.Run("the lone member restarting on the current spec holds it", func(t *testing.T) {
		setStatefulSetStatus(t, ns, cp, func(s *appsv1.StatefulSetStatus) {
			s.ReadyReplicas, s.AvailableReplicas, s.CurrentReplicas, s.CurrentRevision = 0, 0, 0, "old"
		})
		setHighAvailability(t, ns, cp, &lodiav1alpha1.HighAvailability{FailureTolerance: "Zone"})
		expectMembers(t, ns, cp, 1, false)
	})
	t.Run("a held highAvailability lets a certificate generation through", func(t *testing.T) {
		writeGeneration(t, ns, cp, 2)
		waitEtcd(t, ns, cp, namesGeneration(2))
		expectMembers(t, ns, cp, 1, false)
	})
	t.Run("an Etcd etcd-druid has not observed holds it", func(t *testing.T) {
		setStatefulSetStatus(t, ns, cp, nil)
		expectMembers(t, ns, cp, 1, false)
	})
	t.Run("a StatefulSet its controller has not observed holds it", func(t *testing.T) {
		setStatefulSetStatus(t, ns, cp, func(s *appsv1.StatefulSetStatus) { s.ObservedGeneration-- })
		setEtcdStatus(t, ns, cp, nil)
		expectMembers(t, ns, cp, 1, false)
	})
	t.Run("a settled etcd takes it", func(t *testing.T) {
		setStatefulSetStatus(t, ns, cp, nil)
		expectMembers(t, ns, cp, 3, true)
	})
}

func TestRemovingHighAvailabilityBacksOutOfAMemberThatNeverComesUp(t *testing.T) {
	ns := namespace(t)
	const cp = "stuck"
	createClass(t, newClass("stuck", "Retain"))
	ha := controlPlane(ns, cp, "stuck")
	ha.Spec.HighAvailability = &lodiav1alpha1.HighAvailability{FailureTolerance: "Zone"}
	create(t, ha)
	writeGeneration(t, ns, cp, 1)
	waitEtcd(t, ns, cp, nil)
	runStatefulSet(t, ns, cp, 1)
	setStatefulSetStatus(t, ns, cp, func(s *appsv1.StatefulSetStatus) {
		s.Replicas, s.ReadyReplicas, s.AvailableReplicas, s.CurrentReplicas, s.UpdatedReplicas = 3, 2, 2, 3, 3
	})
	druid := func(scaling string) {
		t.Helper()
		setEtcdStatus(t, ns, cp, func(s *druidv1alpha1.EtcdStatus) {
			s.Replicas, s.CurrentReplicas, s.ReadyReplicas, s.Ready = 3, 3, 2, new(false)
			complete := druidv1alpha1.Condition{
				Type: druidv1alpha1.ConditionTypeScaleOperationComplete, Status: druidv1alpha1.ConditionTrue, Reason: druidv1alpha1.ScaleOperationReasonNoScaleOperation,
				LastTransitionTime: metav1.Now(), LastUpdateTime: metav1.Now(),
			}
			if scaling != "" {
				complete.Status, complete.Reason = druidv1alpha1.ConditionFalse, scaling
			}
			s.Conditions = append(s.Conditions, complete)
		})
	}
	druid(druidv1alpha1.ScaleOperationReasonScalingOut)
	waitCondition(t, ns, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, "has quorum, 2 of 3 members ready, while etcd-druid scales it")

	t.Run("etcd-druid's scale in progress holds the removal", func(t *testing.T) {
		setHighAvailability(t, ns, cp, nil)
		expectMembers(t, ns, cp, 3, true)
		c := meta.FindStatusCondition(getControlPlane(t, ns, cp).Status.Conditions, lodiav1alpha1.ConditionDatastoreReady)
		if want := "Etcd stuck-etcd has quorum, 2 of 3 members ready, while etcd-druid scales it"; c == nil || c.Status != metav1.ConditionTrue || c.Reason != lodiav1alpha1.ReasonReady || c.Message != want {
			t.Errorf("DatastoreReady is %+v, want True Ready %q", c, want)
		}
	})
	t.Run("a completed scale lets it through though a member never came up", func(t *testing.T) {
		druid("")
		expectMembers(t, ns, cp, 1, false)
	})
}

func TestRemovingHighAvailabilityWaitsForTheMemberEtcdDruidKeeps(t *testing.T) {
	ns := namespace(t)
	const cp = "kept"
	createClass(t, newClass("kept", "Retain"))
	ha := controlPlane(ns, cp, "kept")
	ha.Spec.HighAvailability = &lodiav1alpha1.HighAvailability{FailureTolerance: "Zone"}
	create(t, ha)
	writeGeneration(t, ns, cp, 1)
	waitEtcd(t, ns, cp, nil)
	runStatefulSet(t, ns, cp, 1)
	druid := func(ready int32, first druidv1alpha1.EtcdMemberConditionStatus, reason string) {
		t.Helper()
		setStatefulSetStatus(t, ns, cp, func(s *appsv1.StatefulSetStatus) {
			s.Replicas, s.ReadyReplicas, s.AvailableReplicas, s.CurrentReplicas, s.UpdatedReplicas = 3, ready, ready, 3, 3
		})
		setEtcdStatus(t, ns, cp, func(s *druidv1alpha1.EtcdStatus) {
			s.Replicas, s.CurrentReplicas, s.ReadyReplicas, s.Ready = 3, 3, ready, new(ready == 3)
			s.Members[0].Status, s.Members[0].Reason = first, reason
			for i := 1; i < 3; i++ {
				s.Members = append(s.Members, druidv1alpha1.EtcdMemberStatus{
					Name: fmt.Sprintf("%s-etcd-%d", cp, i), Status: druidv1alpha1.EtcdMemberStatusReady, Reason: "LeaseSucceeded", LastTransitionTime: metav1.Now(),
				})
			}
			if first == "" {
				s.Members = s.Members[1:]
			}
		})
	}
	druid(2, druidv1alpha1.EtcdMemberStatusNotReady, "ContainersNotReady")
	waitCondition(t, ns, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, "Etcd kept-etcd has quorum, 2 of 3 members ready")

	t.Run("the member etcd-druid keeps, not ready, holds the removal", func(t *testing.T) {
		setHighAvailability(t, ns, cp, nil)
		expectMembers(t, ns, cp, 3, true)
		waitCondition(t, ns, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady,
			"Etcd kept-etcd has quorum, 2 of 3 members ready; removing highAvailability waits for etcd-druid to list member kept-etcd-0, the one it keeps, as ready")
		if m := getControlPlane(t, ns, cp).Status.Datastore.Members; m.Total != 3 || m.Ready != 2 {
			t.Errorf("status.datastore.members counts %d ready of %d, want 2 of 3", m.Ready, m.Total)
		}
	})
	t.Run("missing from etcd-druid's member list, it holds it", func(t *testing.T) {
		druid(2, "", "")
		expectMembers(t, ns, cp, 3, true)
	})
	t.Run("listed unknown, its lease expired, it holds it though its pod is ready", func(t *testing.T) {
		druid(3, druidv1alpha1.EtcdMemberStatusUnknown, "LeaseExpired")
		expectMembers(t, ns, cp, 3, true)
	})
	t.Run("listed ready, it lets it through", func(t *testing.T) {
		druid(3, druidv1alpha1.EtcdMemberStatusReady, "LeaseSucceeded")
		waitEtcd(t, ns, cp, func(etcd *druidv1alpha1.Etcd) error {
			if etcd.Spec.Replicas != 1 {
				return fmt.Errorf("the Etcd asks for %d members", etcd.Spec.Replicas)
			}
			return nil
		})
		expectMembers(t, ns, cp, 1, false)
	})
}

func TestAHeldScaleOutResumesOnceTheStatefulSetSettles(t *testing.T) {
	ns := namespace(t)
	const cp = "resumes"
	createClass(t, newClass("resumes", "Retain"))
	create(t, controlPlane(ns, cp, "resumes"))
	writeGeneration(t, ns, cp, 1)
	waitEtcd(t, ns, cp, nil)
	runStatefulSet(t, ns, cp, 1)
	setEtcdStatus(t, ns, cp, nil)
	waitCondition(t, ns, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, "has quorum, 1 of 1 members ready")
	setStatefulSetStatus(t, ns, cp, func(s *appsv1.StatefulSetStatus) {
		s.ReadyReplicas, s.AvailableReplicas, s.CurrentReplicas, s.CurrentRevision = 0, 0, 0, "old"
	})
	setHighAvailability(t, ns, cp, &lodiav1alpha1.HighAvailability{FailureTolerance: "Zone"})
	expectMembers(t, ns, cp, 1, false)

	setStatefulSetStatus(t, ns, cp, nil)
	waitEtcd(t, ns, cp, func(etcd *druidv1alpha1.Etcd) error {
		if etcd.Spec.Replicas != 3 {
			return fmt.Errorf("the Etcd asks for %d members after the StatefulSet settled", etcd.Spec.Replicas)
		}
		return nil
	})
}

func setHighAvailability(t *testing.T, ns, cp string, ha *lodiav1alpha1.HighAvailability) {
	t.Helper()
	eventually(t, func() error {
		fresh := getControlPlane(t, ns, cp)
		fresh.Spec.HighAvailability = ha
		return k8s.Update(t.Context(), fresh)
	})
}

func expectMembers(t *testing.T, ns, cp string, want int32, spread bool) {
	t.Helper()
	settle(t, ns, cp)
	etcd := getEtcd(t, ns, cp)
	_, annotated := etcd.Annotations["resources.druid.gardener.cloud/allow-unhealthy-pod-eviction"]
	if constraints := etcd.Spec.SchedulingConstraints.TopologySpreadConstraints; etcd.Spec.Replicas != want || annotated != spread || (len(constraints) > 0) != spread {
		t.Errorf("the Etcd asks for %d members, annotated %v, spread by %+v; want %d members, spread and annotated: %t", etcd.Spec.Replicas, etcd.Annotations, constraints, want, spread)
	}
	fresh := getControlPlane(t, ns, cp)
	asked := int32(1)
	if fresh.Spec.HighAvailability != nil {
		asked = 3
	}
	condition := meta.FindStatusCondition(fresh.Status.Conditions, lodiav1alpha1.ConditionDatastoreReady)
	if condition == nil || strings.Contains(condition.Message, "highAvailability waits for Etcd "+cp+"-etcd to settle: every member on its current spec and available, etcd-druid's last scale completed") != (want < asked) {
		t.Errorf("DatastoreReady is %+v, want the wait noted: %t", condition, want < asked)
	}
}

func describeIngress(policy *networkingv1.NetworkPolicy) string {
	var rules []string
	for _, rule := range policy.Spec.Ingress {
		var ports []string
		for _, port := range rule.Ports {
			ports = append(ports, port.Port.String())
		}
		rules = append(rules, fmt.Sprintf("from %d peers to %v", len(rule.From), ports))
	}
	return fmt.Sprint(rules)
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
	previous[lodiav1alpha1.ControlPlaneUIDLabel] = "11111111-2222-3333-4444-555555555555"
	create(t, claim(ns, "dev-etcd-dev-etcd-0", nil))
	create(t, claim(ns, "dev-etcd-dev-etcd-1", previous))
	create(t, claim(ns, "dev-etcd-scratch", etcdLabels))
	create(t, controlPlane(ns, "dev", "reused"))
	writeGeneration(t, ns, "dev", 1)
	waitCondition(t, ns, "dev", metav1.ConditionFalse, lodiav1alpha1.ReasonVolumesExist,
		"dev-etcd-dev-etcd-0, dev-etcd-dev-etcd-1 of another ControlPlane stand where the etcd of dev would be created: delete them to start empty, or keep them, and this ControlPlane waits")
	if condition := meta.FindStatusCondition(getControlPlane(t, ns, "dev").Status.Conditions, lodiav1alpha1.ConditionDatastoreReady); strings.Contains(condition.Message, "dev-etcd-scratch") {
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
	if pvc.Labels[lodiav1alpha1.ControlPlaneUIDLabel] != string(cp.UID) || pvc.Annotations[lodiav1alpha1.DatastoreClassAnnotation] != "reused" {
		t.Errorf("the StatefulSet's claim carries labels %v and annotations %v, want the ControlPlane's UID %s and the class reused", pvc.Labels, pvc.Annotations, cp.UID)
	}
	if pvc := getClaim(t, ns, "dev-etcd-scratch"); pvc.Labels[lodiav1alpha1.ControlPlaneUIDLabel] != "" {
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
		{"another ControlPlane", "succeeded", map[string]string{lodiav1alpha1.ControlPlaneLabel: "succeeded"}, []metav1.OwnerReference{{
			APIVersion: "lodia.io/v1alpha1", Kind: "ControlPlane", Name: "succeeded", UID: "11111111-2222-3333-4444-555555555555",
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
			waitCondition(t, ns, row.cp, metav1.ConditionFalse, lodiav1alpha1.ReasonVolumesExist, row.message)
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
				return gone(k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: row.cp}, &lodiav1alpha1.ControlPlane{}))
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

	class := &lodiav1alpha1.DatastoreClass{}
	if err := k8s.Get(t.Context(), client.ObjectKey{Name: "growing"}, class); err != nil {
		t.Fatal(err)
	}
	class.Spec.Storage = lodiav1alpha1.EtcdStorage{ClassName: "fast", Size: resource.MustParse("2Gi"), ReclaimPolicy: "Retain"}
	class.Spec.Etcd = &lodiav1alpha1.EtcdSettings{QuotaBackendBytes: new(resource.MustParse("3Gi"))}
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
	if got := etcd.Annotations[lodiav1alpha1.DatastoreClassGenerationAnnotation]; got != "2" {
		t.Errorf("class generation %s recorded, want 2", got)
	}
}

func TestRecordedClass(t *testing.T) {
	ns := namespace(t)
	first := newClass("default-first", "Retain")
	first.Annotations = map[string]string{lodiav1alpha1.DefaultClassAnnotation: "true"}
	first.Spec.Storage.Size = resource.MustParse("3Gi")
	createClass(t, first)
	create(t, controlPlane(ns, "dev", ""))
	writeGeneration(t, ns, "dev", 1)
	etcd := waitEtcd(t, ns, "dev", nil)
	if got := etcd.Annotations[lodiav1alpha1.DatastoreClassAnnotation]; got != "default-first" {
		t.Fatalf("class %s recorded, want default-first", got)
	}
	create(t, claim(ns, "dev-etcd-dev-etcd-0", druidv1alpha1.GetDefaultLabels(etcd.ObjectMeta)))
	settle(t, ns, "dev")

	second := newClass("default-second", "Delete")
	second.Annotations = map[string]string{lodiav1alpha1.DefaultClassAnnotation: "true"}
	second.Spec.Storage.Size = resource.MustParse("5Gi")
	createClass(t, second)
	if err := k8s.Get(t.Context(), client.ObjectKey{Name: "default-first"}, first); err != nil {
		t.Fatal(err)
	}
	delete(first.Annotations, lodiav1alpha1.DefaultClassAnnotation)
	if err := k8s.Update(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	settle(t, ns, "dev")
	etcd = getEtcd(t, ns, "dev")
	if etcd.Annotations[lodiav1alpha1.DatastoreClassAnnotation] != "default-first" || quantity(etcd.Spec.StorageCapacity) != "3Gi" {
		t.Errorf("the Etcd follows class %s with %s, want default-first with 3Gi after the default moved", etcd.Annotations[lodiav1alpha1.DatastoreClassAnnotation], quantity(etcd.Spec.StorageCapacity))
	}
	if pvc := getClaim(t, ns, "dev-etcd-dev-etcd-0"); pvc.Annotations[lodiav1alpha1.DatastoreClassAnnotation] != "default-first" {
		t.Errorf("the claim records class %s, want default-first", pvc.Annotations[lodiav1alpha1.DatastoreClassAnnotation])
	}

	if err := k8s.Delete(t.Context(), getControlPlane(t, ns, "dev")); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() error {
		return gone(k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: "dev"}, &lodiav1alpha1.ControlPlane{}))
	})
	untouched(t, ns, "dev-etcd-dev-etcd-0")
}

func reconcileNow(t *testing.T, ns, name string) time.Duration {
	t.Helper()
	result, err := (&datastore.Reconciler{Client: k8s, Reader: k8s, EtcdDruidNamespace: druidNamespace}).Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: name}})
	if err != nil {
		t.Fatal(err)
	}
	return result.RequeueAfter
}

func TestClassSurvivesTheEtcd(t *testing.T) {
	ns := namespace(t)
	first := newClass("survivor-first", "Retain")
	first.Annotations = map[string]string{lodiav1alpha1.DefaultClassAnnotation: "true"}
	first.Spec.Storage.Size = resource.MustParse("3Gi")
	createClass(t, first)
	create(t, controlPlane(ns, "dev", ""))
	writeGeneration(t, ns, "dev", 1)
	etcd := waitEtcd(t, ns, "dev", nil)
	create(t, claim(ns, "dev-etcd-dev-etcd-0", druidv1alpha1.GetDefaultLabels(etcd.ObjectMeta)))
	settle(t, ns, "dev")

	second := newClass("survivor-second", "Delete")
	second.Annotations = map[string]string{lodiav1alpha1.DefaultClassAnnotation: "true"}
	second.Spec.Storage.Size = resource.MustParse("5Gi")
	createClass(t, second)
	if err := k8s.Get(t.Context(), client.ObjectKey{Name: "survivor-first"}, first); err != nil {
		t.Fatal(err)
	}
	delete(first.Annotations, lodiav1alpha1.DefaultClassAnnotation)
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
		if class, size := again.Annotations[lodiav1alpha1.DatastoreClassAnnotation], quantity(again.Spec.StorageCapacity); class != "survivor-first" || size != "3Gi" {
			return fmt.Errorf("the Etcd created again follows class %s with %s, want survivor-first, which its claims record, with 3Gi", class, size)
		}
		return nil
	})

	if err := k8s.Delete(t.Context(), newClass("survivor-first", "")); err != nil {
		t.Fatal(err)
	}
	create(t, controlPlane(ns, "probe", "survivor-first"))
	waitCondition(t, ns, "probe", metav1.ConditionFalse, lodiav1alpha1.ReasonDatastoreClassNotFound, "DatastoreClass survivor-first does not exist")
	if err := k8s.Delete(t.Context(), getEtcd(t, ns, "dev")); err != nil {
		t.Fatal(err)
	}
	waitCondition(t, ns, "dev", metav1.ConditionFalse, lodiav1alpha1.ReasonDatastoreClassNotFound,
		"DatastoreClass survivor-first, which the claims of this ControlPlane record, does not exist")
	if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: "dev-etcd"}, &druidv1alpha1.Etcd{}); !apierrors.IsNotFound(err) {
		t.Fatalf("want no Etcd while the class its claims record is gone, got %v", err)
	}
	if after := reconcileNow(t, ns, "dev"); after != time.Minute {
		t.Errorf("a class the claims record and that is gone requeues after %s, want a minute: the claims are outside the cache", after)
	}

	createClass(t, newClass("survivor-first", "Retain"))
	waitEtcd(t, ns, "dev", func(again *druidv1alpha1.Etcd) error {
		if class := again.Annotations[lodiav1alpha1.DatastoreClassAnnotation]; class != "survivor-first" {
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
	waitCondition(t, ns, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, "has quorum, 1 of 1 members ready")
	create(t, claim(ns, "kept-etcd-kept-etcd-0", nil))
	settle(t, ns, cp)
	before := getEtcd(t, ns, cp)

	if err := k8s.Delete(t.Context(), newClass("vanishing", "")); err != nil {
		t.Fatal(err)
	}
	settle(t, ns, cp)
	waitCondition(t, ns, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, "has quorum, 1 of 1 members ready")

	writeGeneration(t, ns, cp, 2)
	after := waitEtcd(t, ns, cp, namesGeneration(2))
	if got, want := withoutTLS(after), withoutTLS(before); !reflect.DeepEqual(got, want) {
		t.Errorf("the Etcd's fields from the gone class moved:\n got %+v\nwant %+v", got, want)
	}
	for _, key := range []string{lodiav1alpha1.DatastoreClassAnnotation, lodiav1alpha1.DatastoreClassGenerationAnnotation} {
		if after.Annotations[key] != before.Annotations[key] {
			t.Errorf("annotation %s moved from %q to %q", key, before.Annotations[key], after.Annotations[key])
		}
	}
	runStatefulSet(t, ns, cp, 2)
	setEtcdStatus(t, ns, cp, nil)
	waitCondition(t, ns, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, "Etcd kept-etcd has quorum, 1 of 1 members ready")

	if err := k8s.Delete(t.Context(), getControlPlane(t, ns, cp)); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() error {
		return gone(k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: cp}, &lodiav1alpha1.ControlPlane{}))
	})
	untouched(t, ns, "kept-etcd-kept-etcd-0")
}

func TestDeletionBeforeLabelling(t *testing.T) {
	ns := namespace(t)
	createClass(t, newClass("unlabelled", "Delete"))
	create(t, controlPlane(ns, "dev", "unlabelled"))
	writeGeneration(t, ns, "dev", 1)
	waitEtcd(t, ns, "dev", nil)
	refuseEtcds(t, ns)
	settle(t, ns, "dev")
	if c := meta.FindStatusCondition(getControlPlane(t, ns, "dev").Status.Conditions, lodiav1alpha1.ConditionDatastoreReady); c.Reason != lodiav1alpha1.ReasonRefused {
		t.Fatalf("DatastoreReady is %s, want %s: a pass whose apply is not refused labels the claim", c.Reason, lodiav1alpha1.ReasonRefused)
	}

	create(t, claim(ns, "dev-etcd-dev-etcd-0", druidv1alpha1.GetDefaultLabels(metav1.ObjectMeta{Name: "dev-etcd"})))
	if pvc := getClaim(t, ns, "dev-etcd-dev-etcd-0"); pvc.Labels[lodiav1alpha1.ControlPlaneUIDLabel] != "" {
		t.Fatalf("a pass labelled the claim before the deletion: %v", pvc.Labels)
	}
	if err := k8s.Delete(t.Context(), getControlPlane(t, ns, "dev")); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() error {
		return gone(k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: "dev"}, &lodiav1alpha1.ControlPlane{}))
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
			previous[lodiav1alpha1.ControlPlaneUIDLabel] = "11111111-2222-3333-4444-555555555555"
			create(t, claim(ns, "dev-etcd-dev-etcd-0", etcdLabels))
			create(t, claim(ns, "dev-etcd-dev-etcd-1", previous))
			create(t, claim(ns, "someone-elses", nil))
			settle(t, ns, "dev")
			if pvc := getClaim(t, ns, "dev-etcd-dev-etcd-0"); pvc.Labels[lodiav1alpha1.ControlPlaneUIDLabel] != string(cp.UID) || pvc.Annotations[lodiav1alpha1.DatastoreClassAnnotation] != className {
				t.Fatalf("the StatefulSet's claim carries labels %v and annotations %v, want the ControlPlane's UID %s and the class %s", pvc.Labels, pvc.Annotations, cp.UID, className)
			}

			held := client.MergeFrom(etcd.DeepCopy())
			etcd.Finalizers = append(etcd.Finalizers, "lodia.io/test")
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
			if !controllerutil.ContainsFinalizer(getControlPlane(t, ns, "dev"), lodiav1alpha1.DatastoreFinalizer) {
				t.Fatal("the finalizer left before the Etcd was gone")
			}
			waitCondition(t, ns, "dev", metav1.ConditionFalse, lodiav1alpha1.ReasonDeleting, "the ControlPlane is being deleted")
			untouched(t, ns, "dev-etcd-dev-etcd-0")
			untouched(t, ns, "dev-etcd-dev-etcd-1")
			if err := k8s.Delete(t.Context(), waitPolicy(t, ns, "dev", nil)); err != nil {
				t.Fatal(err)
			}

			released := client.MergeFrom(etcd.DeepCopy())
			etcd.Finalizers = nil
			if err := k8s.Patch(t.Context(), etcd, released); err != nil {
				t.Fatal(err)
			}
			eventually(t, func() error {
				return gone(k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: "dev"}, &lodiav1alpha1.ControlPlane{}))
			})
			if err := gone(k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: "dev-etcd"}, &druidv1alpha1.Etcd{})); err != nil {
				t.Errorf("the Etcd: %v", err)
			}
			if err := gone(k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: "dev-etcd"}, &networkingv1.NetworkPolicy{})); err != nil {
				t.Errorf("a pass of the deletion wrote the NetworkPolicy again: %v", err)
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

func waitPolicy(t *testing.T, ns, cp string, check func(*networkingv1.NetworkPolicy) error) *networkingv1.NetworkPolicy {
	t.Helper()
	policy := &networkingv1.NetworkPolicy{}
	eventually(t, func() error {
		if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: cp + "-etcd"}, policy); err != nil {
			return err
		}
		if check != nil {
			return check(policy)
		}
		return nil
	})
	return policy
}

func policySpec(cp string, store bool) networkingv1.NetworkPolicySpec {
	members := &metav1.LabelSelector{MatchLabels: map[string]string{
		"app.kubernetes.io/managed-by": "etcd-druid", "app.kubernetes.io/part-of": cp + "-etcd", "app.kubernetes.io/name": cp + "-etcd",
	}}
	apiServer := &metav1.LabelSelector{MatchLabels: map[string]string{"lodia.io/control-plane": cp, "app.kubernetes.io/name": "kube-apiserver"}}
	tcp := func(port int32) networkingv1.NetworkPolicyPort {
		return networkingv1.NetworkPolicyPort{Protocol: new(corev1.ProtocolTCP), Port: new(intstr.FromInt32(port))}
	}
	memberPorts := []networkingv1.NetworkPolicyPort{tcp(2379), tcp(2380)}
	druidPorts := []networkingv1.NetworkPolicyPort{tcp(2379)}
	if store {
		memberPorts = append(memberPorts, tcp(8080))
		druidPorts = append(druidPorts, tcp(8080))
	}
	return networkingv1.NetworkPolicySpec{
		PodSelector: *members,
		Ingress: []networkingv1.NetworkPolicyIngressRule{
			{From: []networkingv1.NetworkPolicyPeer{{PodSelector: apiServer}}, Ports: []networkingv1.NetworkPolicyPort{tcp(2379)}},
			{From: []networkingv1.NetworkPolicyPeer{{PodSelector: members}}, Ports: memberPorts},
			{
				From:  []networkingv1.NetworkPolicyPeer{{NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"kubernetes.io/metadata.name": druidNamespace}}}},
				Ports: druidPorts,
			},
		},
		PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress},
	}
}

func sameSpec(t *testing.T, got, want networkingv1.NetworkPolicySpec) {
	t.Helper()
	g, err := yaml.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	w, err := yaml.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(g, w) {
		t.Errorf("the NetworkPolicy's spec is\n%s\nwant\n%s", g, w)
	}
}

func TestNetworkPolicy(t *testing.T) {
	ns, credentialsNamespace := namespace(t), namespace(t)
	create(t, credentials(credentialsNamespace))
	createClass(t, fullClass("isolating", credentialsNamespace))
	createClass(t, newClass("isolating-plain", "Retain"))
	create(t, controlPlane(ns, "backed", "isolating"))
	create(t, controlPlane(ns, "plain", "isolating-plain"))
	for name, class := range map[string]string{"backed-ha": "isolating", "plain-ha": "isolating-plain"} {
		ha := controlPlane(ns, name, class)
		ha.Spec.HighAvailability = &lodiav1alpha1.HighAvailability{FailureTolerance: "Node"}
		create(t, ha)
	}
	waitCondition(t, ns, "plain", metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForCertificates, "waiting for a complete generation")
	if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: "plain-etcd"}, &networkingv1.NetworkPolicy{}); !apierrors.IsNotFound(err) {
		t.Fatalf("want no NetworkPolicy before the Etcd, got %v", err)
	}

	for _, row := range []struct {
		cp    string
		store bool
	}{{"backed", true}, {"plain", false}, {"backed-ha", true}, {"plain-ha", false}} {
		t.Run(row.cp, func(t *testing.T) {
			writeGeneration(t, ns, row.cp, 1)
			policy := waitPolicy(t, ns, row.cp, nil)
			cp := getControlPlane(t, ns, row.cp)
			sameSpec(t, policy.Spec, policySpec(row.cp, row.store))
			if want := map[string]string{"lodia.io/control-plane": row.cp}; !reflect.DeepEqual(policy.Labels, want) {
				t.Errorf("labels %v, want %v", policy.Labels, want)
			}
			if want := []metav1.OwnerReference{{
				APIVersion: "lodia.io/v1alpha1", Kind: "ControlPlane", Name: row.cp, UID: cp.UID, Controller: new(true), BlockOwnerDeletion: new(true),
			}}; !reflect.DeepEqual(policy.OwnerReferences, want) {
				t.Errorf("ownerReferences %+v, want %+v", policy.OwnerReferences, want)
			}

			settle(t, ns, row.cp)
			if again := getPolicy(t, ns, row.cp); again.ResourceVersion != policy.ResourceVersion {
				t.Fatalf("a further pass moved the NetworkPolicy from resourceVersion %s to %s", policy.ResourceVersion, again.ResourceVersion)
			}

			if err := k8s.Delete(t.Context(), policy); err != nil {
				t.Fatal(err)
			}
			again := waitPolicy(t, ns, row.cp, func(p *networkingv1.NetworkPolicy) error {
				if p.UID == policy.UID {
					return errors.New("the deleted NetworkPolicy is still there")
				}
				return nil
			})
			sameSpec(t, again.Spec, policySpec(row.cp, row.store))
		})
	}
}

func getPolicy(t *testing.T, ns, cp string) *networkingv1.NetworkPolicy {
	t.Helper()
	policy := &networkingv1.NetworkPolicy{}
	if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: cp + "-etcd"}, policy); err != nil {
		t.Fatal(err)
	}
	return policy
}

func TestNetworkPolicyOfAnotherOwner(t *testing.T) {
	createClass(t, newClass("policed", "Retain"))
	rows := []struct {
		name, cp string
		labels   map[string]string
		owner    metav1.OwnerReference
		message  string
	}{
		{"a previous ControlPlane", "succeeded", map[string]string{lodiav1alpha1.ControlPlaneLabel: "succeeded"}, metav1.OwnerReference{
			APIVersion: "lodia.io/v1alpha1", Kind: "ControlPlane", Name: "succeeded", UID: "11111111-2222-3333-4444-555555555555",
			Controller: new(true), BlockOwnerDeletion: new(true),
		}, "another owner keeps NetworkPolicy succeeded-etcd by ControlPlane succeeded (UID 11111111-2222-3333-4444-555555555555)"},
		{"another controller, unlabelled", "fenced", nil, metav1.OwnerReference{
			APIVersion: "example.com/v1", Kind: "Fence", Name: "edge", UID: "66666666-7777-8888-9999-000000000000", Controller: new(true),
		}, "another owner keeps NetworkPolicy fenced-etcd by Fence edge (UID 66666666-7777-8888-9999-000000000000)"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			ns := namespace(t)
			foreign := &networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{
				Name: row.cp + "-etcd", Namespace: ns, Labels: row.labels, OwnerReferences: []metav1.OwnerReference{row.owner},
			}}
			create(t, foreign)
			create(t, controlPlane(ns, row.cp, "policed"))
			writeGeneration(t, ns, row.cp, 1)
			waitEtcd(t, ns, row.cp, namesGeneration(1))
			waitCondition(t, ns, row.cp, metav1.ConditionFalse, lodiav1alpha1.ReasonProvisioning, "etcd-druid has not observed generation 1")
			runStatefulSet(t, ns, row.cp, 1)
			setEtcdStatus(t, ns, row.cp, nil)
			waitCondition(t, ns, row.cp, metav1.ConditionFalse, lodiav1alpha1.ReasonObjectsControlledElsewhere, row.message)
			if after := reconcileNow(t, ns, row.cp); after != time.Minute {
				t.Errorf("a NetworkPolicy another owner keeps requeues after %s, want a minute: an unlabelled one is outside the cache", after)
			}
			if again := getPolicy(t, ns, row.cp); again.ResourceVersion != foreign.ResourceVersion {
				t.Fatalf("the NetworkPolicy another owner keeps moved from resourceVersion %s to %s: owners %+v", foreign.ResourceVersion, again.ResourceVersion, again.OwnerReferences)
			}

			if err := k8s.Delete(t.Context(), foreign); err != nil {
				t.Fatal(err)
			}
			settle(t, ns, row.cp)
			waitCondition(t, ns, row.cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, "has quorum, 1 of 1 members ready")
			cp := getControlPlane(t, ns, row.cp)
			waitPolicy(t, ns, row.cp, func(p *networkingv1.NetworkPolicy) error {
				if !metav1.IsControlledBy(p, cp) {
					return fmt.Errorf("the NetworkPolicy is controlled by %v", metav1.GetControllerOf(p))
				}
				return nil
			})
		})
	}
}

func TestOwnerlessNetworkPolicyIsAdopted(t *testing.T) {
	ns := namespace(t)
	createClass(t, newClass("adopting-policy", "Retain"))
	create(t, &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "adopter-etcd", Namespace: ns},
		Spec: networkingv1.NetworkPolicySpec{
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress},
			Ingress:     []networkingv1.NetworkPolicyIngressRule{{}},
		},
	})
	create(t, controlPlane(ns, "adopter", "adopting-policy"))
	writeGeneration(t, ns, "adopter", 1)
	cp := getControlPlane(t, ns, "adopter")
	policy := waitPolicy(t, ns, "adopter", func(p *networkingv1.NetworkPolicy) error {
		if !metav1.IsControlledBy(p, cp) {
			return fmt.Errorf("the NetworkPolicy is controlled by %v", metav1.GetControllerOf(p))
		}
		return nil
	})
	sameSpec(t, policy.Spec, policySpec("adopter", false))
}

func TestNetworkPolicyRefused(t *testing.T) {
	ns := namespace(t)
	const cp = "walled"
	createClass(t, newClass("walling", "Retain"))
	binding := refuse(t, ns, admissionregistrationv1.Rule{APIGroups: []string{networkingv1.GroupName}, APIVersions: []string{"v1"}, Resources: []string{"networkpolicies"}}, func() client.Object {
		return &networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Name: "probe", Namespace: ns}}
	})
	create(t, controlPlane(ns, cp, "walling"))
	writeGeneration(t, ns, cp, 1)
	waitEtcd(t, ns, cp, namesGeneration(1))
	waitCondition(t, ns, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonProvisioning, "etcd-druid has not observed generation 1")
	runStatefulSet(t, ns, cp, 1)
	setEtcdStatus(t, ns, cp, nil)
	waitCondition(t, ns, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonRefused, "the API server refuses NetworkPolicy walled-etcd; the reason is in Lodia's log")
	if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: cp + "-etcd"}, &networkingv1.NetworkPolicy{}); !apierrors.IsNotFound(err) {
		t.Fatalf("want no NetworkPolicy while the API server refuses it, got %v", err)
	}

	if err := k8s.Delete(t.Context(), binding); err != nil {
		t.Fatal(err)
	}
	waitPolicy(t, ns, cp, nil)
	waitCondition(t, ns, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, "has quorum, 1 of 1 members ready")
}

func TestAFailedNetworkPolicyCallLeavesTheStatus(t *testing.T) {
	ns := namespace(t)
	const cp = "flaky"
	createClass(t, newClass("flaking", "Retain"))
	create(t, controlPlane(ns, cp, "flaking"))
	writeGeneration(t, ns, cp, 1)
	waitEtcd(t, ns, cp, namesGeneration(1))
	readyOn(t, ns, cp, 1)
	waitPolicy(t, ns, cp, nil)
	settle(t, ns, cp)
	before := getControlPlane(t, ns, cp)
	injected := errors.New("injected: the API server did not answer")
	lagging := func(failPolicies bool) func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
		return func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*networkingv1.NetworkPolicy); ok && failPolicies {
				return injected
			}
			if err := c.Get(ctx, key, obj, opts...); err != nil {
				return err
			}
			if sts, ok := obj.(*appsv1.StatefulSet); ok {
				sts.Status.UpdatedReplicas = 0
			}
			return nil
		}
	}
	for _, row := range []struct {
		name  string
		funcs interceptor.Funcs
		err   error
	}{
		{"the read", interceptor.Funcs{Get: lagging(true)}, injected},
		{"the apply, past its deadline", interceptor.Funcs{Get: lagging(false), Apply: func(ctx context.Context, c client.WithWatch, obj runtime.ApplyConfiguration, opts ...client.ApplyOption) error {
			if _, ok := obj.(*networkingv1ac.NetworkPolicyApplyConfiguration); ok {
				return context.DeadlineExceeded
			}
			return c.Apply(ctx, obj, opts...)
		}}, context.DeadlineExceeded},
	} {
		t.Run(row.name, func(t *testing.T) {
			failing := interceptor.NewClient(k8s, row.funcs)
			reconciler := &datastore.Reconciler{Client: failing, Reader: failing, EtcdDruidNamespace: druidNamespace}
			if _, err := reconciler.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: cp}}); !errors.Is(err, row.err) {
				t.Fatalf("the pass returned %v, want %v", err, row.err)
			}
			if after := getControlPlane(t, ns, cp); after.ResourceVersion != before.ResourceVersion {
				condition := meta.FindStatusCondition(after.Status.Conditions, lodiav1alpha1.ConditionDatastoreReady)
				t.Errorf("the pass moved the ControlPlane from resourceVersion %s to %s: DatastoreReady %s/%s %q",
					before.ResourceVersion, after.ResourceVersion, condition.Status, condition.Reason, condition.Message)
			}
		})
	}
}
