// Copyright 2026 The Brygga Authors.
// SPDX-License-Identifier: Apache-2.0

package datastore_test

import (
	"context"
	"errors"
	"fmt"
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
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/util/wait"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/yaml"

	bryggav1alpha1 "github.com/segla-io/brygga/api/v1alpha1"
	"github.com/segla-io/brygga/internal/controller/datastore"
	"github.com/segla-io/brygga/internal/controller/datastore/datastoretest"
)

var k8s client.Client

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
	if k8s, err = client.New(cfg, client.Options{Scheme: scheme}); err != nil {
		stop("client", err)
	}
	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))
	labelled, err := labels.NewRequirement(bryggav1alpha1.ControlPlaneLabel, selection.Exists, nil)
	if err != nil {
		stop("label selector", err)
	}
	everything := cache.ByObject{Label: labels.Everything()}
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:  scheme,
		Metrics: metricsserver.Options{BindAddress: "0"},
		Cache: cache.Options{
			DefaultLabelSelector:        labels.NewSelector().Add(*labelled),
			DefaultTransform:            cache.TransformStripManagedFields(),
			ReaderFailOnMissingInformer: true,
			ByObject: map[client.Object]cache.ByObject{
				&bryggav1alpha1.ControlPlane{}:   everything,
				&bryggav1alpha1.DatastoreClass{}: everything,
			},
		},
	})
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

func writeGeneration(t *testing.T, ns, cp string, generation int, only ...string) {
	t.Helper()
	secrets, err := datastoretest.Generation(ns, cp, generation)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range secrets {
		if len(only) > 0 && !slices.ContainsFunc(only, func(certificate string) bool {
			return secret.Name == datastore.SecretName(cp, certificate, generation)
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

func waitCondition(t *testing.T, ns, name string, status metav1.ConditionStatus, reason, message string) {
	t.Helper()
	eventually(t, func() error {
		cp := &bryggav1alpha1.ControlPlane{}
		if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: name}, cp); err != nil {
			return err
		}
		condition := meta.FindStatusCondition(cp.Status.Conditions, datastore.ConditionType)
		switch {
		case condition == nil:
			return fmt.Errorf("no %s condition", datastore.ConditionType)
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

// settle bumps the ControlPlane's generation and waits for the controller to
// report it, so that a reconcile is known to have run.
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
		condition := meta.FindStatusCondition(current.Status.Conditions, datastore.ConditionType)
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
		if !controllerutil.ContainsFinalizer(getControlPlane(t, ns, "dev"), datastore.Finalizer) {
			return fmt.Errorf("the finalizer %s is not on the ControlPlane", datastore.Finalizer)
		}
		return nil
	})
	waitCondition(t, ns, "dev", metav1.ConditionFalse, datastore.ReasonProvisioning, "waiting for a complete generation of the etcd certificates")
	if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: "dev-etcd"}, &druidv1alpha1.Etcd{}); !apierrors.IsNotFound(err) {
		t.Fatalf("want no Etcd before the first certificate generation, got %v", err)
	}

	writeGeneration(t, ns, "dev", 1)
	etcd := waitEtcd(t, ns, "dev", namesGeneration(1))
	cp := getControlPlane(t, ns, "dev")
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
		{"backup.store.prefix → spec.backup.store.prefix, the tenant below the class's prefix", store.Prefix, "prod/" + ns + "/dev"},
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
		{"generation 1 → spec.etcd.clientUrlTls", tls(etcd.Spec.Etcd.ClientUrlTLS), "ca dev-etcd-ca-1/ca.crt server dev-etcd-server-1 client dev-etcd-client-1"},
		{"generation 1 → spec.etcd.peerUrlTls", peerTLS(etcd), "ca dev-etcd-ca-1/ca.crt server dev-etcd-peer-1 client "},
		{"generation 1 → spec.backup.tls", tls(etcd.Spec.Backup.TLS), "ca dev-etcd-ca-1/ca.crt server dev-etcd-backup-restore-1 client dev-etcd-client-1"},
		{"the ControlPlane → metadata.labels", etcd.Labels[bryggav1alpha1.ControlPlaneLabel], "dev"},
		{"the ControlPlane → spec.labels", etcd.Spec.Labels[bryggav1alpha1.ControlPlaneLabel], "dev"},
		{"the ControlPlane → metadata.ownerReferences", etcd.OwnerReferences, []metav1.OwnerReference{{
			APIVersion: "brygga.segla.io/v1alpha1", Kind: "ControlPlane", Name: "dev", UID: cp.UID, Controller: new(true), BlockOwnerDeletion: new(true),
		}}},
		{"the Etcd → status.datastore", ptr.Deref(cp.Status.Datastore, bryggav1alpha1.DatastoreStatus{}), bryggav1alpha1.DatastoreStatus{Kind: "Dedicated", Name: "dev-etcd"}},
	}
	for _, row := range rows {
		t.Run(row.field, func(t *testing.T) {
			if !reflect.DeepEqual(row.got, row.want) {
				t.Errorf("got %+v, want %+v", row.got, row.want)
			}
		})
	}
}

func TestNewestCompleteGeneration(t *testing.T) {
	ns := namespace(t)
	createClass(t, newClass("generations", "Retain"))
	create(t, controlPlane(ns, "dev", "generations"))
	writeGeneration(t, ns, "dev", 1)
	waitEtcd(t, ns, "dev", namesGeneration(1))

	writeGeneration(t, ns, "dev", 2, "ca", "server", "peer", "backup-restore")
	settle(t, ns, "dev")
	if err := namesGeneration(1)(getEtcd(t, ns, "dev")); err != nil {
		t.Fatalf("four of five Secrets of generation 2: %v", err)
	}

	secrets, err := datastoretest.Generation(ns, "dev", 2)
	if err != nil {
		t.Fatal(err)
	}
	terminating := secrets[slices.IndexFunc(secrets, func(s *corev1.Secret) bool { return s.Name == datastore.SecretName("dev", "client", 2) })]
	terminating.Finalizers = []string{"brygga.segla.io/test"}
	create(t, terminating)
	if err := k8s.Delete(t.Context(), terminating); err != nil {
		t.Fatal(err)
	}
	settle(t, ns, "dev")
	if err := namesGeneration(1)(getEtcd(t, ns, "dev")); err != nil {
		t.Fatalf("the fifth Secret of generation 2 being deleted: %v", err)
	}
	patch := client.MergeFrom(terminating.DeepCopy())
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

	writeGeneration(t, ns, "dev", 2, "client")
	etcd := waitEtcd(t, ns, "dev", namesGeneration(2))

	settle(t, ns, "dev")
	if again := getEtcd(t, ns, "dev"); again.ResourceVersion != etcd.ResourceVersion {
		t.Fatalf("a further reconcile moved the Etcd from resourceVersion %s to %s", etcd.ResourceVersion, again.ResourceVersion)
	}
}

func TestDatastoreClassNotFound(t *testing.T) {
	ns := namespace(t)
	create(t, controlPlane(ns, "named", "absent"))
	waitCondition(t, ns, "named", metav1.ConditionFalse, datastore.ReasonDatastoreClassNotFound, "DatastoreClass absent does not exist")

	create(t, controlPlane(ns, "unnamed", ""))
	waitCondition(t, ns, "unnamed", metav1.ConditionFalse, datastore.ReasonDatastoreClassNotFound, "no DatastoreClass is annotated")

	for _, name := range []string{"default-a", "default-b"} {
		class := newClass(name, "Retain")
		class.Annotations = map[string]string{datastore.DefaultClassAnnotation: "true"}
		class.Spec.Storage.Size = resource.MustParse("3Gi")
		createClass(t, class)
	}
	waitCondition(t, ns, "unnamed", metav1.ConditionFalse, datastore.ReasonDatastoreClassNotFound, "2 DatastoreClasses are annotated")

	if err := k8s.Delete(t.Context(), newClass("default-b", "")); err != nil {
		t.Fatal(err)
	}
	waitCondition(t, ns, "unnamed", metav1.ConditionFalse, datastore.ReasonProvisioning, "waiting for a complete generation")
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
	waitCondition(t, ns, "backed", metav1.ConditionFalse, datastore.ReasonProvisioning, "read the credentials Secret "+credentialsNamespace+"/s3 of DatastoreClass backed-up")

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

func TestDatastoreReadyFollowsEtcdStatus(t *testing.T) {
	ns := namespace(t)
	createClass(t, newClass("reported", "Retain"))
	create(t, controlPlane(ns, "reported", "reported"))
	writeGeneration(t, ns, "reported", 1)
	etcd := waitEtcd(t, ns, "reported", nil)
	waitCondition(t, ns, "reported", metav1.ConditionFalse, datastore.ReasonProvisioning, "etcd-druid has not reported Etcd reported-etcd ready for generation 1")

	etcd.Status = druidv1alpha1.EtcdStatus{
		ObservedGeneration: new(etcd.Generation),
		Ready:              new(false),
		Replicas:           1,
		LastErrors:         []druidapicommon.LastError{{Code: "ERR_SYNC_STATEFULSET", Description: "pod reported-etcd-0 is not ready", ObservedAt: metav1.Now()}},
	}
	if err := k8s.Status().Update(t.Context(), etcd); err != nil {
		t.Fatal(err)
	}
	waitCondition(t, ns, "reported", metav1.ConditionFalse, datastore.ReasonProvisioning, "last errors: ERR_SYNC_STATEFULSET: pod reported-etcd-0 is not ready")

	etcd = getEtcd(t, ns, "reported")
	etcd.Status.Ready = new(true)
	etcd.Status.ReadyReplicas = 1
	etcd.Status.LastErrors = nil
	etcd.Status.Members = []druidv1alpha1.EtcdMemberStatus{{
		Name: "reported-etcd-0", ID: new("8e9e05c52164694d"), Role: new(druidv1alpha1.EtcdRoleLeader),
		Status: druidv1alpha1.EtcdMemberStatusReady, Reason: "LeaseSucceeded", LastTransitionTime: metav1.Now(),
	}}
	if err := k8s.Status().Update(t.Context(), etcd); err != nil {
		t.Fatal(err)
	}
	waitCondition(t, ns, "reported", metav1.ConditionTrue, datastore.ReasonReady, "etcd-druid reports Etcd reported-etcd ready, 1 of 1 replicas")
	if got, want := ptr.Deref(getControlPlane(t, ns, "reported").Status.Datastore, bryggav1alpha1.DatastoreStatus{}), (bryggav1alpha1.DatastoreStatus{Kind: "Dedicated", Name: "reported-etcd", Members: 1}); got != want {
		t.Errorf("status.datastore %+v, want %+v", got, want)
	}
}

func TestDeletion(t *testing.T) {
	for _, policy := range []string{"Delete", "Retain"} {
		t.Run(policy, func(t *testing.T) {
			ns := namespace(t)
			createClass(t, newClass("reclaim-"+strings.ToLower(policy), policy))
			create(t, controlPlane(ns, "dev", "reclaim-"+strings.ToLower(policy)))
			writeGeneration(t, ns, "dev", 1)
			etcd := waitEtcd(t, ns, "dev", nil)

			claim := func(name string, labels map[string]string) *corev1.PersistentVolumeClaim {
				return &corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Labels: labels},
					Spec: corev1.PersistentVolumeClaimSpec{
						AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
						Resources:   corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")}},
					},
				}
			}
			create(t, claim("dev-etcd-dev-etcd-0", druidv1alpha1.GetDefaultLabels(etcd.ObjectMeta)))
			create(t, claim("someone-elses", nil))

			if err := k8s.Delete(t.Context(), getControlPlane(t, ns, "dev")); err != nil {
				t.Fatal(err)
			}
			eventually(t, func() error {
				err := k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: "dev"}, &bryggav1alpha1.ControlPlane{})
				switch {
				case err == nil:
					return errors.New("the ControlPlane is still there")
				case !apierrors.IsNotFound(err):
					return err
				}
				return nil
			})
			if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: "dev-etcd"}, &druidv1alpha1.Etcd{}); !apierrors.IsNotFound(err) {
				t.Errorf("want the Etcd gone, got %v", err)
			}
			members := &corev1.PersistentVolumeClaim{}
			err := k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: "dev-etcd-dev-etcd-0"}, members)
			switch {
			case policy == "Delete" && !apierrors.IsNotFound(err) && (err != nil || members.DeletionTimestamp.IsZero()):
				t.Errorf("want the member's claim deleted, got %v, deletionTimestamp %v", err, members.DeletionTimestamp)
			case policy == "Retain" && (err != nil || !members.DeletionTimestamp.IsZero()):
				t.Errorf("want the member's claim kept, got %v, deletionTimestamp %v", err, members.DeletionTimestamp)
			}
			other := &corev1.PersistentVolumeClaim{}
			if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: "someone-elses"}, other); err != nil || !other.DeletionTimestamp.IsZero() {
				t.Errorf("want the unlabelled claim untouched, got %v, deletionTimestamp %v", err, other.DeletionTimestamp)
			}
		})
	}
}
