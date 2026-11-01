// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

//go:build e2e && restore

package e2e

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"maps"
	"path"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	certificatesv1 "k8s.io/api/certificates/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

const (
	restoreTenant = "team-restore"
	restoreClass  = "e2e-restore"
	restoreSource = "a"
	s3Namespace   = "lodia-e2e-s3"
	s3Bucket      = "lodia-e2e"
	s3Secret      = "e2e-s3"
	s3Region      = "us-east-1"
	versitygw     = "docker.io/versity/versitygw:v1.10.0@sha256:89cad13cc981986ee284cbafea4613f9533ce6fd5f37a2c96c4c4fb8adb3a316"
	awsCLI        = "docker.io/amazon/aws-cli:2.39.3@sha256:d557b3524475d70dd3104cf44d96568ee3188897b73a6ece2b37aa50e1af362f"
)

var s3Endpoint = "http://versitygw." + s3Namespace + ".svc:7070"

func TestRestoreGate(t *testing.T) {
	var (
		k8s    client.Client
		access *tenantAccess
		hashes map[string]string
	)
	key := client.ObjectKey{Namespace: restoreTenant, Name: restoreSource}
	f := features.New("gate G3 on kind: a Backup sealed on etcd-druid and an S3 store").
		Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			k8s = newClient(t, cfg)
			startStore(ctx, t, k8s)
			create(ctx, t, k8s, &lodiav1alpha1.DatastoreClass{
				ObjectMeta: metav1.ObjectMeta{Name: restoreClass},
				Spec: lodiav1alpha1.DatastoreClassSpec{
					Storage: lodiav1alpha1.EtcdStorage{Size: resource.MustParse("8Gi"), ReclaimPolicy: "Retain"},
					Backup: &lodiav1alpha1.EtcdBackup{
						Store: lodiav1alpha1.BackupStore{
							Provider: "S3", Bucket: s3Bucket, Prefix: "e2e", Endpoint: s3Endpoint,
							SecretRef: lodiav1alpha1.SecretReference{Name: s3Secret, Namespace: operatorNamespace},
						},
						DeltaSnapshotPeriod: &metav1.Duration{Duration: 10 * time.Second},
					},
				},
			})
			create(ctx, t, k8s, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: key.Namespace}})
			create(ctx, t, k8s, &lodiav1alpha1.ControlPlane{
				ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace},
				Spec: lodiav1alpha1.ControlPlaneSpec{
					Version:   kindNodeVersion(t),
					Datastore: lodiav1alpha1.DatastoreChoice{Dedicated: &lodiav1alpha1.DedicatedDatastore{ClassName: restoreClass}},
					Endpoint:  lodiav1alpha1.Endpoint{NodePort: &lodiav1alpha1.NodePortEndpoint{}},
					Network:   lodiav1alpha1.Network{ServiceCIDRs: []lodiav1alpha1.CIDR{"10.32.0.0/16"}, PodCIDRs: []lodiav1alpha1.CIDR{gatePodCIDR}},
				},
			})
			return ctx
		}).
		Assess("1. the source is Ready and etcd-druid reports its snapshots", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			until(ctx, t, 5*time.Minute, "Ready=True", func(ctx context.Context) error {
				return controlPlaneReady(ctx, k8s, key, &lodiav1alpha1.ControlPlane{})
			})
			until(ctx, t, 3*time.Minute, "BackupReady=True, BackupSucceeded", func(ctx context.Context) error {
				cp := &lodiav1alpha1.ControlPlane{}
				if err := k8s.Get(ctx, key, cp); err != nil {
					return err
				}
				if c := meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionBackupReady); c == nil || c.Status != metav1.ConditionTrue || c.Reason != lodiav1alpha1.ReasonBackupSucceeded {
					return fmt.Errorf("BackupReady is %+v", c)
				}
				return nil
			})
			return ctx
		}).
		Assess("2. the source holds data and objects that bind it to its world", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			access = openTenant(ctx, t, k8s, key)
			hashes = writeSourceData(ctx, t, access.client)
			bootstrapToken(ctx, t, access.client)
			for _, name := range slices.Sorted(maps.Keys(hashes)) {
				t.Logf("%s: %s", name, hashes[name])
			}
			return ctx
		}).
		Assess("3. a Backup of the source is sealed and copied", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			backup := &lodiav1alpha1.Backup{
				ObjectMeta: metav1.ObjectMeta{Name: restoreSource + "-1", Namespace: key.Namespace},
				Spec:       lodiav1alpha1.BackupSpec{ControlPlaneRef: lodiav1alpha1.LocalReference{Name: key.Name}},
			}
			create(ctx, t, k8s, backup)
			until(ctx, t, 3*time.Minute, "the Backup is Ready, Completed", func(ctx context.Context) error {
				if err := k8s.Get(ctx, client.ObjectKeyFromObject(backup), backup); err != nil {
					return err
				}
				if c := meta.FindStatusCondition(backup.Status.Conditions, lodiav1alpha1.ConditionReady); c == nil || c.Status != metav1.ConditionTrue || c.Reason != lodiav1alpha1.ReasonCompleted {
					return fmt.Errorf("Ready is %+v", c)
				}
				return nil
			})
			ready := meta.FindStatusCondition(backup.Status.Conditions, lodiav1alpha1.ConditionReady)
			t.Logf("Backup %s: %s", backup.Name, ready.Message)
			source := &lodiav1alpha1.ControlPlane{}
			if err := k8s.Get(ctx, key, source); err != nil {
				t.Fatal(err)
			}
			ns := &corev1.Namespace{}
			if err := k8s.Get(ctx, client.ObjectKey{Name: key.Namespace}, ns); err != nil {
				t.Fatal(err)
			}
			status := backup.Status
			if status.Source == nil || status.Source.UID != source.UID {
				t.Errorf("status.source is %+v, want the source's UID %s", status.Source, source.UID)
			}
			if want := path.Join("e2e", string(ns.UID), "backups", string(backup.UID)); status.Snapshots == nil || status.Snapshots.Prefix != want {
				t.Errorf("status.snapshots is %+v, want the prefix %s", status.Snapshots, want)
			}
			if status.Seal == nil || status.Seal.Revision <= 0 {
				t.Errorf("status.seal is %+v, want a revision", status.Seal)
			}
			if mark := backup.Annotations[lodiav1alpha1.SealedUIDAnnotation]; mark != string(backup.UID) {
				until(ctx, t, time.Minute, "the Backup carries its UID in "+lodiav1alpha1.SealedUIDAnnotation, func(ctx context.Context) error {
					if err := k8s.Get(ctx, client.ObjectKeyFromObject(backup), backup); err != nil {
						return err
					}
					if mark := backup.Annotations[lodiav1alpha1.SealedUIDAnnotation]; mark != string(backup.UID) {
						return fmt.Errorf("%s is %q", lodiav1alpha1.SealedUIDAnnotation, mark)
					}
					return nil
				})
			}
			keys := &corev1.Secret{}
			if err := k8s.Get(ctx, client.ObjectKey{Namespace: key.Namespace, Name: lodiav1alpha1.BackupEncryptionConfigSecretName(backup.Name)}, keys); err != nil {
				t.Fatal(err)
			}
			if keys.Immutable == nil || !*keys.Immutable || !metav1.IsControlledBy(keys, backup) {
				t.Errorf("%s is immutable %v with owners %+v, want immutable and controlled by the Backup", keys.Name, keys.Immutable, keys.OwnerReferences)
			}
			until(ctx, t, time.Minute, "the Backup's tasks are deleted", func(ctx context.Context) error {
				name := client.ObjectKey{Namespace: key.Namespace, Name: "backup-" + string(backup.UID)}
				for _, task := range []client.Object{&druidv1alpha1.EtcdOpsTask{}, &druidv1alpha1.EtcdCopyBackupsTask{}} {
					if err := gone(k8s.Get(ctx, name, task)); err != nil {
						return err
					}
				}
				return nil
			})
			if status.Snapshots != nil && status.Seal != nil {
				logs := kubernetes.NewForConfigOrDie(cfg.Client().RESTConfig())
				keys := strings.Fields(string(awsOutput(ctx, t, k8s, logs, "list-"+backup.Name, "s3api", "list-objects-v2", "--bucket", s3Bucket, "--prefix", status.Snapshots.Prefix+"/", "--query", "Contents[].Key", "--output", "text")))
				var last int64
				for _, key := range keys {
					name := strings.Split(path.Base(key), "-")
					if len(name) != 4 || name[0] != "Full" {
						continue
					}
					if revision, err := strconv.ParseInt(name[2], 10, 64); err == nil {
						last = max(last, revision)
					}
				}
				if last < status.Seal.Revision {
					t.Errorf("the copy under %s holds full snapshots up to revision %d, below the seal's %d: %v", status.Snapshots.Prefix, last, status.Seal.Revision, keys)
				}
			}
			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			closeTenant(ctx, t, k8s, key, restoreClass)
			for _, obj := range []client.Object{
				&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: s3Namespace}},
				&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: operatorNamespace, Name: s3Secret}},
			} {
				if k8s != nil {
					if err := client.IgnoreNotFound(k8s.Delete(ctx, obj)); err != nil {
						t.Error(err)
					}
				}
			}
			return ctx
		}).
		Feature()
	testenv.Test(t, f)
}

func startStore(ctx context.Context, t *testing.T, k8s client.Client) {
	t.Helper()
	access, secret := random(t, 10), random(t, 20)
	labels := map[string]string{"app.kubernetes.io/name": "versitygw"}
	keys := map[string]string{"accessKeyID": access, "secretAccessKey": secret}
	create(ctx, t, k8s, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: s3Namespace}})
	create(ctx, t, k8s, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: s3Namespace, Name: "root"}, StringData: keys})
	create(ctx, t, k8s, &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: s3Namespace, Name: "versitygw"},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:           "versitygw",
						Image:          versitygw,
						Args:           []string{"--port", ":7070", "posix", "/data"},
						Env:            []corev1.EnvVar{rootKey("ROOT_ACCESS_KEY", "accessKeyID"), rootKey("ROOT_SECRET_KEY", "secretAccessKey")},
						Ports:          []corev1.ContainerPort{{Name: "s3", ContainerPort: 7070}},
						ReadinessProbe: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromString("s3")}}},
						VolumeMounts:   []corev1.VolumeMount{{Name: "data", MountPath: "/data"}},
					}},
					Volumes: []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}}},
				},
			},
		},
	})
	create(ctx, t, k8s, &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: s3Namespace, Name: "versitygw"},
		Spec:       corev1.ServiceSpec{Selector: labels, Ports: []corev1.ServicePort{{Name: "s3", Port: 7070, TargetPort: intstr.FromString("s3")}}},
	})
	until(ctx, t, 3*time.Minute, "versitygw is Available", func(ctx context.Context) error {
		deployment := &appsv1.Deployment{}
		if err := k8s.Get(ctx, client.ObjectKey{Namespace: s3Namespace, Name: "versitygw"}, deployment); err != nil {
			return err
		}
		for _, c := range deployment.Status.Conditions {
			if c.Type == appsv1.DeploymentAvailable && c.Status == corev1.ConditionTrue {
				return nil
			}
		}
		return fmt.Errorf("conditions %+v", deployment.Status.Conditions)
	})
	runAWS(ctx, t, k8s, "bucket", "s3api", "create-bucket", "--bucket", s3Bucket)
	keys["region"], keys["s3ForcePathStyle"] = s3Region, "true"
	create(ctx, t, k8s, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: operatorNamespace, Name: s3Secret}, StringData: keys})
}

func rootKey(name, key string) corev1.EnvVar {
	return corev1.EnvVar{Name: name, ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "root"}, Key: key}}}
}

func runAWS(ctx context.Context, t *testing.T, k8s client.Client, name string, args ...string) {
	t.Helper()
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Namespace: s3Namespace, Name: name},
		Spec: batchv1.JobSpec{
			BackoffLimit: new(int32(3)),
			Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
				RestartPolicy: corev1.RestartPolicyNever,
				Containers: []corev1.Container{{
					Name:  "aws",
					Image: awsCLI,
					Args:  append([]string{"--endpoint-url", s3Endpoint}, args...),
					Env: []corev1.EnvVar{
						rootKey("AWS_ACCESS_KEY_ID", "accessKeyID"), rootKey("AWS_SECRET_ACCESS_KEY", "secretAccessKey"),
						{Name: "AWS_DEFAULT_REGION", Value: s3Region},
					},
				}},
			}},
		},
	}
	create(ctx, t, k8s, job)
	until(ctx, t, 3*time.Minute, "the Job "+name+" completed", func(ctx context.Context) error {
		if err := k8s.Get(ctx, client.ObjectKeyFromObject(job), job); err != nil {
			return err
		}
		for _, c := range job.Status.Conditions {
			if c.Type == batchv1.JobComplete && c.Status == corev1.ConditionTrue {
				return nil
			}
		}
		return fmt.Errorf("conditions %+v", job.Status.Conditions)
	})
}

func awsOutput(ctx context.Context, t *testing.T, k8s client.Client, logs kubernetes.Interface, name string, args ...string) []byte {
	t.Helper()
	runAWS(ctx, t, k8s, name, args...)
	pods := &corev1.PodList{}
	if err := k8s.List(ctx, pods, client.InNamespace(s3Namespace), client.MatchingLabels{batchv1.JobNameLabel: name}); err != nil {
		t.Fatal(err)
	}
	for _, pod := range pods.Items {
		if pod.Status.Phase == corev1.PodSucceeded {
			raw, err := logs.CoreV1().Pods(pod.Namespace).GetLogs(pod.Name, &corev1.PodLogOptions{}).DoRaw(ctx)
			if err != nil {
				t.Fatal(err)
			}
			return raw
		}
	}
	t.Fatalf("no pod of Job %s succeeded: %d pods", name, len(pods.Items))
	return nil
}

func random(t *testing.T, n int) string {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}

func writeSourceData(ctx context.Context, t *testing.T, c client.Client) map[string]string {
	t.Helper()
	hashes := map[string]string{}
	for _, ns := range []string{"restore-a", "restore-b"} {
		create(ctx, t, c, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}})
		for i := range 3 {
			secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: fmt.Sprintf("secret-%d", i)}, Data: map[string][]byte{"value": []byte(random(t, 32))}}
			create(ctx, t, c, secret)
			hashes["Secret "+ns+"/"+secret.Name] = lodiav1alpha1.DataHash(secret.Data)
		}
		configMap := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "settings"}, Data: map[string]string{"value": random(t, 16)}}
		create(ctx, t, c, configMap)
		hashes["ConfigMap "+ns+"/"+configMap.Name] = lodiav1alpha1.DataHash(map[string][]byte{"value": []byte(configMap.Data["value"])})
	}
	crd := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apiextensions.k8s.io/v1",
		"kind":       "CustomResourceDefinition",
		"metadata":   map[string]any{"name": "widgets.restore.example.com"},
		"spec": map[string]any{
			"group": "restore.example.com",
			"names": map[string]any{"kind": "Widget", "plural": "widgets", "singular": "widget", "listKind": "WidgetList"},
			"scope": "Namespaced",
			"versions": []any{map[string]any{
				"name": "v1", "served": true, "storage": true,
				"schema": map[string]any{"openAPIV3Schema": map[string]any{"type": "object", "x-kubernetes-preserve-unknown-fields": true}},
			}},
		},
	}}
	create(ctx, t, c, crd)
	until(ctx, t, time.Minute, "the Widget CRD is Established", func(ctx context.Context) error {
		if err := c.Get(ctx, client.ObjectKeyFromObject(crd), crd); err != nil {
			return err
		}
		conditions, _, _ := unstructured.NestedSlice(crd.Object, "status", "conditions")
		for _, condition := range conditions {
			if c, ok := condition.(map[string]any); ok && c["type"] == "Established" && c["status"] == "True" {
				return nil
			}
		}
		return fmt.Errorf("conditions %v", conditions)
	})
	create(ctx, t, c, &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "restore.example.com/v1", "kind": "Widget",
		"metadata": map[string]any{"name": "w", "namespace": "restore-a"},
		"spec":     map[string]any{"size": int64(3)},
	}})
	create(ctx, t, c, &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Namespace: "restore-a", Name: "reader"},
		Rules:      []rbacv1.PolicyRule{{APIGroups: []string{""}, Resources: []string{"configmaps"}, Verbs: []string{"get"}}},
	})
	create(ctx, t, c, &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Namespace: "restore-a", Name: "reader"},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: "reader"},
		Subjects:   []rbacv1.Subject{{APIGroup: rbacv1.GroupName, Kind: rbacv1.UserKind, Name: "restore-reader"}},
	})
	create(ctx, t, c, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "source-node"}})
	create(ctx, t, c, &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Namespace: corev1.NamespaceNodeLease, Name: "source-node"},
		Spec:       coordinationv1.LeaseSpec{HolderIdentity: new("source-node"), LeaseDurationSeconds: new(int32(40))},
	})
	create(ctx, t, c, &certificatesv1.CertificateSigningRequest{
		ObjectMeta: metav1.ObjectMeta{Name: "source-node"},
		Spec: certificatesv1.CertificateSigningRequestSpec{
			Request:    csrFor(t, "system:node:source-node"),
			SignerName: certificatesv1.KubeletServingSignerName,
			Usages:     []certificatesv1.KeyUsage{certificatesv1.UsageDigitalSignature, certificatesv1.UsageServerAuth},
		},
	})
	create(ctx, t, c, &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: "restore-a", Name: "public"},
		Spec:       corev1.ServiceSpec{Type: corev1.ServiceTypeLoadBalancer, Ports: []corev1.ServicePort{{Port: 80}}},
	})
	create(ctx, t, c, &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "cloud-disk"},
		Spec: corev1.PersistentVolumeSpec{
			Capacity:                      corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
			AccessModes:                   []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
			PersistentVolumeSource:        corev1.PersistentVolumeSource{CSI: &corev1.CSIPersistentVolumeSource{Driver: "disk.csi.example.com", VolumeHandle: "vol-source"}},
		},
	})
	create(ctx, t, c, &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{Namespace: "restore-a", Name: "nightly"},
		Spec: batchv1.CronJobSpec{
			Schedule: "0 3 1 1 *",
			JobTemplate: batchv1.JobTemplateSpec{Spec: batchv1.JobSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
				RestartPolicy: corev1.RestartPolicyNever,
				Containers:    []corev1.Container{{Name: "job", Image: agnhost}},
			}}}},
		},
	})
	return hashes
}

func csrFor(t *testing.T, commonName string) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{Subject: pkix.Name{CommonName: commonName, Organization: []string{"system:nodes"}}}, key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: der})
}
