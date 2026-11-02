// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

//go:build e2e && restore

package e2e

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"encoding/xml"
	"fmt"
	"maps"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/distribution/reference"
	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	clientv3 "go.etcd.io/etcd/client/v3"
	appsv1 "k8s.io/api/apps/v1"
	authenticationv1 "k8s.io/api/authentication/v1"
	batchv1 "k8s.io/api/batch/v1"
	certificatesv1 "k8s.io/api/certificates/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	apiserverv1 "k8s.io/apiserver/pkg/apis/apiserver/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	certutil "k8s.io/client-go/util/cert"
	bootstraputil "k8s.io/cluster-bootstrap/token/util"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"
	"sigs.k8s.io/yaml"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

const (
	restoreTenant    = "team-restore"
	restoreClass     = "e2e-restore"
	restoreSource    = "a"
	restoreTarget    = "b"
	s3Namespace      = "lodia-e2e-s3"
	s3Bucket         = "lodia-e2e"
	s3Secret         = "e2e-s3"
	s3Region         = "us-east-1"
	versitygw        = "docker.io/versity/versitygw:v1.10.0@sha256:89cad13cc981986ee284cbafea4613f9533ce6fd5f37a2c96c4c4fb8adb3a316"
	awsCLI           = "docker.io/amazon/aws-cli:2.39.3@sha256:d557b3524475d70dd3104cf44d96568ee3188897b73a6ece2b37aa50e1af362f"
	conformanceImage = "registry.k8s.io/conformance:v1.37.0@sha256:ac2957464746344fe3e44c4ef24bc03e4161c3abb86213cdf62ae3b5cd48946c"
	busyboxImage     = "registry.k8s.io/e2e-test-images/busybox:1.36.1-1@sha256:a9155b13325b2abef48e71de77bb8ac015412a566829f621d06bfae5c699b1b9"
)

var s3Endpoint = "http://versitygw." + s3Namespace + ".svc:7070"

var smokeSubset = []string{
	`\QWatchers should be able to restart watching from the last resource version observed by the previous watch\E.*\[Conformance\]`,
	`\QWatchers should observe add, update, and delete watch notifications on configmaps\E.*\[Conformance\]`,
	`\QGarbage collector should delete pods created by rc when not orphaning\E.*\[Conformance\]`,
	`\QSimple CustomResourceDefinition creating/deleting custom resource definition objects works\E.*\[Conformance\]`,
	`\QServiceAccounts should mount projected service account token\E.*\[Conformance\]`,
	`\QServiceAccounts should guarantee kube-root-ca.crt exist in any namespace\E.*\[Conformance\]`,
	`\QSubjectReview should support SubjectReview API operations\E.*\[Conformance\]`,
	`\QSecrets should be consumable from pods in env vars\E.*\[Conformance\]`,
	`\QProjected secret should be consumable from pods in volume [Conformance]\E`,
	`\QConfigMap should be consumable via environment variable\E.*\[Conformance\]`,
	`\QPods should be submitted and removed\E.*\[Conformance\]`,
	`\QPods should support retrieving logs from the container over websockets\E.*\[Conformance\]`,
	`\QPods should support remote command execution over websockets\E.*\[Conformance\]`,
	`\QDNS should provide DNS for the cluster\E.*\[Conformance\]`,
	`\QServices should serve a basic endpoint from pods\E.*\[Conformance\]`,
	`\QDeployment RollingUpdateDeployment should delete old pods and create new ones\E.*\[Conformance\]`,
}

func TestRestoreGate(t *testing.T) {
	var (
		k8s                 client.Client
		access              *tenantAccess
		restoredAccess      *tenantAccess
		written             map[string]client.Object
		hashes              map[string]string
		token               string
		serviceAccountToken string
		sourceCA            []*x509.Certificate
		sourceClientCA      []*x509.Certificate
		sourceUID           types.UID
		backup              *lodiav1alpha1.Backup
	)
	key := client.ObjectKey{Namespace: restoreTenant, Name: restoreSource}
	restoredKey := client.ObjectKey{Namespace: restoreTenant, Name: restoreTarget}
	f := features.New("gate G3 on kind: a Backup sealed on etcd-druid and an S3 store, and a ControlPlane restored from it that replaces its source").
		Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			conformance, err := reference.ParseNormalizedNamed(conformanceImage)
			tagged, ok := conformance.(reference.Tagged)
			if err != nil || !ok || tagged.Tag() != kindNodeVersion(t) {
				t.Fatalf("%s is not of %s, the version of kind's node image: check the smoke subset against that version's conformance.yaml and pin its conformance image", conformanceImage, kindNodeVersion(t))
			}
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
				Spec:       restoreSpec(t, nil),
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
			written = writeSourceData(ctx, t, access.client)
			token = bootstrapToken(ctx, t, access.client)
			hashes = map[string]string{}
			for _, name := range slices.Sorted(maps.Keys(written)) {
				hashes[name] = hashOf(written[name])
				t.Logf("%s: %s", name, hashes[name])
			}
			sourceCA = certificatesIn(ctx, t, k8s, key.Namespace, lodiav1alpha1.CABundleSecret.Name(key.Name))
			sourceClientCA = certificatesIn(ctx, t, k8s, key.Namespace, lodiav1alpha1.ClientCASecret.Name(key.Name))
			until(ctx, t, time.Minute, "the ServiceAccount restore-a/default exists", func(ctx context.Context) error {
				return access.client.Get(ctx, client.ObjectKey{Namespace: "restore-a", Name: "default"}, &corev1.ServiceAccount{})
			})
			request, err := kubernetes.NewForConfigOrDie(access.cfg).CoreV1().ServiceAccounts("restore-a").CreateToken(ctx, "default", &authenticationv1.TokenRequest{}, metav1.CreateOptions{})
			if err != nil {
				t.Fatal(err)
			}
			serviceAccountToken = request.Status.Token
			return ctx
		}).
		Assess("3. a Backup of the source is sealed and copied", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			backup = &lodiav1alpha1.Backup{
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
			sourceUID = source.UID
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
			until(ctx, t, time.Minute, "the Backup carries its UID in "+lodiav1alpha1.SealedUIDAnnotation+" and the finalizer "+lodiav1alpha1.RestoreSourceFinalizer, func(ctx context.Context) error {
				if err := k8s.Get(ctx, client.ObjectKeyFromObject(backup), backup); err != nil {
					return err
				}
				if mark := backup.Annotations[lodiav1alpha1.SealedUIDAnnotation]; mark != string(backup.UID) || !controllerutil.ContainsFinalizer(backup, lodiav1alpha1.RestoreSourceFinalizer) {
					return fmt.Errorf("%s is %q, finalizers %v", lodiav1alpha1.SealedUIDAnnotation, mark, backup.Finalizers)
				}
				return nil
			})
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
		Assess("4. the source changes after the Backup", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			create(ctx, t, access.client, &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Namespace: "restore-a", Name: "after-backup"},
				Data:       map[string]string{"value": random(t, 16)},
			})
			return ctx
		}).
		Assess("5. b, restored from the Backup, serves with its controller manager held", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			create(ctx, t, k8s, &lodiav1alpha1.ControlPlane{
				ObjectMeta: metav1.ObjectMeta{Name: restoredKey.Name, Namespace: restoredKey.Namespace},
				Spec:       restoreSpec(t, &lodiav1alpha1.RestoreFrom{BackupRef: lodiav1alpha1.LocalReference{Name: backup.Name}}),
			})
			until(ctx, t, 5*time.Minute, "b's DatastoreReady=True, its status.restore naming the Backup and its source", func(ctx context.Context) error {
				cp, _, err := conditionOf(ctx, k8s, restoredKey, lodiav1alpha1.ConditionDatastoreReady, metav1.ConditionTrue, lodiav1alpha1.ReasonReady)
				if err != nil {
					return err
				}
				return restoredFrom(cp, backup, sourceUID)
			})
			until(ctx, t, 5*time.Minute, "b's APIServerReady=True, its controller manager held", func(ctx context.Context) error {
				_, c, err := conditionOf(ctx, k8s, restoredKey, lodiav1alpha1.ConditionAPIServerReady, metav1.ConditionTrue, lodiav1alpha1.ReasonReady)
				if err != nil {
					return err
				}
				if held := "kube-controller-manager is held at 0 replicas until the restore is released: spec.restoreFrom.release is Held"; !strings.Contains(c.Message, held) {
					return fmt.Errorf("APIServerReady's message lacks %q: %s", held, c.Message)
				}
				return nil
			})
			if err := controllerManagerAt(ctx, k8s, restoredKey, 0); err != nil {
				t.Error(err)
			}
			until(ctx, t, 3*time.Minute, "b's AddonsApplied=True", func(ctx context.Context) error {
				_, _, err := conditionOf(ctx, k8s, restoredKey, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionTrue, lodiav1alpha1.ReasonApplied)
				return err
			})
			until(ctx, t, 3*time.Minute, "b's ControllersLeading=False, AwaitingRelease, naming the LoadBalancer Service, the PersistentVolume and the CronJob", func(ctx context.Context) error {
				_, c, err := conditionOf(ctx, k8s, restoredKey, lodiav1alpha1.ConditionControllersLeading, metav1.ConditionFalse, lodiav1alpha1.ReasonAwaitingRelease)
				if err != nil {
					return err
				}
				for _, name := range []string{"restore-a/public", "cloud-disk", "restore-a/nightly"} {
					if !strings.Contains(c.Message, name) {
						return fmt.Errorf("ControllersLeading's message names no %s: %s", name, c.Message)
					}
				}
				return nil
			})
			cp := &lodiav1alpha1.ControlPlane{}
			if err := k8s.Get(ctx, restoredKey, cp); err != nil {
				t.Fatal(err)
			}
			if c := meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionReady); c == nil || c.Status != metav1.ConditionFalse {
				t.Errorf("b's Ready is %+v, want False", c)
			}
			return ctx
		}).
		Assess("6. b holds the source's data as the Backup sealed it, and nothing that bound it to the source's world", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			restoredAccess = openTenant(ctx, t, k8s, restoredKey)
			for _, name := range slices.Sorted(maps.Keys(written)) {
				if hash, err := readHash(ctx, restoredAccess.client, written[name]); err != nil || hash != hashes[name] {
					t.Errorf("%s in b hashes to %q (%v), in a to %s", name, hash, err, hashes[name])
				}
			}
			for _, object := range []struct{ apiVersion, kind, name string }{
				{"restore.example.com/v1", "Widget", "w"},
				{rbacv1.SchemeGroupVersion.String(), "Role", "reader"},
				{rbacv1.SchemeGroupVersion.String(), "RoleBinding", "reader"},
			} {
				until(ctx, t, time.Minute, fmt.Sprintf("b serves %s restore-a/%s as a does, with its UID", object.kind, object.name), func(ctx context.Context) error {
					var read [2]*unstructured.Unstructured
					for i, c := range []client.Client{access.client, restoredAccess.client} {
						read[i] = &unstructured.Unstructured{}
						read[i].SetAPIVersion(object.apiVersion)
						read[i].SetKind(object.kind)
						if err := c.Get(ctx, client.ObjectKey{Namespace: "restore-a", Name: object.name}, read[i]); err != nil {
							return err
						}
					}
					if read[0].GetUID() != read[1].GetUID() {
						return fmt.Errorf("UID %s in a, %s in b", read[0].GetUID(), read[1].GetUID())
					}
					delete(read[0].Object, "metadata")
					delete(read[1].Object, "metadata")
					if !reflect.DeepEqual(read[0].Object, read[1].Object) {
						return fmt.Errorf("%v in a, %v in b", read[0].Object, read[1].Object)
					}
					return nil
				})
			}
			id, _, _ := strings.Cut(token, ".")
			for _, object := range []struct {
				what string
				obj  client.Object
			}{
				{"ConfigMap restore-a/after-backup", &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "restore-a", Name: "after-backup"}}},
				{"Node source-node", &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "source-node"}}},
				{"Lease kube-node-lease/source-node", &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Namespace: corev1.NamespaceNodeLease, Name: "source-node"}}},
				{"CertificateSigningRequest source-node", &certificatesv1.CertificateSigningRequest{ObjectMeta: metav1.ObjectMeta{Name: "source-node"}}},
				{"the bootstrap token Secret", &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: metav1.NamespaceSystem, Name: bootstraputil.BootstrapTokenSecretName(id)}}},
			} {
				name := client.ObjectKeyFromObject(object.obj)
				if err := access.client.Get(ctx, name, object.obj); err != nil {
					t.Errorf("%s in a: %v", object.what, err)
				}
				if err := gone(restoredAccess.client.Get(ctx, name, object.obj)); err != nil {
					t.Errorf("%s in b: %v", object.what, err)
				}
			}
			restoredClientCA := certificatesIn(ctx, t, k8s, restoredKey.Namespace, lodiav1alpha1.ClientCASecret.Name(restoredKey.Name))
			until(ctx, t, time.Minute, "b's extension-apiserver-authentication holds b's client CA, not a's", func(ctx context.Context) error {
				authentication := &corev1.ConfigMap{}
				if err := restoredAccess.client.Get(ctx, client.ObjectKey{Namespace: metav1.NamespaceSystem, Name: "extension-apiserver-authentication"}, authentication); err != nil {
					return err
				}
				held, err := certutil.ParseCertsPEM([]byte(authentication.Data["client-ca-file"]))
				if err != nil {
					return err
				}
				if !holds(held, restoredClientCA, sourceClientCA) {
					return fmt.Errorf("its client-ca-file holds %d certificates, not b's client CA alone", len(held))
				}
				return nil
			})
			withCertificate := func(server, holder *tenantAccess) *rest.Config {
				cfg := rest.AnonymousClientConfig(server.cfg)
				cfg.CertData, cfg.KeyData = holder.cfg.CertData, holder.cfg.KeyData
				return cfg
			}
			withToken := func(server *tenantAccess) *rest.Config {
				cfg := rest.AnonymousClientConfig(server.cfg)
				cfg.BearerToken = token
				return cfg
			}
			for _, request := range []struct {
				what         string
				cfg          *rest.Config
				unauthorized bool
			}{
				{"a's admin certificate on b", withCertificate(restoredAccess, access), true},
				{"b's admin certificate on a", withCertificate(access, restoredAccess), true},
				{"a's bootstrap token on a", withToken(access), false},
				{"a's bootstrap token on b", withToken(restoredAccess), true},
			} {
				httpClient, err := rest.HTTPClientFor(request.cfg)
				if err != nil {
					t.Fatal(err)
				}
				if status, _ := httpGet(ctx, t, httpClient, request.cfg.Host+"/api/v1/namespaces"); (status == http.StatusUnauthorized) != request.unauthorized {
					t.Errorf("%s: GET /api/v1/namespaces answers %d", request.what, status)
				}
			}
			var signingKeys [][]byte
			for _, k := range []client.ObjectKey{key, restoredKey} {
				keys := &corev1.Secret{}
				if err := k8s.Get(ctx, client.ObjectKey{Namespace: k.Namespace, Name: lodiav1alpha1.ServiceAccountKeysSecret.Name(k.Name)}, keys); err != nil {
					t.Fatal(err)
				}
				signingKeys = append(signingKeys, keys.Data[lodiav1alpha1.SigningKeyKey])
			}
			if len(signingKeys[0]) == 0 || bytes.Equal(signingKeys[0], signingKeys[1]) {
				t.Errorf("a and b sign ServiceAccount tokens with the same key, or a has none")
			}
			for _, reviewer := range []struct {
				what          string
				c             client.Client
				authenticated bool
			}{{"a", access.client, true}, {"b", restoredAccess.client, false}} {
				review := &authenticationv1.TokenReview{Spec: authenticationv1.TokenReviewSpec{Token: serviceAccountToken}}
				create(ctx, t, reviewer.c, review)
				if review.Status.Authenticated != reviewer.authenticated {
					t.Errorf("a TokenReview on %s of a's ServiceAccount token: authenticated %t, error %q", reviewer.what, review.Status.Authenticated, review.Status.Error)
				}
			}
			providers := encryptionProviders(ctx, t, k8s, restoredKey.Namespace, lodiav1alpha1.EncryptionConfigSecret.Name(restoredKey.Name))
			if len(providers) == 0 || providers[0].Secretbox == nil || len(providers[0].Secretbox.Keys) == 0 {
				t.Fatalf("the first provider of %s is not a secretbox key, spec's default: %+v", lodiav1alpha1.EncryptionConfigSecret.Name(restoredKey.Name), providers)
			}
			first := providers[0].Secretbox.Keys[0].Name
			var sealed []string
			for _, p := range encryptionProviders(ctx, t, k8s, key.Namespace, lodiav1alpha1.BackupEncryptionConfigSecretName(backup.Name)) {
				if p.Secretbox != nil {
					for _, k := range p.Secretbox.Keys {
						sealed = append(sealed, k.Name)
					}
				}
			}
			if len(sealed) == 0 || slices.Contains(sealed, first) {
				t.Errorf("b's first key %s, a-1's keys %v: want a key a-1 lacks", first, sealed)
			}
			secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "restore-a", Name: "written-in-b"}, Data: map[string][]byte{"value": []byte(random(t, 32))}}
			create(ctx, t, restoredAccess.client, secret)
			raw := etcdValue(ctx, t, cfg, k8s, restoredKey, "/registry/secrets/restore-a/written-in-b")
			if prefix := "k8s:enc:secretbox:v1:" + first + ":"; !bytes.HasPrefix(raw, []byte(prefix)) {
				t.Errorf("Secret restore-a/written-in-b is stored in b's etcd as %q…, want the prefix %q", raw[:min(len(raw), len(prefix))], prefix)
			}
			return ctx
		}).
		Assess("7. b, released, waits while the source exists", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			cp := &lodiav1alpha1.ControlPlane{}
			if err := k8s.Get(ctx, restoredKey, cp); err != nil {
				t.Fatal(err)
			}
			patch := client.MergeFrom(cp.DeepCopy())
			cp.Spec.RestoreFrom.Release = lodiav1alpha1.RestoreReleased
			if err := k8s.Patch(ctx, cp, patch); err != nil {
				t.Fatal(err)
			}
			until(ctx, t, 3*time.Minute, "b's ControllersLeading=False, SourceExists", func(ctx context.Context) error {
				_, _, err := conditionOf(ctx, k8s, restoredKey, lodiav1alpha1.ConditionControllersLeading, metav1.ConditionFalse, lodiav1alpha1.ReasonSourceExists)
				return err
			})
			until(ctx, t, 3*time.Minute, "b's APIServerReady=True, held while the source exists", func(ctx context.Context) error {
				_, c, err := conditionOf(ctx, k8s, restoredKey, lodiav1alpha1.ConditionAPIServerReady, metav1.ConditionTrue, lodiav1alpha1.ReasonReady)
				if err != nil {
					return err
				}
				if !strings.HasSuffix(c.Message, fmt.Sprintf("until the restore is released: ControlPlane %s (UID %s), the Backup's source, exists", restoreSource, sourceUID)) {
					return fmt.Errorf("APIServerReady's message: %s", c.Message)
				}
				return nil
			})
			if err := controllerManagerAt(ctx, k8s, restoredKey, 0); err != nil {
				t.Error(err)
			}
			return ctx
		}).
		Assess("8. the source deleted, b runs its controller manager, turns Ready and publishes its own CA", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			if err := k8s.Delete(ctx, &lodiav1alpha1.ControlPlane{ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace}}); err != nil {
				t.Fatal(err)
			}
			until(ctx, t, 3*time.Minute, "a is gone", func(ctx context.Context) error {
				return gone(k8s.Get(ctx, key, &lodiav1alpha1.ControlPlane{}))
			})
			until(ctx, t, 3*time.Minute, "b's controller manager asks for 1 replica", func(ctx context.Context) error {
				return controllerManagerAt(ctx, k8s, restoredKey, 1)
			})
			until(ctx, t, 3*time.Minute, "b's ControllersLeading=True", func(ctx context.Context) error {
				_, _, err := conditionOf(ctx, k8s, restoredKey, lodiav1alpha1.ConditionControllersLeading, metav1.ConditionTrue, lodiav1alpha1.ReasonLeading)
				return err
			})
			until(ctx, t, 3*time.Minute, "b's Ready=True", func(ctx context.Context) error {
				return controlPlaneReady(ctx, k8s, restoredKey, &lodiav1alpha1.ControlPlane{})
			})
			restoredCA := certificatesIn(ctx, t, k8s, restoredKey.Namespace, lodiav1alpha1.CABundleSecret.Name(restoredKey.Name))
			until(ctx, t, 2*time.Minute, "b's restore-a/kube-root-ca.crt holds b's CA, not a's", func(ctx context.Context) error {
				published := &corev1.ConfigMap{}
				if err := restoredAccess.client.Get(ctx, client.ObjectKey{Namespace: "restore-a", Name: "kube-root-ca.crt"}, published); err != nil {
					return err
				}
				held, err := certutil.ParseCertsPEM([]byte(published.Data["ca.crt"]))
				if err != nil {
					return err
				}
				if !holds(held, restoredCA, sourceCA) {
					return fmt.Errorf("its ca.crt holds %d certificates, not b's CA alone", len(held))
				}
				return nil
			})
			return ctx
		}).
		Assess("9. a node joins b, and the conformance smoke subset passes in it", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			joinNodes(ctx, t, cfg, k8s, restoredKey, restoredAccess, 1)
			until(ctx, t, 3*time.Minute, "CoreDNS is available in b", func(ctx context.Context) error {
				return coreDNSAvailable(ctx, restoredAccess.client)
			})
			results := t.ArtifactDir()
			running, cancel := context.WithTimeout(ctx, 15*time.Minute)
			defer cancel()
			runHydrophone(running, t, restoredAccess, results, "--conformance-image", conformanceImage, "--busybox-image", busyboxImage,
				"--focus", strings.Join(smokeSubset, "|"), "--extra-ginkgo-args=--fail-on-empty=true")
			passed := passedSpecs(t, filepath.Join(results, "junit_01.xml"))
			for _, pattern := range smokeSubset {
				matching := regexp.MustCompile(pattern)
				n := 0
				for _, spec := range passed {
					if matching.MatchString(spec) {
						n++
					}
				}
				if n != 1 {
					t.Errorf("%d passed specs match %s, want 1", n, pattern)
				}
			}
			if len(passed) != len(smokeSubset) {
				t.Errorf("junit_01.xml holds %d passed specs, want %d: %s", len(passed), len(smokeSubset), strings.Join(passed, "; "))
			}
			return ctx
		}).
		Assess("10. a, created again from the Backup over the claims the class kept, waits at VolumesExist until they are deleted, and restores the Backup deleted meanwhile", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			claims := &corev1.PersistentVolumeClaimList{}
			if err := k8s.List(ctx, claims, client.InNamespace(key.Namespace), client.MatchingLabels{lodiav1alpha1.ControlPlaneUIDLabel: string(sourceUID)}); err != nil {
				t.Fatal(err)
			}
			if len(claims.Items) == 0 {
				t.Fatalf("the class's Retain kept no claim of a, UID %s", sourceUID)
			}
			create(ctx, t, k8s, &lodiav1alpha1.ControlPlane{
				ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace},
				Spec:       restoreSpec(t, &lodiav1alpha1.RestoreFrom{BackupRef: lodiav1alpha1.LocalReference{Name: backup.Name}, Release: lodiav1alpha1.RestoreReleased}),
			})
			until(ctx, t, 5*time.Minute, "the new a's DatastoreReady=False, VolumesExist, naming the old a's claims", func(ctx context.Context) error {
				_, c, err := conditionOf(ctx, k8s, key, lodiav1alpha1.ConditionDatastoreReady, metav1.ConditionFalse, lodiav1alpha1.ReasonVolumesExist)
				if err != nil {
					return err
				}
				for _, claim := range claims.Items {
					if !strings.Contains(c.Message, claim.Name) {
						return fmt.Errorf("DatastoreReady's message names no %s: %s", claim.Name, c.Message)
					}
				}
				return nil
			})
			if err := k8s.Delete(ctx, &lodiav1alpha1.Backup{ObjectMeta: metav1.ObjectMeta{Name: backup.Name, Namespace: backup.Namespace}}); err != nil {
				t.Fatal(err)
			}
			for i := range claims.Items {
				if err := k8s.Delete(ctx, &claims.Items[i]); err != nil {
					t.Fatal(err)
				}
			}
			until(ctx, t, 5*time.Minute, "the new a creates its Etcd", func(ctx context.Context) error {
				cp := &lodiav1alpha1.ControlPlane{}
				if err := k8s.Get(ctx, key, cp); err != nil {
					return err
				}
				etcd := &druidv1alpha1.Etcd{}
				if err := k8s.Get(ctx, client.ObjectKey{Namespace: key.Namespace, Name: lodiav1alpha1.EtcdName(key.Name)}, etcd); err != nil {
					return err
				}
				if !metav1.IsControlledBy(etcd, cp) {
					return fmt.Errorf("Etcd %s is controlled by %+v", etcd.Name, metav1.GetControllerOf(etcd))
				}
				return nil
			})
			return ctx
		}).
		Assess("11. the Backup, deleted before the new a had an Etcd, goes once both restores of it are complete, and leaves only its copy in the bucket", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			var early []string
			until(ctx, t, 5*time.Minute, "Backup "+backup.Name+" is gone", func(ctx context.Context) error {
				left := &lodiav1alpha1.Backup{}
				err := k8s.Get(ctx, client.ObjectKeyFromObject(backup), left)
				if err == nil {
					return fmt.Errorf("it stays, being deleted %t, with the finalizers %v", !left.DeletionTimestamp.IsZero(), left.Finalizers)
				}
				if !apierrors.IsNotFound(err) {
					return err
				}
				early = nil
				for _, restoring := range []client.ObjectKey{restoredKey, key} {
					cp := &lodiav1alpha1.ControlPlane{}
					if err := k8s.Get(ctx, restoring, cp); err != nil {
						return err
					}
					if err := restoredFrom(cp, backup, sourceUID); err != nil {
						early = append(early, err.Error())
					}
				}
				return nil
			})
			if len(early) > 0 {
				t.Errorf("Backup %s went before every restore of it completed: %s", backup.Name, strings.Join(early, "; "))
			}
			until(ctx, t, 5*time.Minute, "the new a's DatastoreReady=True", func(ctx context.Context) error {
				_, _, err := conditionOf(ctx, k8s, key, lodiav1alpha1.ConditionDatastoreReady, metav1.ConditionTrue, lodiav1alpha1.ReasonReady)
				return err
			})
			until(ctx, t, 5*time.Minute, "the new a's APIServerReady=True", func(ctx context.Context) error {
				_, _, err := conditionOf(ctx, k8s, key, lodiav1alpha1.ConditionAPIServerReady, metav1.ConditionTrue, lodiav1alpha1.ReasonReady)
				return err
			})
			revived := openTenant(ctx, t, k8s, key)
			sample := "Secret restore-a/secret-0"
			until(ctx, t, 2*time.Minute, "the new a serves "+sample+" as the Backup sealed it", func(ctx context.Context) error {
				hash, err := readHash(ctx, revived.client, written[sample])
				if err != nil {
					return err
				}
				if hash != hashes[sample] {
					return fmt.Errorf("it hashes to %s, in the Backup to %s", hash, hashes[sample])
				}
				return nil
			})
			var restoring []string
			for _, k := range []client.ObjectKey{restoredKey, key} {
				cp := &lodiav1alpha1.ControlPlane{}
				if err := k8s.Get(ctx, k, cp); err != nil {
					t.Fatal(err)
				}
				restoring = append(restoring, "restore-"+string(cp.UID))
			}
			until(ctx, t, 2*time.Minute, "nothing of Backup "+backup.Name+" is left in the cluster", func(ctx context.Context) error {
				objects := []client.Object{&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: lodiav1alpha1.BackupEncryptionConfigSecretName(backup.Name)}}}
				for _, name := range restoring {
					objects = append(objects, &druidv1alpha1.EtcdCopyBackupsTask{ObjectMeta: metav1.ObjectMeta{Name: name}}, &druidv1alpha1.EtcdOpsTask{ObjectMeta: metav1.ObjectMeta{Name: name}})
				}
				for _, obj := range objects {
					if err := gone(k8s.Get(ctx, client.ObjectKey{Namespace: key.Namespace, Name: obj.GetName()}, obj)); err != nil {
						return fmt.Errorf("%T %s: %w", obj, obj.GetName(), err)
					}
				}
				return nil
			})
			if backup.Status.Snapshots == nil {
				t.Fatalf("Backup %s recorded no status.snapshots", backup.Name)
			}
			prefix := backup.Status.Snapshots.Prefix + "/"
			logs := kubernetes.NewForConfigOrDie(cfg.Client().RESTConfig())
			left := strings.Fields(string(awsOutput(ctx, t, k8s, logs, "left-"+backup.Name, "s3api", "list-objects-v2", "--bucket", s3Bucket, "--prefix", prefix, "--query", "Contents[].Key", "--output", "text")))
			if !slices.ContainsFunc(left, func(object string) bool { return strings.HasPrefix(object, prefix) }) {
				t.Errorf("the bucket holds nothing under %s: %v", prefix, left)
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

func restoreSpec(t *testing.T, from *lodiav1alpha1.RestoreFrom) lodiav1alpha1.ControlPlaneSpec {
	t.Helper()
	return lodiav1alpha1.ControlPlaneSpec{
		Version:     kindNodeVersion(t),
		Datastore:   lodiav1alpha1.DatastoreChoice{Dedicated: &lodiav1alpha1.DedicatedDatastore{ClassName: restoreClass}},
		Endpoint:    lodiav1alpha1.Endpoint{NodePort: &lodiav1alpha1.NodePortEndpoint{}},
		Network:     lodiav1alpha1.Network{ServiceCIDRs: []lodiav1alpha1.CIDR{"10.32.0.0/16"}, PodCIDRs: []lodiav1alpha1.CIDR{gatePodCIDR}},
		Addons:      &lodiav1alpha1.Addons{KubeProxy: &lodiav1alpha1.KubeProxy{Conntrack: &lodiav1alpha1.KubeProxyConntrack{MaxPerCore: new(int32(0))}}},
		RestoreFrom: from,
	}
}

func conditionOf(ctx context.Context, k8s client.Client, key client.ObjectKey, conditionType string, status metav1.ConditionStatus, reason string) (*lodiav1alpha1.ControlPlane, *metav1.Condition, error) {
	cp := &lodiav1alpha1.ControlPlane{}
	if err := k8s.Get(ctx, key, cp); err != nil {
		return nil, nil, err
	}
	c := meta.FindStatusCondition(cp.Status.Conditions, conditionType)
	if c == nil || c.Status != status || c.Reason != reason || c.ObservedGeneration != cp.Generation {
		return nil, nil, fmt.Errorf("%s of %s is %+v at generation %d", conditionType, key.Name, c, cp.Generation)
	}
	return cp, c, nil
}

func restoredFrom(cp *lodiav1alpha1.ControlPlane, backup *lodiav1alpha1.Backup, source types.UID) error {
	restore := cp.Status.Restore
	if restore == nil || restore.Backup.Name != backup.Name || restore.Backup.UID != backup.UID || restore.Source.Name != restoreSource || restore.Source.UID != source || restore.CompletedAt.IsZero() {
		return fmt.Errorf("status.restore of %s is %+v, want Backup %s (UID %s) of ControlPlane %s (UID %s)", cp.Name, restore, backup.Name, backup.UID, restoreSource, source)
	}
	return nil
}

func controllerManagerAt(ctx context.Context, k8s client.Client, key client.ObjectKey, replicas int32) error {
	d := &appsv1.Deployment{}
	if err := k8s.Get(ctx, client.ObjectKey{Namespace: key.Namespace, Name: key.Name + "-kube-controller-manager"}, d); err != nil {
		return err
	}
	if asked := ptr.Deref(d.Spec.Replicas, 1); asked != replicas {
		return fmt.Errorf("Deployment %s asks for %d replicas, want %d", d.Name, asked, replicas)
	}
	return nil
}

func hashOf(obj client.Object) string {
	switch o := obj.(type) {
	case *corev1.Secret:
		return lodiav1alpha1.DataHash(o.Data)
	case *corev1.ConfigMap:
		data := make(map[string][]byte, len(o.Data))
		for k, v := range o.Data {
			data[k] = []byte(v)
		}
		return lodiav1alpha1.DataHash(data)
	default:
		return ""
	}
}

func readHash(ctx context.Context, c client.Client, written client.Object) (string, error) {
	var read client.Object = &corev1.ConfigMap{}
	if _, ok := written.(*corev1.Secret); ok {
		read = &corev1.Secret{}
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(written), read); err != nil {
		return "", err
	}
	return hashOf(read), nil
}

func certificatesIn(ctx context.Context, t *testing.T, k8s client.Client, namespace, name string) []*x509.Certificate {
	t.Helper()
	secret := &corev1.Secret{}
	if err := k8s.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, secret); err != nil {
		t.Fatal(err)
	}
	certificates, err := certutil.ParseCertsPEM(secret.Data[lodiav1alpha1.CACertKey])
	if err != nil {
		t.Fatalf("Secret %s: %v", name, err)
	}
	return certificates
}

func holds(bundle, these, notThose []*x509.Certificate) bool {
	for _, c := range these {
		if !slices.ContainsFunc(bundle, c.Equal) {
			return false
		}
	}
	for _, c := range notThose {
		if slices.ContainsFunc(bundle, c.Equal) {
			return false
		}
	}
	return true
}

func encryptionProviders(ctx context.Context, t *testing.T, k8s client.Client, namespace, name string) []apiserverv1.ProviderConfiguration {
	t.Helper()
	secret := &corev1.Secret{}
	if err := k8s.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, secret); err != nil {
		t.Fatal(err)
	}
	config := &apiserverv1.EncryptionConfiguration{}
	if err := yaml.Unmarshal(secret.Data[lodiav1alpha1.EncryptionConfigKey], config); err != nil || len(config.Resources) != 1 {
		t.Fatalf("Secret %s holds no encryption configuration of one resource: %v", name, err)
	}
	return config.Resources[0].Providers
}

func etcdValue(ctx context.Context, t *testing.T, cfg *envconf.Config, k8s client.Client, key client.ObjectKey, etcdKey string) []byte {
	t.Helper()
	etcd := &druidv1alpha1.Etcd{}
	if err := k8s.Get(ctx, client.ObjectKey{Namespace: key.Namespace, Name: lodiav1alpha1.EtcdName(key.Name)}, etcd); err != nil {
		t.Fatal(err)
	}
	refs := etcd.Spec.Etcd.ClientUrlTLS
	if refs == nil {
		t.Fatalf("Etcd %s has no client TLS", etcd.Name)
	}
	read := func(name string) *corev1.Secret {
		secret := &corev1.Secret{}
		if err := k8s.Get(ctx, client.ObjectKey{Namespace: key.Namespace, Name: name}, secret); err != nil {
			t.Fatal(err)
		}
		return secret
	}
	leaf := read(refs.ClientTLSSecretRef.Name)
	certificate, err := tls.X509KeyPair(leaf.Data[corev1.TLSCertKey], leaf.Data[corev1.TLSPrivateKeyKey])
	if err != nil {
		t.Fatalf("Secret %s: %v", leaf.Name, err)
	}
	roots := x509.NewCertPool()
	if bundle := read(refs.TLSCASecretRef.Name); !roots.AppendCertsFromPEM(bundle.Data[ptr.Deref(refs.TLSCASecretRef.DataKey, lodiav1alpha1.CACertKey)]) {
		t.Fatalf("Secret %s holds no CA certificate", bundle.Name)
	}
	member := &corev1.Pod{}
	if err := k8s.Get(ctx, client.ObjectKey{Namespace: key.Namespace, Name: etcd.Name + "-0"}, member); err != nil {
		t.Fatal(err)
	}
	local, stop := forward(t, cfg.Client().RESTConfig(), member, druidv1alpha1.GetClientPort(etcd))
	defer stop()
	etcdClient, err := clientv3.New(clientv3.Config{
		Context:     ctx,
		Endpoints:   []string{"https://" + local},
		DialTimeout: 10 * time.Second,
		TLS:         &tls.Config{Certificates: []tls.Certificate{certificate}, RootCAs: roots, MinVersion: tls.VersionTLS12},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = etcdClient.Close() }()
	reading, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	got, err := etcdClient.Get(reading, etcdKey)
	if err != nil {
		t.Fatalf("read %s from Etcd %s: %v", etcdKey, etcd.Name, err)
	}
	if len(got.Kvs) != 1 {
		t.Fatalf("Etcd %s holds %d values under %s", etcd.Name, len(got.Kvs), etcdKey)
	}
	return got.Kvs[0].GetValue()
}

func passedSpecs(t *testing.T, file string) []string {
	t.Helper()
	raw, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	var report struct {
		Suites []struct {
			Cases []struct {
				Name   string `xml:"name,attr"`
				Status string `xml:"status,attr"`
			} `xml:"testcase"`
		} `xml:"testsuite"`
	}
	if err := xml.Unmarshal(raw, &report); err != nil {
		t.Fatalf("%s: %v", file, err)
	}
	var passed []string
	for _, suite := range report.Suites {
		for _, c := range suite.Cases {
			if spec, ok := strings.CutPrefix(c.Name, "[It] "); ok && c.Status == "passed" {
				passed = append(passed, spec)
			}
		}
	}
	return passed
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

func writeSourceData(ctx context.Context, t *testing.T, c client.Client) map[string]client.Object {
	t.Helper()
	written := map[string]client.Object{}
	for _, ns := range []string{"restore-a", "restore-b"} {
		create(ctx, t, c, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}})
		for i := range 3 {
			secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: fmt.Sprintf("secret-%d", i)}, Data: map[string][]byte{"value": []byte(random(t, 32))}}
			create(ctx, t, c, secret)
			written["Secret "+ns+"/"+secret.Name] = secret
		}
		configMap := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "settings"}, Data: map[string]string{"value": random(t, 16)}}
		create(ctx, t, c, configMap)
		written["ConfigMap "+ns+"/"+configMap.Name] = configMap
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
	return written
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
