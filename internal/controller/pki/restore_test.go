// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package pki_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	apiserverv1 "k8s.io/apiserver/pkg/apis/apiserver/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/yaml"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/operator"
)

const backupName = "a-1"

var backupKeys = lodiav1alpha1.BackupEncryptionConfigSecretName(backupName)

func restoring(spec *lodiav1alpha1.ControlPlaneSpec) {
	spec.RestoreFrom = &lodiav1alpha1.RestoreFrom{BackupRef: lodiav1alpha1.LocalReference{Name: backupName}}
}

func TestARestoreAdoptsNoSecret(t *testing.T) {
	other := types.UID("0b6c1c2e-5d0a-4a43-9a1e-2f1a7d3c9e10")
	configMap := metav1.OwnerReference{APIVersion: "v1", Kind: "ConfigMap", Name: "restore", UID: other}
	for i, c := range []struct {
		name     string
		labelled bool
		plant    func(cp *lodiav1alpha1.ControlPlane) map[string][]metav1.OwnerReference
		reason   string
		requeue  time.Duration
	}{
		{"an ownerless CA bundle", true, ownerless("dev-ca-bundle"), lodiav1alpha1.ReasonSecretsExist, time.Minute},
		{"an ownerless encryption configuration", false, ownerless("dev-encryption-config"), lodiav1alpha1.ReasonSecretsExist, time.Minute},
		{"an ownerless CA key", false, ownerless("dev-cakey-cluster"), lodiav1alpha1.ReasonSecretsExist, time.Minute},
		{"an ownerless staged kubelet signer", true, ownerless("dev-kubelet-signer-next"), lodiav1alpha1.ReasonSecretsExist, time.Minute},
		{"an ownerless request CA", false, ownerless("dev-cakey-kubeconfig-requests"), lodiav1alpha1.ReasonSecretsExist, time.Minute},
		{"an ownerless etcd generation", true, ownerless("dev-etcd-ca-1", "dev-etcd-server-1"), lodiav1alpha1.ReasonSecretsExist, time.Minute},
		{"owners but no controller", true, func(*lodiav1alpha1.ControlPlane) map[string][]metav1.OwnerReference {
			return map[string][]metav1.OwnerReference{"dev-sa-keys": {configMap}}
		}, lodiav1alpha1.ReasonSecretsExist, time.Minute},
		{"this ControlPlane as an owner but not the controller", false, func(cp *lodiav1alpha1.ControlPlane) map[string][]metav1.OwnerReference {
			return map[string][]metav1.OwnerReference{"dev-admin-kubeconfig": {{APIVersion: lodiav1alpha1.GroupVersion.String(), Kind: "ControlPlane", Name: cp.Name, UID: cp.UID}}}
		}, lodiav1alpha1.ReasonSecretsExist, time.Minute},
		{"a controller of another kind beside an ownerless Secret", true, func(*lodiav1alpha1.ControlPlane) map[string][]metav1.OwnerReference {
			controller := configMap
			controller.Controller = new(true)
			return map[string][]metav1.OwnerReference{"dev-front-proxy-client": {controller}, "dev-ca-bundle": nil}
		}, lodiav1alpha1.ReasonSecretsControlledElsewhere, time.Minute},
		{"a predecessor's reference", true, func(*lodiav1alpha1.ControlPlane) map[string][]metav1.OwnerReference {
			return map[string][]metav1.OwnerReference{"dev-apiserver-kubelet-client": {{APIVersion: lodiav1alpha1.GroupVersion.String(), Kind: "ControlPlane", Name: "dev", UID: other}}}
		}, lodiav1alpha1.ReasonSecretsControlledElsewhere, 10 * time.Second},
	} {
		t.Run(c.name, func(t *testing.T) {
			ctx := t.Context()
			r := newReconciler(time.Hour)
			cp := controlPlane(t, fmt.Sprintf("pki-restore-adopts-%d", i), restoring)
			publishEndpoint(t, cp)
			planted := c.plant(cp)
			for name, owners := range planted {
				s := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cp.Namespace, OwnerReferences: owners}, Data: map[string][]byte{"restored": []byte(name)}}
				if c.labelled {
					s.Labels = map[string]string{lodiav1alpha1.ControlPlaneLabel: cp.Name}
				}
				if err := k8s.Create(ctx, s); err != nil {
					t.Fatal(err)
				}
			}

			before := namespaceSecrets(t, cp.Namespace)
			result := reconcile(t, r, cp)
			got := condition(t, cp, lodiav1alpha1.ConditionCertificatesReady)
			if got.Status != metav1.ConditionFalse || got.Reason != c.reason {
				t.Fatalf("CertificatesReady is %s %s: %s", got.Status, got.Reason, got.Message)
			}
			if c.reason == lodiav1alpha1.ReasonSecretsExist {
				for name := range planted {
					if !strings.Contains(got.Message, name) {
						t.Errorf("the message %q does not name %s", got.Message, name)
					}
				}
			}
			if after := namespaceSecrets(t, cp.Namespace); !maps.Equal(before, after) {
				t.Errorf("a restore wrote Secrets beside the ones it found: %v, then %v", before, after)
			}
			if result.RequeueAfter != c.requeue {
				t.Errorf("requeue after %s, want %s", result.RequeueAfter, c.requeue)
			}

			for name := range planted {
				if err := k8s.Delete(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cp.Namespace}}); err != nil {
					t.Fatal(err)
				}
			}
			reconcile(t, r, cp)
			expectCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForBackup)
			for name := range planted {
				if s := secretsOf(t, cp)[name]; name != "dev-encryption-config" && name != "dev-kubelet-signer-next" && (s == nil || !metav1.IsControlledBy(s, cp)) {
					t.Errorf("%s was not written once the one found was deleted", name)
				}
			}
		})
	}
}

func ownerless(names ...string) func(*lodiav1alpha1.ControlPlane) map[string][]metav1.OwnerReference {
	return func(*lodiav1alpha1.ControlPlane) map[string][]metav1.OwnerReference {
		planted := map[string][]metav1.OwnerReference{}
		for _, name := range names {
			planted[name] = nil
		}
		return planted
	}
}

func TestARestoreWaitsForItsBackup(t *testing.T) {
	notSealed := "Backup " + backupName + " is not sealed yet"
	notItsKeys := backupKeys + " does not exist, or is not Backup " + backupName + "'s"
	for i, c := range []struct {
		name    string
		prepare func(t *testing.T, namespace string)
		message string
	}{
		{"no Backup", func(*testing.T, string) {}, notSealed},
		{"a Backup that is copying, its keys written", func(t *testing.T, namespace string) {
			b := newBackup(t, namespace)
			setReady(t, b, metav1.ConditionUnknown, lodiav1alpha1.ReasonCopying)
			writeBackupKeys(t, b, controllerOf(b), true, encryptionConfiguration(t, secretboxKey()))
		}, notSealed},
		{"a Backup waiting for its source", func(t *testing.T, namespace string) {
			setReady(t, newBackup(t, namespace), metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForDatastore)
		}, notSealed},
		{"a Backup whose source is not created yet", func(t *testing.T, namespace string) {
			setReady(t, newBackup(t, namespace), metav1.ConditionFalse, lodiav1alpha1.ReasonControlPlaneNotFound)
		}, notSealed},
		{"a Backup whose source is gone", func(t *testing.T, namespace string) {
			b := newBackup(t, namespace)
			b.Status.StartedAt = new(metav1.Now())
			setReady(t, b, metav1.ConditionFalse, lodiav1alpha1.ReasonControlPlaneNotFound)
		}, "Backup " + backupName + " is ControlPlaneNotFound: ControlPlaneNotFound; restore another Backup"},
		{"a Backup that failed", func(t *testing.T, namespace string) {
			setReady(t, newBackup(t, namespace), metav1.ConditionFalse, lodiav1alpha1.ReasonFailed)
		}, "Backup " + backupName + " is Failed: Failed; restore another Backup"},
		{"a Backup without a store", func(t *testing.T, namespace string) {
			setReady(t, newBackup(t, namespace), metav1.ConditionFalse, lodiav1alpha1.ReasonNoBackupStore)
		}, "Backup " + backupName + " is NoBackupStore: NoBackupStore; restore another Backup"},
		{"an unsupported Backup", func(t *testing.T, namespace string) {
			setReady(t, newBackup(t, namespace), metav1.ConditionFalse, lodiav1alpha1.ReasonUnsupported)
		}, "Backup " + backupName + " is Unsupported: Unsupported; restore another Backup"},
		{"a Backup whose seal is lost", func(t *testing.T, namespace string) {
			setReady(t, newBackup(t, namespace), metav1.ConditionFalse, lodiav1alpha1.ReasonSealLost)
		}, "Backup " + backupName + " is SealLost: SealLost; restore another Backup"},
		{"a Backup whose keys another controls", func(t *testing.T, namespace string) {
			setReady(t, newBackup(t, namespace), metav1.ConditionFalse, lodiav1alpha1.ReasonObjectsControlledElsewhere)
		}, "Backup " + backupName + " is ObjectsControlledElsewhere: ObjectsControlledElsewhere; restore another Backup"},
		{"no keys", func(t *testing.T, namespace string) {
			setReady(t, newBackup(t, namespace), metav1.ConditionTrue, lodiav1alpha1.ReasonCompleted)
		}, notItsKeys},
		{"the keys of a predecessor of the Backup", func(t *testing.T, namespace string) {
			b := newBackup(t, namespace)
			setReady(t, b, metav1.ConditionTrue, lodiav1alpha1.ReasonCompleted)
			predecessor := controllerOf(b)
			predecessor.UID = "7d1f0a52-3c2b-4f0e-8a6d-5b9c4e2f1a30"
			writeBackupKeys(t, b, predecessor, true, encryptionConfiguration(t, secretboxKey()))
		}, notItsKeys},
		{"keys without an owner", func(t *testing.T, namespace string) {
			b := newBackup(t, namespace)
			setReady(t, b, metav1.ConditionTrue, lodiav1alpha1.ReasonCompleted)
			writeBackupKeys(t, b, metav1.OwnerReference{}, true, encryptionConfiguration(t, secretboxKey()))
		}, notItsKeys},
		{"mutable keys", func(t *testing.T, namespace string) {
			b := newBackup(t, namespace)
			setReady(t, b, metav1.ConditionTrue, lodiav1alpha1.ReasonCompleted)
			writeBackupKeys(t, b, controllerOf(b), false, encryptionConfiguration(t, secretboxKey()))
		}, notItsKeys},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := newReconciler(time.Hour)
			cp := controlPlane(t, fmt.Sprintf("pki-restore-waits-%d", i), restoring)
			publishEndpoint(t, cp)
			c.prepare(t, cp.Namespace)
			reconcile(t, r, cp)
			expectCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForBackup)
			if got := condition(t, cp, lodiav1alpha1.ConditionCertificatesReady).Message; got != c.message {
				t.Errorf("the message is %q, want %q", got, c.message)
			}
			secrets := secretsOf(t, cp)
			for _, row := range secretTable(cp.Name) {
				if issued := secrets[row.name] != nil; issued != (row.name != "dev-encryption-config") {
					t.Errorf("%s issued %t while the restore waits for its Backup", row.name, issued)
				}
			}

			b := seal(t, cp.Namespace, encryptionConfiguration(t, secretboxKey()))
			reconcile(t, r, cp)
			expectCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionTrue, lodiav1alpha1.ReasonIssued)
			if got := writtenProviders(t, cp)[1:]; !reflect.DeepEqual(got, backupProviders(t, b)) {
				t.Errorf("the configuration holds %+v after its new key, want the Backup's %+v", got, backupProviders(t, b))
			}
		})
	}
}

func TestARestoreCopiesTheBackupsKeysBehindANewOne(t *testing.T) {
	ctx := t.Context()
	cp := controlPlane(t, "pki-restore-keys", restoring)
	publishEndpoint(t, cp)
	b := seal(t, cp.Namespace, encryptionConfiguration(t, aescbcKey(), secretboxKey(), apiserverv1.ProviderConfiguration{Identity: &apiserverv1.IdentityConfiguration{}}))
	want := backupProviders(t, b)

	r := newReconciler(time.Hour)
	r.Client = failingApply{Client: controller, secret: "dev-encryption-config"}
	reconcileFails(t, r, cp)
	if c := condition(t, cp, lodiav1alpha1.ConditionCertificatesReady); c.Reason != lodiav1alpha1.ReasonIssuing || !strings.Contains(c.Message, "dev-encryption-config") {
		t.Fatalf("CertificatesReady is %s while the configuration cannot be written: %s", c.Reason, c.Message)
	}
	if secretsOf(t, cp)["dev-encryption-config"] != nil {
		t.Fatal("the configuration was written although its apply failed")
	}
	r.Client = controller
	reconcile(t, r, cp)
	expectCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionTrue, lodiav1alpha1.ReasonIssued)
	providers := writtenProviders(t, cp)
	if len(providers) != len(want)+1 || providers[0].Secretbox == nil || len(providers[0].Secretbox.Keys) != 1 || !newKeyName.MatchString(providers[0].Secretbox.Keys[0].Name) {
		t.Fatalf("the configuration is %+v, want a new secretbox key first, then the Backup's %+v", providers, want)
	}
	if !reflect.DeepEqual(providers[1:], want) {
		t.Errorf("the configuration holds %+v after its new key, want the Backup's %+v in its order", providers[1:], want)
	}
	if key := providers[0].Secretbox.Keys[0]; key.Name == want[0].AESCBC.Keys[0].Name || key.Name == want[1].Secretbox.Keys[0].Name || key.Secret == want[1].Secretbox.Keys[0].Secret {
		t.Errorf("the new key %s is one of the Backup's", key.Name)
	}

	written := secretOf(t, cp, "dev-encryption-config").Data[lodiav1alpha1.EncryptionConfigKey]
	for _, obj := range []client.Object{
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: backupKeys, Namespace: cp.Namespace}},
		&lodiav1alpha1.Backup{ObjectMeta: metav1.ObjectMeta{Name: backupName, Namespace: cp.Namespace}},
	} {
		if err := k8s.Delete(ctx, obj); err != nil {
			t.Fatal(err)
		}
	}
	restarted := newReconciler(time.Hour)
	restarted.Client = noBackupReads{controller}
	restarted.Reader = noBackupKeyReads{apiReader}
	reconcile(t, restarted, cp)
	expectCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionTrue, lodiav1alpha1.ReasonIssued)
	if got := secretOf(t, cp, "dev-encryption-config").Data[lodiav1alpha1.EncryptionConfigKey]; !bytes.Equal(got, written) {
		t.Errorf("the configuration changed once the Backup was gone:\n%s", got)
	}
}

func TestARestoreWithoutEncryptionHoldsIdentityOnce(t *testing.T) {
	cp := controlPlane(t, "pki-restore-identity", restoring, func(spec *lodiav1alpha1.ControlPlaneSpec) {
		spec.APIServer = &lodiav1alpha1.APIServer{Encryption: &lodiav1alpha1.Encryption{Provider: "None"}}
	})
	publishEndpoint(t, cp)
	identity := apiserverv1.ProviderConfiguration{Identity: &apiserverv1.IdentityConfiguration{}}
	b := seal(t, cp.Namespace, encryptionConfiguration(t, secretboxKey(), identity))
	reconcile(t, newReconciler(time.Hour), cp)
	expectCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionTrue, lodiav1alpha1.ReasonIssued)
	want := []apiserverv1.ProviderConfiguration{identity, backupProviders(t, b)[0]}
	if got := writtenProviders(t, cp); !reflect.DeepEqual(got, want) {
		t.Errorf("the configuration is %+v, want identity once, first, then the Backup's secretbox key", got)
	}
}

func TestUnreadableBackupKeysAreNeverReplaced(t *testing.T) {
	cp := controlPlane(t, "pki-restore-unreadable", restoring)
	publishEndpoint(t, cp)
	short := base64.StdEncoding.EncodeToString(make([]byte, 16))
	unreadable := []byte("apiVersion: apiserver.config.k8s.io/v1\nkind: EncryptionConfiguration\nresources:\n- resources: [secrets]\n  providers:\n  - secretbox:\n      keys:\n      - name: key-1761350400123456789\n        secret: " + short + "\n")
	seal(t, cp.Namespace, unreadable)
	before := namespaceSecrets(t, cp.Namespace)[backupKeys]
	reconcile(t, newReconciler(time.Hour), cp)
	got := condition(t, cp, lodiav1alpha1.ConditionCertificatesReady)
	if got.Reason != lodiav1alpha1.ReasonSecretUnreadable || !strings.Contains(got.Message, backupKeys) {
		t.Fatalf("CertificatesReady is %s: %s", got.Reason, got.Message)
	}
	if secretsOf(t, cp)["dev-encryption-config"] != nil {
		t.Error("an encryption configuration was written in place of the Backup's PKI cannot read")
	}
	if after := namespaceSecrets(t, cp.Namespace)[backupKeys]; after != before {
		t.Errorf("%s was written", backupKeys)
	}
}

func TestARestoreTakesKeysBeingDeleted(t *testing.T) {
	ctx := t.Context()
	cp := controlPlane(t, "pki-restore-keys-deleted", restoring)
	publishEndpoint(t, cp)
	b := seal(t, cp.Namespace, encryptionConfiguration(t, aescbcKey(), secretboxKey()))
	want := backupProviders(t, b)
	keys := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: backupKeys, Namespace: cp.Namespace}}
	patch := client.RawPatch(types.MergePatchType, []byte(`{"metadata":{"finalizers":["`+lodiav1alpha1.RestoreSourceFinalizer+`"]}}`))
	if err := k8s.Patch(ctx, keys, patch); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := k8s.Patch(context.Background(), keys, client.RawPatch(types.MergePatchType, []byte(`{"metadata":{"finalizers":null}}`))); err != nil {
			t.Error(err)
		}
	})
	if err := k8s.Delete(ctx, keys); err != nil {
		t.Fatal(err)
	}
	if err := k8s.Get(ctx, client.ObjectKeyFromObject(keys), keys); err != nil || keys.DeletionTimestamp.IsZero() {
		t.Fatalf("%s is not being deleted: %v", backupKeys, err)
	}
	reconcile(t, newReconciler(time.Hour), cp)
	expectCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionTrue, lodiav1alpha1.ReasonIssued)
	if got := writtenProviders(t, cp)[1:]; !reflect.DeepEqual(got, want) {
		t.Errorf("the configuration holds %+v after its new key, want the Backup's %+v", got, want)
	}
}

func TestATransientErrorReadingTheBackupWritesNothing(t *testing.T) {
	ctx := t.Context()
	r := newReconciler(time.Hour)
	cp := controlPlane(t, "pki-restore-read-fails", restoring)
	publishEndpoint(t, cp)
	reconcile(t, r, cp)
	expectCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForBackup)
	seal(t, cp.Namespace, encryptionConfiguration(t, secretboxKey()))
	secrets, status := resourceVersions(t, cp)
	for _, down := range []struct {
		name   string
		client client.Client
		reader client.Reader
	}{
		{"the Backup", noBackupReads{controller}, apiReader},
		{"its keys", controller, noBackupKeyReads{apiReader}},
	} {
		r.Client, r.Reader = down.client, down.reader
		if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); err == nil || err.Error() != "the API server is down" {
			t.Errorf("a pass that cannot read %s returned %v, want the read's error", down.name, err)
		}
		if after, afterStatus := resourceVersions(t, cp); !maps.Equal(after, secrets) || afterStatus != status {
			t.Errorf("a pass that cannot read %s wrote", down.name)
		}
	}
	r.Client, r.Reader = controller, apiReader
	reconcile(t, r, cp)
	expectCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionTrue, lodiav1alpha1.ReasonIssued)
}

func TestARestoreGoesOnWhenItsBackupCompletes(t *testing.T) {
	cp := controlPlane(t, "pki-restore-watch-"+strconv.FormatInt(time.Now().UnixNano(), 36), restoring)
	publishEndpoint(t, cp)
	b := newBackup(t, cp.Namespace)
	setReady(t, b, metav1.ConditionUnknown, lodiav1alpha1.ReasonCopying)
	options, err := operator.Cache()
	if err != nil {
		t.Fatal(err)
	}
	options.DefaultNamespaces = map[string]cache.Config{cp.Namespace: {}}
	options.ReaderFailOnMissingInformer = true
	mgr, err := ctrl.NewManager(controllerCfg, ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
		Cache:                  options,
		Controller:             config.Controller{SkipNameValidation: new(true)},
	})
	if err != nil {
		t.Fatal(err)
	}
	r := newReconciler(time.Hour)
	r.Client, r.Reader = mgr.GetClient(), mgr.GetAPIReader()
	if err := r.SetupWithManager(mgr, 10); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- mgr.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})

	waitFor(t, 30*time.Second, func() bool {
		c := condition(t, cp, lodiav1alpha1.ConditionCertificatesReady)
		return c != nil && c.Reason == lodiav1alpha1.ReasonWaitingForBackup && secretsOf(t, cp)["dev-etcd-ca-1"] != nil
	})
	settled(t, cp)
	writeBackupKeys(t, b, controllerOf(b), true, encryptionConfiguration(t, secretboxKey()))
	setReady(t, b, metav1.ConditionTrue, lodiav1alpha1.ReasonCompleted)
	waitFor(t, 30*time.Second, func() bool {
		c := condition(t, cp, lodiav1alpha1.ConditionCertificatesReady)
		return c != nil && c.Status == metav1.ConditionTrue && c.Reason == lodiav1alpha1.ReasonIssued
	})
	if got := writtenProviders(t, cp)[1:]; !reflect.DeepEqual(got, backupProviders(t, b)) {
		t.Errorf("the configuration holds %+v after its new key, want the Backup's", got)
	}
}

func settled(t *testing.T, cp *lodiav1alpha1.ControlPlane) {
	t.Helper()
	secrets, status := resourceVersions(t, cp)
	since := time.Now()
	waitFor(t, 30*time.Second, func() bool {
		after, afterStatus := resourceVersions(t, cp)
		if !maps.Equal(after, secrets) || afterStatus != status {
			secrets, status, since = after, afterStatus, time.Now()
		}
		return time.Since(since) >= 2*time.Second
	})
}

type noBackupReads struct {
	client.Client
}

func (c noBackupReads) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, backup := obj.(*lodiav1alpha1.Backup); backup {
		return errors.New("the API server is down")
	}
	return c.Client.Get(ctx, key, obj, opts...)
}

type noBackupKeyReads struct {
	client.Reader
}

func (r noBackupKeyReads) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if key.Name == backupKeys {
		return errors.New("the API server is down")
	}
	return r.Reader.Get(ctx, key, obj, opts...)
}

func newBackup(t *testing.T, namespace string) *lodiav1alpha1.Backup {
	t.Helper()
	b := &lodiav1alpha1.Backup{
		ObjectMeta: metav1.ObjectMeta{Name: backupName, Namespace: namespace},
		Spec:       lodiav1alpha1.BackupSpec{ControlPlaneRef: lodiav1alpha1.LocalReference{Name: "source"}},
	}
	if err := k8s.Create(t.Context(), b); err != nil {
		t.Fatal(err)
	}
	return b
}

func setReady(t *testing.T, b *lodiav1alpha1.Backup, status metav1.ConditionStatus, reason string) {
	t.Helper()
	meta.SetStatusCondition(&b.Status.Conditions, metav1.Condition{Type: lodiav1alpha1.ConditionReady, Status: status, Reason: reason, Message: reason, ObservedGeneration: b.Generation})
	if err := k8s.Status().Update(t.Context(), b); err != nil {
		t.Fatal(err)
	}
}

func controllerOf(b *lodiav1alpha1.Backup) metav1.OwnerReference {
	return *metav1.NewControllerRef(b, lodiav1alpha1.GroupVersion.WithKind("Backup"))
}

func writeBackupKeys(t *testing.T, b *lodiav1alpha1.Backup, owner metav1.OwnerReference, immutable bool, configuration []byte) {
	t.Helper()
	s := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: backupKeys, Namespace: b.Namespace, Labels: map[string]string{lodiav1alpha1.ControlPlaneLabel: b.Spec.ControlPlaneRef.Name}},
		Type:       corev1.SecretTypeOpaque,
		Immutable:  new(immutable),
		Data:       map[string][]byte{lodiav1alpha1.EncryptionConfigKey: configuration},
	}
	if owner.UID != "" {
		s.OwnerReferences = []metav1.OwnerReference{owner}
	}
	if err := k8s.Create(t.Context(), s); err != nil {
		t.Fatal(err)
	}
}

func seal(t *testing.T, namespace string, configuration []byte) *lodiav1alpha1.Backup {
	t.Helper()
	b := &lodiav1alpha1.Backup{}
	if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: namespace, Name: backupName}, b); err != nil {
		if client.IgnoreNotFound(err) != nil {
			t.Fatal(err)
		}
		b = newBackup(t, namespace)
	}
	setReady(t, b, metav1.ConditionTrue, lodiav1alpha1.ReasonCompleted)
	err := k8s.Delete(t.Context(), &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: backupKeys, Namespace: namespace}})
	if client.IgnoreNotFound(err) != nil {
		t.Fatal(err)
	}
	writeBackupKeys(t, b, controllerOf(b), true, configuration)
	return b
}

func encryptionConfiguration(t *testing.T, providers ...apiserverv1.ProviderConfiguration) []byte {
	t.Helper()
	out, err := yaml.Marshal(apiserverv1.EncryptionConfiguration{
		TypeMeta:  metav1.TypeMeta{APIVersion: apiserverv1.SchemeGroupVersion.String(), Kind: "EncryptionConfiguration"},
		Resources: []apiserverv1.ResourceConfiguration{{Resources: []string{"secrets"}, Providers: providers}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func randomKey() []apiserverv1.Key {
	secret := make([]byte, 32)
	_, _ = rand.Read(secret)
	return []apiserverv1.Key{{Name: "key-" + strconv.FormatInt(time.Now().UnixNano(), 10), Secret: base64.StdEncoding.EncodeToString(secret)}}
}

func secretboxKey() apiserverv1.ProviderConfiguration {
	return apiserverv1.ProviderConfiguration{Secretbox: &apiserverv1.SecretboxConfiguration{Keys: randomKey()}}
}

func aescbcKey() apiserverv1.ProviderConfiguration {
	return apiserverv1.ProviderConfiguration{AESCBC: &apiserverv1.AESConfiguration{Keys: randomKey()}}
}

func providersIn(t *testing.T, s *corev1.Secret) []apiserverv1.ProviderConfiguration {
	t.Helper()
	cfg := &apiserverv1.EncryptionConfiguration{}
	if err := yaml.UnmarshalStrict(s.Data[lodiav1alpha1.EncryptionConfigKey], cfg); err != nil {
		t.Fatalf("%s: %v", s.Name, err)
	}
	if len(cfg.Resources) != 1 || !slices.Equal(cfg.Resources[0].Resources, []string{"secrets"}) {
		t.Fatalf("%s holds %+v", s.Name, cfg.Resources)
	}
	return cfg.Resources[0].Providers
}

func writtenProviders(t *testing.T, cp *lodiav1alpha1.ControlPlane) []apiserverv1.ProviderConfiguration {
	t.Helper()
	return providersIn(t, secretOf(t, cp, cp.Name+"-encryption-config"))
}

func backupProviders(t *testing.T, b *lodiav1alpha1.Backup) []apiserverv1.ProviderConfiguration {
	t.Helper()
	s := &corev1.Secret{}
	if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: b.Namespace, Name: backupKeys}, s); err != nil {
		t.Fatal(err)
	}
	return providersIn(t, s)
}
