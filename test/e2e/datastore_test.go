// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

const (
	tenant       = "team-e2e"
	controlPlane = "dev"
)

func TestDatastoreOnKind(t *testing.T) {
	var k8s client.Client
	f := features.New("the PKI and Datastore controllers with etcd-druid").
		Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			k8s = newClient(t, cfg)
			create(ctx, t, k8s, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: tenant}})
			create(ctx, t, k8s, &lodiav1alpha1.DatastoreClass{
				ObjectMeta: metav1.ObjectMeta{Name: "e2e"},
				Spec:       lodiav1alpha1.DatastoreClassSpec{Storage: lodiav1alpha1.EtcdStorage{Size: resource.MustParse("1Gi"), ReclaimPolicy: "Delete"}},
			})
			create(ctx, t, k8s, &lodiav1alpha1.ControlPlane{
				ObjectMeta: metav1.ObjectMeta{Name: controlPlane, Namespace: tenant},
				Spec: lodiav1alpha1.ControlPlaneSpec{
					Version:   "v1.37.1",
					Datastore: lodiav1alpha1.DatastoreChoice{Dedicated: &lodiav1alpha1.DedicatedDatastore{ClassName: "e2e"}},
					Endpoint:  lodiav1alpha1.Endpoint{LoadBalancer: &lodiav1alpha1.LoadBalancerEndpoint{}},
					Network:   lodiav1alpha1.Network{ServiceCIDRs: []lodiav1alpha1.CIDR{"10.96.0.0/16"}, PodCIDRs: []lodiav1alpha1.CIDR{"10.244.0.0/16"}},
				},
			})
			return ctx
		}).
		Assess("DatastoreReady=True with the member running on PKI's generation 1", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			until(ctx, t, 2*time.Minute, "PKI issued generation 1 of the etcd certificates", func(ctx context.Context) error {
				return issued(ctx, k8s, 1)
			})
			until(ctx, t, 5*time.Minute, "DatastoreReady=True for generation 1", func(ctx context.Context) error {
				return conditionIs(ctx, k8s, metav1.ConditionTrue, lodiav1alpha1.ReasonReady)
			})
			pod := &corev1.Pod{}
			if err := k8s.Get(ctx, client.ObjectKey{Namespace: tenant, Name: controlPlane + "-etcd-0"}, pod); err != nil {
				t.Fatal(err)
			}
			if err := running(pod); err != nil {
				t.Fatal(err)
			}
			if err := mountsGeneration(pod, 1); err != nil {
				t.Fatal(err)
			}
			t.Logf("pod %s is %s and mounts %s", pod.Name, pod.Status.Phase, strings.Join(secretVolumes(pod), ", "))
			logEtcd(ctx, t, k8s)
			return ctx
		}).
		Assess("a member Secret deleted, PKI issues generation 2 under the same CA and the member rolls onto it", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			first := &corev1.Secret{}
			if err := k8s.Get(ctx, client.ObjectKey{Namespace: tenant, Name: lodiav1alpha1.EtcdCertificateSecretName(controlPlane, lodiav1alpha1.EtcdCARole, 1)}, first); err != nil {
				t.Fatal(err)
			}
			member := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: tenant, Name: lodiav1alpha1.EtcdCertificateSecretName(controlPlane, lodiav1alpha1.EtcdClientRole, 1)}}
			if err := k8s.Delete(ctx, member); err != nil {
				t.Fatal(err)
			}
			t.Logf("Secret %s of generation 1 deleted", member.Name)
			until(ctx, t, 2*time.Minute, "PKI issued generation 2 of the etcd certificates", func(ctx context.Context) error {
				return issued(ctx, k8s, 2)
			})
			second := &corev1.Secret{}
			if err := k8s.Get(ctx, client.ObjectKey{Namespace: tenant, Name: lodiav1alpha1.EtcdCertificateSecretName(controlPlane, lodiav1alpha1.EtcdCARole, 2)}, second); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(first.Data[lodiav1alpha1.CACertKey], second.Data[lodiav1alpha1.CACertKey]) {
				t.Fatalf("generation 2 is not under the CA of generation 1")
			}
			t.Logf("generation 2 is under the CA of generation 1")
			until(ctx, t, 2*time.Minute, "the Etcd names generation 2 in clientUrlTls, peerUrlTls and backup.tls", func(ctx context.Context) error {
				etcd := &druidv1alpha1.Etcd{}
				if err := k8s.Get(ctx, client.ObjectKey{Namespace: tenant, Name: controlPlane + "-etcd"}, etcd); err != nil {
					return err
				}
				for _, ref := range []string{
					etcd.Spec.Etcd.ClientUrlTLS.TLSCASecretRef.Name, etcd.Spec.Etcd.ClientUrlTLS.ServerTLSSecretRef.Name, etcd.Spec.Etcd.ClientUrlTLS.ClientTLSSecretRef.Name,
					etcd.Spec.Etcd.PeerUrlTLS.TLSCASecretRef.Name, etcd.Spec.Etcd.PeerUrlTLS.ServerTLSSecretRef.Name,
					etcd.Spec.Backup.TLS.TLSCASecretRef.Name, etcd.Spec.Backup.TLS.ServerTLSSecretRef.Name, etcd.Spec.Backup.TLS.ClientTLSSecretRef.Name,
				} {
					if !strings.HasSuffix(ref, "-2") {
						return fmt.Errorf("%s is not of generation 2", ref)
					}
				}
				return nil
			})
			until(ctx, t, 2*time.Minute, "DatastoreReady leaves True for the roll", func(ctx context.Context) error {
				return conditionIs(ctx, k8s, metav1.ConditionFalse, lodiav1alpha1.ReasonProvisioning)
			})
			until(ctx, t, 5*time.Minute, "the member runs with generation 2 mounted", func(ctx context.Context) error {
				pod := &corev1.Pod{}
				if err := k8s.Get(ctx, client.ObjectKey{Namespace: tenant, Name: controlPlane + "-etcd-0"}, pod); err != nil {
					return err
				}
				if err := mountsGeneration(pod, 2); err != nil {
					return err
				}
				return running(pod)
			})
			until(ctx, t, 3*time.Minute, "DatastoreReady=True for generation 2", func(ctx context.Context) error {
				etcd := &druidv1alpha1.Etcd{}
				if err := k8s.Get(ctx, client.ObjectKey{Namespace: tenant, Name: controlPlane + "-etcd"}, etcd); err != nil {
					return err
				}
				if ptr.Deref(etcd.Status.ObservedGeneration, 0) != etcd.Generation {
					return fmt.Errorf("etcd-druid observed generation %d of %d", ptr.Deref(etcd.Status.ObservedGeneration, 0), etcd.Generation)
				}
				return conditionIs(ctx, k8s, metav1.ConditionTrue, lodiav1alpha1.ReasonReady)
			})
			logEtcd(ctx, t, k8s)
			return ctx
		}).
		Assess("deleting the ControlPlane removes the Etcd and the member's claim", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			cp := &lodiav1alpha1.ControlPlane{ObjectMeta: metav1.ObjectMeta{Name: controlPlane, Namespace: tenant}}
			if err := k8s.Delete(ctx, cp); err != nil {
				t.Fatal(err)
			}
			until(ctx, t, 3*time.Minute, "the ControlPlane is gone", func(ctx context.Context) error {
				return gone(k8s.Get(ctx, client.ObjectKeyFromObject(cp), &lodiav1alpha1.ControlPlane{}))
			})
			if err := gone(k8s.Get(ctx, client.ObjectKey{Namespace: tenant, Name: controlPlane + "-etcd"}, &druidv1alpha1.Etcd{})); err != nil {
				t.Errorf("the Etcd: %v", err)
			}
			until(ctx, t, 2*time.Minute, "the member's claim is gone", func(ctx context.Context) error {
				return gone(k8s.Get(ctx, client.ObjectKey{Namespace: tenant, Name: controlPlane + "-etcd-" + controlPlane + "-etcd-0"}, &corev1.PersistentVolumeClaim{}))
			})
			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			if k8s != nil {
				for _, obj := range []client.Object{
					&lodiav1alpha1.DatastoreClass{ObjectMeta: metav1.ObjectMeta{Name: "e2e"}},
					&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: tenant}},
				} {
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

func create(ctx context.Context, t *testing.T, k8s client.Client, obj client.Object) {
	t.Helper()
	if err := k8s.Create(ctx, obj); err != nil {
		t.Fatal(err)
	}
}

func issued(ctx context.Context, k8s client.Client, generation int) error {
	secrets := &corev1.SecretList{}
	if err := k8s.List(ctx, secrets, client.InNamespace(tenant), client.MatchingLabels{lodiav1alpha1.EtcdCertificatesLabel: strconv.Itoa(generation)}); err != nil {
		return err
	}
	var names []string
	for _, secret := range secrets.Items {
		if secret.DeletionTimestamp.IsZero() {
			names = append(names, secret.Name)
		}
	}
	for _, certificate := range lodiav1alpha1.EtcdCertificateRoles() {
		if name := lodiav1alpha1.EtcdCertificateSecretName(controlPlane, certificate, generation); !slices.Contains(names, name) {
			return fmt.Errorf("%s is missing; generation %d holds %s", name, generation, strings.Join(names, ", "))
		}
	}
	return nil
}

func until(ctx context.Context, t *testing.T, timeout time.Duration, what string, check func(context.Context) error) {
	t.Helper()
	start := time.Now()
	var last error
	err := wait.PollUntilContextTimeout(ctx, 2*time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		last = check(ctx)
		return last == nil, nil
	})
	if err != nil {
		t.Fatalf("%s: %v: %v", what, err, last)
	}
	t.Logf("%s, after %s", what, time.Since(start).Round(time.Second))
}

func conditionIs(ctx context.Context, k8s client.Client, status metav1.ConditionStatus, reason string) error {
	cp := &lodiav1alpha1.ControlPlane{}
	if err := k8s.Get(ctx, client.ObjectKey{Namespace: tenant, Name: controlPlane}, cp); err != nil {
		return err
	}
	condition := meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionDatastoreReady)
	if condition == nil {
		return fmt.Errorf("no %s condition yet", lodiav1alpha1.ConditionDatastoreReady)
	}
	if condition.Status != status || condition.Reason != reason || condition.ObservedGeneration != cp.Generation {
		return fmt.Errorf("%s=%s (%s, generation %d of %d): %s", condition.Type, condition.Status, condition.Reason, condition.ObservedGeneration, cp.Generation, condition.Message)
	}
	return nil
}

func running(pod *corev1.Pod) error {
	if pod.Status.Phase != corev1.PodRunning {
		return fmt.Errorf("pod %s is %s", pod.Name, pod.Status.Phase)
	}
	for _, container := range pod.Status.ContainerStatuses {
		if !container.Ready {
			return fmt.Errorf("container %s of pod %s is not ready", container.Name, pod.Name)
		}
	}
	return nil
}

func secretVolumes(pod *corev1.Pod) []string {
	var names []string
	for _, volume := range pod.Spec.Volumes {
		if volume.Secret != nil {
			names = append(names, volume.Secret.SecretName)
		}
	}
	slices.Sort(names)
	return slices.Compact(names)
}

func mountsGeneration(pod *corev1.Pod, generation int) error {
	mounted := secretVolumes(pod)
	for _, certificate := range lodiav1alpha1.EtcdCertificateRoles() {
		if name := lodiav1alpha1.EtcdCertificateSecretName(controlPlane, certificate, generation); !slices.Contains(mounted, name) {
			return fmt.Errorf("pod %s does not mount %s; it mounts %s", pod.Name, name, strings.Join(mounted, ", "))
		}
	}
	return nil
}

func gone(err error) error {
	switch {
	case err == nil:
		return fmt.Errorf("still there")
	case apierrors.IsNotFound(err):
		return nil
	default:
		return err
	}
}

func logEtcd(ctx context.Context, t *testing.T, k8s client.Client) {
	t.Helper()
	etcd := &druidv1alpha1.Etcd{}
	if err := k8s.Get(ctx, client.ObjectKey{Namespace: tenant, Name: controlPlane + "-etcd"}, etcd); err != nil {
		t.Fatal(err)
	}
	var members []string
	for _, member := range etcd.Status.Members {
		members = append(members, fmt.Sprintf("%s %s %s", member.Name, ptr.Deref(member.Role, ""), member.Status))
	}
	t.Logf("Etcd %s generation %d: observedGeneration %d, ready %t, %d of %d replicas ready, members [%s], server certificate %s",
		etcd.Name, etcd.Generation, ptr.Deref(etcd.Status.ObservedGeneration, 0), ptr.Deref(etcd.Status.Ready, false),
		etcd.Status.ReadyReplicas, etcd.Spec.Replicas, strings.Join(members, "; "), etcd.Spec.Etcd.ClientUrlTLS.ServerTLSSecretRef.Name)
}
