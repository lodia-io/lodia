// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

//go:build e2e && upgrade

package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/distribution/reference"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"
	"sigs.k8s.io/kind/pkg/apis/config/defaults"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/versions"
)

const nodeImage135 = "kindest/node:v1.35.8@sha256:07b2536e30b803ed61d1677a79df6115f798ce64c80f9e22f6ed45afd09323c0"

func TestMinorUpgradeChain(t *testing.T) {
	const (
		from  = "v1.35.8"
		to    = "v1.37.1"
		class = "e2e-upgrade-chain"
	)
	var (
		k8s     client.WithWatch
		access  *tenantAccess
		upgrade *watchedUpgrade
	)
	key := client.ObjectKey{Namespace: "team-upgrade-chain", Name: controlPlane}
	through, ok := versions.Latest("1.36")
	if !ok {
		t.Fatal("the table has no latest of 1.36")
	}
	f := features.New("nightly on kind: a chain of two minors through the table's latest, a Backup before each").
		Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			k8s = newClient(t, cfg)
			startStore(ctx, t, k8s)
			createStoreTenant(ctx, t, k8s, key, class, from)
			nodeImage = nodeImage135
			return ctx
		}).
		Assess("the tenant is Ready at the first minor and a node of that minor joins", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			until(ctx, t, 5*time.Minute, "Ready=True", func(ctx context.Context) error {
				return controlPlaneReady(ctx, k8s, key, &lodiav1alpha1.ControlPlane{})
			})
			access = openTenant(ctx, t, k8s, key)
			joinNodes(ctx, t, cfg, k8s, key, access, 2)
			upgrade = watchUpgrade(ctx, t, k8s, key)
			return ctx
		}).
		Assess("the first minor step ends at the table's latest of 1.36, through its own Backup", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			setVersionTo(ctx, t, k8s, key, to)
			expectBackupCompleted(ctx, t, k8s, key, "1.36", from, through, upgrade)
			until(ctx, t, 10*time.Minute, "every part reports "+through, func(ctx context.Context) error {
				cp := &lodiav1alpha1.ControlPlane{}
				if err := k8s.Get(ctx, key, cp); err != nil {
					return err
				}
				c := cp.Status.Components
				if c == nil || c.APIServer == nil || c.ControllerManager == nil || c.Scheduler == nil || cp.Status.Addons == nil ||
					c.APIServer.Version != through || c.ControllerManager.Version != through || c.Scheduler.Version != through || cp.Status.Addons.Version != through {
					return fmt.Errorf("components %+v, addons %+v", c, cp.Status.Addons)
				}
				return nil
			})
			nodeImage = nodeImage136
			addNode(ctx, t, cfg, k8s, key, access, 3)
			return ctx
		}).
		Assess("the second minor step ends at spec.version, through its own Backup, and a node of the last minor joins", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			expectBackupCompleted(ctx, t, k8s, key, "1.37", through, to, upgrade)
			until(ctx, t, 10*time.Minute, "the upgrade ends at "+to, func(ctx context.Context) error {
				return upgradeEnded(ctx, k8s, key, to)
			})
			nodeImage = defaults.Image
			addNode(ctx, t, cfg, k8s, key, access, 4)
			upgrade.check(t)
			for name, want := range map[string]string{
				fmt.Sprintf("%s-tenant-node-1", clusterName(t, cfg)): from,
				fmt.Sprintf("%s-tenant-node-3", clusterName(t, cfg)): "v1.36.4",
			} {
				node := &corev1.Node{}
				if err := access.client.Get(ctx, client.ObjectKey{Name: name}, node); err != nil {
					t.Fatal(err)
				}
				if got := node.Status.NodeInfo.KubeletVersion; got != want {
					t.Errorf("node %s runs kubelet %s, want it kept at %s", name, got, want)
				}
			}
			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			if upgrade != nil {
				upgrade.stop()
			}
			closeStoreTenant(ctx, t, k8s, key, class)
			return ctx
		}).
		Feature()
	testenv.Test(t, f)
}

func addNode(ctx context.Context, t *testing.T, cfg *envconf.Config, k8s client.Client, key client.ObjectKey, access *tenantAccess, i int) {
	t.Helper()
	node := joinNode(ctx, t, cfg, k8s, key, access, bootstrapToken(ctx, t, access.client), i)
	waitForNode(ctx, t, access, node)
	waitForAgents(ctx, t, access, i)
}

func clusterName(t *testing.T, cfg *envconf.Config) string {
	t.Helper()
	name, err := kindCluster(cfg.KubeconfigFile())
	if err != nil {
		t.Fatal(err)
	}
	return name
}

func TestHighAvailabilityUpgrade(t *testing.T) {
	const (
		from  = upgradeFrom
		to    = upgradeTo
		class = "e2e-upgrade-ha"
	)
	var (
		k8s     client.WithWatch
		access  *tenantAccess
		loop    reference.NamedTagged
		loops   []apiLoopRun
		before  []loopCounts
		upgrade *watchedUpgrade
	)
	key := client.ObjectKey{Namespace: "team-upgrade-ha", Name: controlPlane}
	f := features.New("nightly on kind: a minor upgrade of a highly available tenant under two loops").
		Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			k8s = newClient(t, cfg)
			startStore(ctx, t, k8s)
			createStoreTenant(ctx, t, k8s, key, class, from, func(spec *lodiav1alpha1.ControlPlaneSpec) {
				spec.HighAvailability = &lodiav1alpha1.HighAvailability{FailureTolerance: "Zone"}
			})
			image, err := build(ctx, cfg.KubeconfigFile(), "./test/"+apiLoop)
			if err != nil {
				t.Fatal(err)
			}
			loop = image
			nodeImage = nodeImage136
			return ctx
		}).
		Assess("the tenant is Ready, a node joins, and the loops run", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			until(ctx, t, 8*time.Minute, "Ready=True", func(ctx context.Context) error {
				return controlPlaneReady(ctx, k8s, key, &lodiav1alpha1.ControlPlane{})
			})
			access = openTenant(ctx, t, k8s, key)
			node := joinNodes(ctx, t, cfg, k8s, key, access, 1)[0]
			until(ctx, t, 3*time.Minute, "CoreDNS is available", func(ctx context.Context) error {
				return coreDNSAvailable(ctx, access.client)
			})
			loops = startLoops(ctx, t, cfg, k8s, access, key, node, loop)
			before, _ = readLoops(ctx, t, "both loops answer on new and reused connections", loops)
			upgrade = watchUpgrade(ctx, t, k8s, key)
			return ctx
		}).
		Assess("the upgrade ends with every part at the new version and neither loop lost a request", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			setVersionTo(ctx, t, k8s, key, to)
			expectBackupCompleted(ctx, t, k8s, key, "1.37", from, to, upgrade)
			until(ctx, t, 15*time.Minute, "the upgrade ends at "+to, func(ctx context.Context) error {
				return upgradeEnded(ctx, k8s, key, to)
			})
			after, logs := readLoopsPast(ctx, t, time.Now(), loops)
			for i, l := range loops {
				checkLoop(t, l.where, before[i], after[i], logs[i])
			}
			stopLoops(ctx, t, loops)
			upgrade.check(t)
			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			if upgrade != nil {
				upgrade.stop()
			}
			closeStoreTenant(ctx, t, k8s, key, class)
			return ctx
		}).
		Feature()
	testenv.Test(t, f)
}
