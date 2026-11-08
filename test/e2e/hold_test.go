// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

//go:build e2e && hold

package e2e

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/distribution/reference"
	appsv1 "k8s.io/api/apps/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
	watchtools "k8s.io/client-go/tools/watch"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

const (
	holdTenant      = "team-hold"
	holdClass       = "e2e-hold"
	envoyProxyName  = "cluster-traffic"
	envoyProxyGroup = "gateway.envoyproxy.io"

	webReplicas           = 4
	controllerManagerName = "kube-controller-manager"

	healthPeriod       = 5 * time.Second
	cutSpread          = time.Second
	holdWithin         = 30 * time.Second
	leaderExitWithin   = 15 * time.Second
	cutFor             = 10 * time.Minute
	releaseDelay       = 2 * time.Minute
	leadWithin         = 10 * time.Second
	quietAfterRelease  = time.Minute
	kubeletRenewSpread = 10400 * time.Millisecond
	unknownLate        = 10 * time.Second
	taintWithin        = 10 * time.Second
	evictionDelay      = 300 * time.Second
	evictionSlack      = 15 * time.Second
	replacedWithin     = time.Minute

	curlTimedOut = 28
)

func TestControllerManagerHold(t *testing.T) {
	var (
		k8s       client.WithWatch
		access    *tenantAccess
		tenantAPI client.WithWatch
		nodes     []string
		history   *recorder
		operator  []string
		followers []*follower
	)
	seen := &holdRecord{pods: map[types.UID]*podSample{}}
	cutting := &cutter{}
	key := client.ObjectKey{Namespace: holdTenant, Name: controlPlane}
	gateway := tenantsGateway(key.Namespace)
	f := features.New("gate G4 on kind: both nodes of a highly available tenant cut off from the Gateway for 10 minutes, then one of them").
		Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			k8s = newClient(t, cfg)
			return ctx
		}).
		Assess("the GatewayClass's EnvoyProxy sends the Gateway's traffic to every node, and a highly available ControlPlane behind the Gateway turns Ready", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			clusterTraffic(ctx, t, k8s)
			createGateway(ctx, t, k8s, gateway)
			until(ctx, t, time.Minute, "the Service of Gateway "+gatewayNamespace+"/"+gatewayName+" has externalTrafficPolicy Cluster", func(ctx context.Context) error {
				services := &corev1.ServiceList{}
				if err := k8s.List(ctx, services, client.InNamespace(envoyGatewayNamespace), client.MatchingLabels{
					"gateway.envoyproxy.io/owning-gateway-namespace": gatewayNamespace, "gateway.envoyproxy.io/owning-gateway-name": gatewayName,
				}); err != nil {
					return err
				}
				if len(services.Items) != 1 || services.Items[0].Spec.ExternalTrafficPolicy != corev1.ServiceExternalTrafficPolicyCluster {
					return fmt.Errorf("%d Services, the first with %+v", len(services.Items), services.Items)
				}
				return nil
			})
			createTenant(ctx, t, k8s, key, holdClass, behindTheGateway(key.Namespace), func(spec *lodiav1alpha1.ControlPlaneSpec) {
				spec.HighAvailability = &lodiav1alpha1.HighAvailability{FailureTolerance: "Zone"}
			})
			until(ctx, t, 8*time.Minute, "Ready=True", func(ctx context.Context) error {
				return controlPlaneReady(ctx, k8s, key, &lodiav1alpha1.ControlPlane{})
			})
			operator = operatorPods(ctx, t, k8s)
			return ctx
		}).
		Assess("two nodes join by hostname, the Deployment web runs four pods with the 300 s tolerations on both, and the test starts to watch", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			kubeconfig := adminKubeconfig(ctx, t, k8s, key)
			throughTheGateway(kubeconfig)
			access = connect(t, kubeconfig)
			var err error
			if tenantAPI, err = client.NewWithWatch(access.cfg, client.Options{}); err != nil {
				t.Fatal(err)
			}
			nodeImages = []reference.NamedTagged{tunnelImage(ctx, t, access)}
			nodes = joinNodes(ctx, t, cfg, k8s, key, access, 2)
			until(ctx, t, 3*time.Minute, "CoreDNS is available", func(ctx context.Context) error {
				return coreDNSAvailable(ctx, access.client)
			})
			create(ctx, t, access.client, webDeployment())
			until(ctx, t, 5*time.Minute, "four web pods run, each with the 300 s tolerations, and each node runs one at least", func(ctx context.Context) error {
				pods, err := webPods(ctx, access.client)
				if err != nil {
					return err
				}
				perNode := map[string]int{}
				for i := range pods {
					if err := errors.Join(running(&pods[i]), tolerates(&pods[i])); err != nil {
						return err
					}
					perNode[pods[i].Spec.NodeName]++
				}
				if len(pods) != webReplicas || len(perNode) != len(nodes) {
					return fmt.Errorf("%d web pods on %v", len(pods), perNode)
				}
				return nil
			})
			followers = append(followers,
				follow(ctx, t, tenantAPI, &corev1.NodeList{}, func(_ watch.EventType, obj client.Object, at time.Time) { seen.observeNode(obj.(*corev1.Node), at) }),
				follow(ctx, t, tenantAPI, &coordinationv1.LeaseList{}, func(_ watch.EventType, obj client.Object, at time.Time) {
					seen.observeLease(obj.(*coordinationv1.Lease), at)
				},
					client.InNamespace(lodiav1alpha1.LeaderElectionNamespace), client.MatchingFields{"metadata.name": lodiav1alpha1.ControllerManagerLease}),
				follow(ctx, t, tenantAPI, &corev1.PodList{}, func(event watch.EventType, obj client.Object, at time.Time) {
					seen.observePod(event, obj.(*corev1.Pod), at)
				}),
				follow(ctx, t, k8s, &lodiav1alpha1.ControlPlaneList{}, func(_ watch.EventType, obj client.Object, at time.Time) {
					seen.observeControlPlane(obj.(*lodiav1alpha1.ControlPlane), at)
				},
					client.InNamespace(key.Namespace), client.MatchingFields{"metadata.name": key.Name}),
			)
			history = record(ctx, t, k8s, key)
			until(ctx, t, 2*time.Minute, "a controller manager of the ControlPlane holds the Lease and ControllerManagerHeld is NotHeld", func(ctx context.Context) error {
				holder := seen.lastHolder().holder
				pod, _, _ := strings.Cut(holder, "_")
				if err := k8s.Get(ctx, client.ObjectKey{Namespace: key.Namespace, Name: pod}, &corev1.Pod{}); err != nil {
					return fmt.Errorf("holder %q: %w", holder, err)
				}
				if h := seen.lastHeld(); h.status != metav1.ConditionFalse || h.reason != lodiav1alpha1.ReasonNotHeld {
					return fmt.Errorf("ControllerManagerHeld is %s, %s: %s", h.status, h.reason, h.message)
				}
				return nil
			})
			return ctx
		}).
		Assess("A: both nodes cut for 10 minutes, the Lease is held and the controller manager exits, no node is marked and no pod is lost, and the release comes 2 minutes after they are back", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			cp := &lodiav1alpha1.ControlPlane{}
			if err := k8s.Get(ctx, key, cp); err != nil {
				t.Fatal(err)
			}
			targets, ips := cutTargets(ctx, t, nodes[0], cp)
			cutting.ips = ips
			requireBlocked(ctx, t, nodes, targets, false)
			start := time.Now()
			podsBefore := tenantPods(ctx, t, access.client)
			restartsBefore := controllerManagerRestarts(ctx, t, k8s, key)
			leader, _, _ := strings.Cut(seen.lastHolder().holder, "_")
			t.Logf("controller manager pod %s leads; the pods are %v", leader, controllerManagerNames(ctx, t, k8s, key))

			cuts := cutting.cut(ctx, t, nodes...)
			defer cutting.lift(ctx, t, nodes...)
			firstCut, lastCut := slices.MinFunc(cuts, time.Time.Compare), slices.MaxFunc(cuts, time.Time.Compare)
			t.Logf("both nodes are cut off from %v: at %s and %s", ips, stamp(firstCut), stamp(lastCut))
			if spread := lastCut.Sub(firstCut); spread > cutSpread {
				t.Errorf("the cuts were %s apart, want at most %s", spread, cutSpread)
			}
			requireBlocked(ctx, t, nodes, targets, true)

			var taken holderSample
			until(ctx, t, time.Minute, "the Lease is held by "+lodiav1alpha1.HoldHolderIdentity, func(ctx context.Context) error {
				var ok bool
				if taken, ok = seen.firstHolder(firstCut, func(s holderSample) bool { return s.holder == lodiav1alpha1.HoldHolderIdentity }); !ok {
					return errors.New("its holder is not yet " + lodiav1alpha1.HoldHolderIdentity)
				}
				return nil
			})
			t.Logf("the Lease went to %s at %s, %s after the later cut", taken.holder, stamp(taken.at), taken.at.Sub(lastCut))
			if took := taken.at.Sub(lastCut); took > holdWithin {
				t.Errorf("the holder became %s %s after the later cut, want at most %s", taken.holder, took, holdWithin)
			}

			var reported heldSample
			until(ctx, t, time.Minute, "ControllerManagerHeld is True, NodesSilent, and status.health.controllerManagerHold is present", func(ctx context.Context) error {
				var ok bool
				reported, ok = seen.firstHeld(firstCut, func(s heldSample) bool {
					return s.status == metav1.ConditionTrue && s.reason == lodiav1alpha1.ReasonNodesSilent && s.hold != nil
				})
				if !ok {
					return errors.New("not yet reported")
				}
				return nil
			})
			t.Logf("ControllerManagerHeld turned True at %s: %s; %+v", stamp(reported.at), reported.message, *reported.hold)
			if late := reported.at.Sub(taken.at); late > healthPeriod {
				t.Errorf("ControllerManagerHeld came %s after the holder changed, want at most one period, %s", late, healthPeriod)
			}
			if h := reported.hold; h.SilentNodes != 2 || h.CountedNodes != 2 || h.Since == nil {
				t.Errorf("status.health.controllerManagerHold is %+v, want 2 of 2 nodes silent since the Lease was taken", *h)
			}

			var finished time.Time
			until(ctx, t, time.Minute, "the container of pod "+leader+", the previous leader, has exited and restarted", func(ctx context.Context) error {
				pod := &corev1.Pod{}
				if err := k8s.Get(ctx, client.ObjectKey{Namespace: key.Namespace, Name: leader}, pod); err != nil {
					return err
				}
				c := controllerManagerContainer(pod)
				if c == nil || c.RestartCount <= restartsBefore[leader] || c.LastTerminationState.Terminated == nil {
					return fmt.Errorf("container status %+v", c)
				}
				finished = c.LastTerminationState.Terminated.FinishedAt.Time
				return nil
			})
			t.Logf("the container of pod %s finished at %s, %s after the holder changed", leader, stamp(finished), finished.Sub(taken.at))
			if d := finished.Sub(taken.at); d < -time.Second || d > leaderExitWithin {
				t.Errorf("the previous leader's container finished %s after the holder changed, want within %s", d, leaderExitWithin)
			}

			sleepUntil(ctx, t, lastCut.Add(cutFor))
			reconnected := cutting.lift(ctx, t, nodes...)
			requireBlocked(ctx, t, nodes, targets, false)
			t.Logf("both nodes are reconnected at %s, %s after the later cut", stamp(reconnected), reconnected.Sub(lastCut))

			var back heldSample
			until(ctx, t, 5*time.Minute, "fewer silent nodes than the fraction: the first node renews its Lease again", func(ctx context.Context) error {
				var ok bool
				back, ok = seen.firstHeld(reconnected, func(s heldSample) bool {
					return s.hold != nil && s.hold.CountedNodes > 0 && s.hold.SilentNodes*10 < 6*s.hold.CountedNodes
				})
				if !ok {
					return errors.New("not yet reported")
				}
				return nil
			})
			t.Logf("the nodes are back at %s, %s after the reconnect: %s", stamp(back.at), back.at.Sub(reconnected), back.message)

			var released holderSample
			until(ctx, t, 5*time.Minute, "the Lease is released: no holder and a duration of 1 s", func(ctx context.Context) error {
				var ok bool
				released, ok = seen.firstHolder(reconnected, func(s holderSample) bool { return s.holder == "" && s.duration == 1 })
				if !ok {
					return errors.New("not yet released")
				}
				return nil
			})
			t.Logf("the Lease was released at %s, %s after the nodes were back", stamp(released.at), released.at.Sub(back.at))
			if d := released.at.Sub(back.at); d < releaseDelay-time.Second || d > releaseDelay+2*healthPeriod {
				t.Errorf("the Lease was released %s after the nodes were back, want %s to %s", d, releaseDelay-time.Second, releaseDelay+2*healthPeriod)
			}

			var leading holderSample
			until(ctx, t, time.Minute, "a controller manager holds the Lease again", func(ctx context.Context) error {
				var ok bool
				leading, ok = seen.firstHolder(released.at, func(s holderSample) bool {
					return s.holder != "" && s.holder != lodiav1alpha1.HoldHolderIdentity
				})
				if !ok {
					return errors.New("no controller manager holds it")
				}
				return nil
			})
			t.Logf("%s took the Lease at %s, %s after the release", leading.holder, stamp(leading.at), leading.at.Sub(released.at))
			if d := leading.at.Sub(released.at); d > leadWithin {
				t.Errorf("a controller manager led %s after the release, want within %s", d, leadWithin)
			}
			until(ctx, t, 30*time.Second, "ControllerManagerHeld is NotHeld and status.health.controllerManagerHold is gone", func(ctx context.Context) error {
				if h := seen.lastHeld(); h.status != metav1.ConditionFalse || h.reason != lodiav1alpha1.ReasonNotHeld || h.hold != nil {
					return fmt.Errorf("ControllerManagerHeld is %s, %s: %s; hold %v", h.status, h.reason, h.message, h.hold)
				}
				return nil
			})

			end := released.at.Add(quietAfterRelease)
			sleepUntil(ctx, t, end)
			for _, h := range seen.holdersBetween(taken.at, released.at) {
				if h.holder != lodiav1alpha1.HoldHolderIdentity {
					t.Errorf("at %s the holder was %q between the hold and its release", stamp(h.at), h.holder)
				}
			}
			for _, problem := range seen.unsettled(firstCut, end) {
				t.Errorf("between the cut and a minute after the release: %s", problem)
			}
			podsAfter := tenantPods(ctx, t, access.client)
			if !maps.Equal(podsBefore, podsAfter) {
				t.Errorf("the tenant's pods differ after the cut: before %v, after %v", podsBefore, podsAfter)
			}
			t.Logf("restarts of the controller managers: %v before the cut, %v after", restartsBefore, controllerManagerRestarts(ctx, t, k8s, key))
			history.log(t, start, end)
			history.alwaysTrue(t, start, end, lodiav1alpha1.ConditionReady)
			return ctx
		}).
		Assess("B: one node cut, no hold, the node is Unknown and tainted, its web pods are evicted after the toleration and replaced on the other node, DaemonSet pods stay", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			cut, other := nodes[0], nodes[1]
			cp := &lodiav1alpha1.ControlPlane{}
			if err := k8s.Get(ctx, key, cp); err != nil {
				t.Fatal(err)
			}
			targets, ips := cutTargets(ctx, t, cut, cp)
			cutting.ips = ips
			until(ctx, t, 3*time.Minute, "the tenant has recovered: Ready, both nodes Ready and untainted, a controller manager holding the Lease, four web pods running", func(ctx context.Context) error {
				if err := controlPlaneReady(ctx, k8s, key, &lodiav1alpha1.ControlPlane{}); err != nil {
					return err
				}
				if problems := nodeProblems(ctx, access.client); len(problems) > 0 {
					return fmt.Errorf("nodes: %v", problems)
				}
				if holder := seen.lastHolder().holder; holder == "" || holder == lodiav1alpha1.HoldHolderIdentity {
					return fmt.Errorf("the Lease is held by %q", holder)
				}
				pods, err := webPods(ctx, access.client)
				if err != nil {
					return err
				}
				if len(pods) != webReplicas {
					return fmt.Errorf("%d web pods", len(pods))
				}
				for i := range pods {
					if err := running(&pods[i]); err != nil {
						return err
					}
				}
				return nil
			})
			before := seen.podsNow()
			evicting := podsOn(before, cut, "web", "")
			untouched := append(podsOn(before, cut, "", "DaemonSet"), podsOn(before, other, "web", "")...)
			if len(evicting) == 0 {
				t.Fatalf("node %s runs no web pod: %v", cut, before)
			}
			t.Logf("node %s runs %d web pods %v and node %s %v; %d pods are not to be touched", cut, len(evicting), names(before, evicting), other, names(before, podsOn(before, other, "web", "")), len(untouched))

			start := time.Now()
			cuts := cutting.cut(ctx, t, cut)
			defer cutting.lift(ctx, t, cut)
			cutAt := cuts[0]
			requireBlocked(ctx, t, []string{cut}, targets, true)
			requireBlocked(ctx, t, []string{other}, targets, false)
			t.Logf("node %s is cut off at %s", cut, stamp(cutAt))

			var unknown nodeSample
			until(ctx, t, 3*time.Minute, "node "+cut+" has Ready=Unknown", func(ctx context.Context) error {
				var ok bool
				if unknown, ok = seen.firstNode(cutAt, func(s nodeSample) bool { return s.node == cut && s.ready == corev1.ConditionUnknown }); !ok {
					return errors.New("not yet")
				}
				return nil
			})
			lowest, highest := lodiav1alpha1.NodeMonitorGracePeriod-kubeletRenewSpread, lodiav1alpha1.NodeMonitorGracePeriod+unknownLate
			t.Logf("node %s turned Unknown at %s, %s after the cut", cut, stamp(unknown.at), unknown.at.Sub(cutAt))
			if d := unknown.at.Sub(cutAt); d < lowest || d > highest {
				t.Errorf("node %s turned Unknown %s after the cut, want %s to %s", cut, d, lowest, highest)
			}

			unreachable := corev1.TaintNodeUnreachable + ":" + string(corev1.TaintEffectNoExecute)
			var tainted nodeSample
			until(ctx, t, time.Minute, "node "+cut+" carries the taint "+unreachable, func(ctx context.Context) error {
				var ok bool
				if tainted, ok = seen.firstNode(cutAt, func(s nodeSample) bool { return s.node == cut && slices.Contains(s.taints, unreachable) }); !ok {
					return errors.New("not yet")
				}
				return nil
			})
			t.Logf("node %s was tainted at %s, %s after it turned Unknown", cut, stamp(tainted.at), tainted.at.Sub(unknown.at))
			if d := tainted.at.Sub(unknown.at); d > taintWithin {
				t.Errorf("node %s was tainted %s after it turned Unknown, want within %s", cut, d, taintWithin)
			}

			var lastDeleted time.Time
			until(ctx, t, evictionDelay+evictionSlack+time.Minute, "the web pods of node "+cut+" are being deleted", func(ctx context.Context) error {
				for _, p := range seen.podsNow() {
					if slices.Contains(evicting, p.uid) && p.deleting.IsZero() {
						return fmt.Errorf("pod %s is not deleted", p.name)
					}
				}
				return nil
			})
			for _, p := range seen.podsNow() {
				if !slices.Contains(evicting, p.uid) {
					continue
				}
				lastDeleted = latest(lastDeleted, p.deleting)
				t.Logf("pod %s was deleted at %s, %s after the taint", p.name, stamp(p.deleting), p.deleting.Sub(tainted.at))
				if d := p.deleting.Sub(tainted.at); d < evictionDelay-evictionSlack || d > evictionDelay+evictionSlack {
					t.Errorf("pod %s was deleted %s after the taint, want %s to %s", p.name, d, evictionDelay-evictionSlack, evictionDelay+evictionSlack)
				}
			}

			var replacedBy time.Time
			until(ctx, t, 2*time.Minute, fmt.Sprintf("%d replacement web pods run on node %s", len(evicting), other), func(ctx context.Context) error {
				var ready []time.Time
				for uid, p := range seen.podsNow() {
					if _, old := before[uid]; old || p.app != "web" || p.ready.IsZero() {
						continue
					}
					if p.node != other {
						return fmt.Errorf("the new pod %s runs on %s", p.name, p.node)
					}
					ready = append(ready, p.ready)
				}
				if len(ready) < len(evicting) {
					return fmt.Errorf("%d of %d replacements run", len(ready), len(evicting))
				}
				slices.SortFunc(ready, time.Time.Compare)
				replacedBy = ready[len(evicting)-1]
				return nil
			})
			t.Logf("the replacements run on node %s at %s, %s after the last deletion", other, stamp(replacedBy), replacedBy.Sub(lastDeleted))
			if d := replacedBy.Sub(lastDeleted); d > replacedWithin {
				t.Errorf("the replacements ran %s after the last deletion, want within %s", d, replacedWithin)
			}
			for _, uid := range untouched {
				if p := seen.podsNow()[uid]; !p.deleting.IsZero() || !p.gone.IsZero() {
					t.Errorf("pod %s on node %s or %s was deleted at %s", p.name, cut, other, stamp(p.deleting))
				}
			}

			cutting.lift(ctx, t, cut)
			requireBlocked(ctx, t, []string{cut}, targets, false)
			until(ctx, t, 5*time.Minute, "node "+cut+" is Ready again and no node carries a taint", func(ctx context.Context) error {
				if problems := nodeProblems(ctx, access.client); len(problems) > 0 {
					return fmt.Errorf("nodes: %v", problems)
				}
				return nil
			})
			end := time.Now()

			for _, h := range seen.holdersBetween(cutAt, end) {
				if h.holder == lodiav1alpha1.HoldHolderIdentity {
					t.Errorf("at %s the holder was %s while only one node was cut", stamp(h.at), h.holder)
				}
			}
			for _, h := range seen.heldAfter(cutAt) {
				if h.status != metav1.ConditionFalse || h.reason != lodiav1alpha1.ReasonNotHeld {
					t.Errorf("at %s ControllerManagerHeld was %s, %s: %s", stamp(h.at), h.status, h.reason, h.message)
				}
			}
			if after := operatorPods(ctx, t, k8s); !slices.Equal(after, operator) {
				t.Errorf("the operator's pods were [%s] before the cuts and are [%s]", strings.Join(operator, ", "), strings.Join(after, ", "))
			}
			history.log(t, start, end)
			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			for _, w := range followers {
				w.stop()
				if err := w.err(); err != nil {
					t.Errorf("a watch broke, so the record is incomplete: %v", err)
				}
			}
			if history != nil {
				history.stop()
			}
			cutting.lift(ctx, t, nodes...)
			if k8s == nil {
				return ctx
			}
			if err := client.IgnoreNotFound(k8s.Delete(ctx, gateway)); err != nil {
				t.Error(err)
			}
			closeTenant(ctx, t, k8s, key, holdClass)
			return ctx
		}).
		Feature()
	testenv.Test(t, f)
}

func clusterTraffic(ctx context.Context, t *testing.T, k8s client.Client) {
	t.Helper()
	create(ctx, t, k8s, &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": envoyProxyGroup + "/v1alpha1",
		"kind":       "EnvoyProxy",
		"metadata":   map[string]any{"namespace": envoyGatewayNamespace, "name": envoyProxyName},
		"spec": map[string]any{"provider": map[string]any{"type": "Kubernetes", "kubernetes": map[string]any{
			"envoyService": map[string]any{"externalTrafficPolicy": string(corev1.ServiceExternalTrafficPolicyCluster)},
		}}},
	}})
	class := &gatewayv1.GatewayClass{}
	if err := k8s.Get(ctx, client.ObjectKey{Name: gatewayClassName}, class); err != nil {
		t.Fatal(err)
	}
	patch := client.MergeFrom(class.DeepCopy())
	class.Spec.ParametersRef = &gatewayv1.ParametersReference{
		Group: envoyProxyGroup, Kind: "EnvoyProxy", Name: envoyProxyName, Namespace: new(gatewayv1.Namespace(envoyGatewayNamespace)),
	}
	if err := k8s.Patch(ctx, class, patch); err != nil {
		t.Fatal(err)
	}
	until(ctx, t, 2*time.Minute, "GatewayClass "+gatewayClassName+" is Accepted with the EnvoyProxy "+envoyProxyName, func(ctx context.Context) error {
		if err := k8s.Get(ctx, client.ObjectKeyFromObject(class), class); err != nil {
			return err
		}
		if c := meta.FindStatusCondition(class.Status.Conditions, string(gatewayv1.GatewayClassConditionStatusAccepted)); c == nil || c.Status != metav1.ConditionTrue || c.ObservedGeneration != class.Generation {
			return fmt.Errorf("Accepted is %+v at generation %d", c, class.Generation)
		}
		return nil
	})
}

func webDeployment() *appsv1.Deployment {
	labels := map[string]string{"app": "web"}
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: metav1.NamespaceDefault, Name: "web"},
		Spec: appsv1.DeploymentSpec{
			Replicas: new(int32(webReplicas)),
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "web", Image: agnhost, Args: []string{"netexec", "--http-port=8080"}}},
					TopologySpreadConstraints: []corev1.TopologySpreadConstraint{{
						MaxSkew: 1, TopologyKey: corev1.LabelHostname, WhenUnsatisfiable: corev1.ScheduleAnyway,
						LabelSelector: &metav1.LabelSelector{MatchLabels: labels},
					}},
				},
			},
		},
	}
}

func webPods(ctx context.Context, c client.Client) ([]corev1.Pod, error) {
	pods := &corev1.PodList{}
	if err := c.List(ctx, pods, client.InNamespace(metav1.NamespaceDefault), client.MatchingLabels{"app": "web"}); err != nil {
		return nil, err
	}
	return pods.Items, nil
}

func tolerates(pod *corev1.Pod) error {
	for _, taint := range []string{corev1.TaintNodeNotReady, corev1.TaintNodeUnreachable} {
		if !slices.ContainsFunc(pod.Spec.Tolerations, func(t corev1.Toleration) bool {
			return t.Key == taint && t.Operator == corev1.TolerationOpExists && t.Effect == corev1.TaintEffectNoExecute &&
				ptr.Deref(t.TolerationSeconds, 0) == int64(evictionDelay/time.Second)
		}) {
			return fmt.Errorf("pod %s does not tolerate %s for %s: %+v", pod.Name, taint, evictionDelay, pod.Spec.Tolerations)
		}
	}
	return nil
}

func tenantPods(ctx context.Context, t *testing.T, c client.Client) map[string]types.UID {
	t.Helper()
	pods := &corev1.PodList{}
	if err := c.List(ctx, pods); err != nil {
		t.Fatal(err)
	}
	uids := map[string]types.UID{}
	for _, pod := range pods.Items {
		uids[pod.Namespace+"/"+pod.Name] = pod.UID
	}
	return uids
}

func nodeProblems(ctx context.Context, c client.Client) []string {
	nodes := &corev1.NodeList{}
	if err := c.List(ctx, nodes); err != nil {
		return []string{err.Error()}
	}
	var problems []string
	for i := range nodes.Items {
		sample := sampleOf(&nodes.Items[i], time.Time{})
		if sample.ready != corev1.ConditionTrue || len(sample.unsettledTaints()) > 0 {
			problems = append(problems, fmt.Sprintf("%s: Ready=%s, taints %v", sample.node, sample.ready, sample.taints))
		}
	}
	return problems
}

func controllerManagerNames(ctx context.Context, t *testing.T, k8s client.Client, key client.ObjectKey) []string {
	t.Helper()
	pods := &corev1.PodList{}
	if err := k8s.List(ctx, pods, client.InNamespace(key.Namespace), client.MatchingLabels{
		lodiav1alpha1.ControlPlaneLabel: key.Name, lodiav1alpha1.ComponentLabel: controllerManagerName,
	}); err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, pod := range pods.Items {
		out = append(out, pod.Name)
	}
	slices.Sort(out)
	return out
}

func controllerManagerRestarts(ctx context.Context, t *testing.T, k8s client.Client, key client.ObjectKey) map[string]int32 {
	t.Helper()
	restarts := map[string]int32{}
	for _, name := range controllerManagerNames(ctx, t, k8s, key) {
		pod := &corev1.Pod{}
		if err := k8s.Get(ctx, client.ObjectKey{Namespace: key.Namespace, Name: name}, pod); err != nil {
			t.Fatal(err)
		}
		if c := controllerManagerContainer(pod); c != nil {
			restarts[name] = c.RestartCount
		}
	}
	return restarts
}

func controllerManagerContainer(pod *corev1.Pod) *corev1.ContainerStatus {
	for i := range pod.Status.ContainerStatuses {
		if pod.Status.ContainerStatuses[i].Name == controllerManagerName {
			return &pod.Status.ContainerStatuses[i]
		}
	}
	return nil
}

func stamp(at time.Time) string {
	return at.Format("15:04:05.000")
}

func latest(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}

func sleepUntil(ctx context.Context, t *testing.T, at time.Time) {
	t.Helper()
	select {
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	case <-time.After(time.Until(at)):
	}
}

type target struct {
	host string
	port int32
	ip   string
}

func cutTargets(ctx context.Context, t *testing.T, node string, cp *lodiav1alpha1.ControlPlane) ([]target, []string) {
	t.Helper()
	e := cp.Status.Endpoint
	if e == nil || e.Tunnel == nil {
		t.Fatalf("status.endpoint is %+v, want the Gateway's three hostnames", e)
	}
	var targets []target
	var ips []string
	for _, path := range []lodiav1alpha1.HostPort{e.APIServer, e.Konnectivity, *e.Tunnel} {
		resolved := resolve(ctx, t, node, path.Host)
		targets = append(targets, target{host: path.Host, port: path.Port, ip: resolved[0]})
		for _, ip := range resolved {
			if !slices.Contains(ips, ip) {
				ips = append(ips, ip)
			}
		}
	}
	return targets, ips
}

func resolve(ctx context.Context, t *testing.T, node, host string) []string {
	t.Helper()
	if net.ParseIP(host) != nil {
		return []string{host}
	}
	out, err := docker(ctx, "exec", node, "getent", "ahostsv4", host)
	if err != nil {
		t.Fatal(err)
	}
	var ips []string
	for line := range strings.Lines(out) {
		if fields := strings.Fields(line); len(fields) > 0 && net.ParseIP(fields[0]) != nil && !slices.Contains(ips, fields[0]) {
			ips = append(ips, fields[0])
		}
	}
	if len(ips) == 0 {
		t.Fatalf("node %s resolves %s to no IPv4 address: %q", node, host, out)
	}
	return ips
}

func requireBlocked(ctx context.Context, t *testing.T, nodes []string, targets []target, want bool) {
	t.Helper()
	for _, node := range nodes {
		for _, to := range targets {
			_, err := docker(ctx, "exec", node, "curl", "--silent", "--insecure", "--output", "/dev/null", "--connect-timeout", "2", "--max-time", "2",
				"--resolve", to.host+":"+strconv.Itoa(int(to.port))+":"+to.ip, "https://"+net.JoinHostPort(to.host, strconv.Itoa(int(to.port)))+"/")
			var exit *exec.ExitError
			blocked := false
			switch {
			case err == nil:
			case errors.As(err, &exit):
				blocked = exit.ExitCode() == curlTimedOut
			default:
				t.Fatal(err)
			}
			if blocked != want {
				t.Fatalf("a 2 s curl from node %s to %s:%d at %s: blocked %t, want %t (%v)", node, to.host, to.port, to.ip, blocked, want, err)
			}
		}
	}
}

type cutter struct {
	ips []string
}

func (c *cutter) cut(ctx context.Context, t *testing.T, nodes ...string) []time.Time {
	t.Helper()
	var rules strings.Builder
	for _, ip := range c.ips {
		for _, chain := range []string{"PREROUTING", "OUTPUT"} {
			fmt.Fprintf(&rules, "iptables -t raw -I %s 1 -d %s -j DROP\n", chain, ip)
		}
	}
	script := rules.String()
	cuts, failed := make([]time.Time, len(nodes)), make([]error, len(nodes))
	var wg sync.WaitGroup
	for i, node := range nodes {
		wg.Go(func() {
			_, failed[i] = docker(ctx, "exec", node, "sh", "-ec", script)
			cuts[i] = time.Now()
		})
	}
	wg.Wait()
	if err := errors.Join(failed...); err != nil {
		t.Fatal(err)
	}
	return cuts
}

func (c *cutter) lift(ctx context.Context, t *testing.T, nodes ...string) time.Time {
	t.Helper()
	var rules strings.Builder
	for _, ip := range c.ips {
		for _, chain := range []string{"PREROUTING", "OUTPUT"} {
			fmt.Fprintf(&rules, "while iptables -t raw -C %[1]s -d %[2]s -j DROP 2>/dev/null; do iptables -t raw -D %[1]s -d %[2]s -j DROP || exit 1; done\n", chain, ip)
		}
	}
	lifting, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
	defer cancel()
	for _, node := range nodes {
		if len(c.ips) == 0 {
			break
		}
		if _, err := docker(lifting, "exec", node, "sh", "-ec", rules.String()); err != nil {
			t.Errorf("the rules cutting node %s off stay: %v", node, err)
		}
	}
	return time.Now()
}

type follower struct {
	mu     sync.Mutex
	broken error
	stop   func()
}

func (f *follower) err() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.broken
}

func follow(ctx context.Context, t *testing.T, c client.WithWatch, list client.ObjectList, handle func(watch.EventType, client.Object, time.Time), options ...client.ListOption) *follower {
	t.Helper()
	if err := c.List(ctx, list, options...); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if err := meta.EachListItem(list, func(item runtime.Object) error {
		handle(watch.Added, item.(client.Object), now)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	lw := &cache.ListWatch{WatchFuncWithContext: func(ctx context.Context, raw metav1.ListOptions) (watch.Interface, error) {
		return c.Watch(ctx, list.DeepCopyObject().(client.ObjectList), slices.Concat(options, []client.ListOption{&client.ListOptions{Raw: &raw}})...)
	}}
	watching, cancel := context.WithCancel(context.WithoutCancel(ctx))
	w, err := watchtools.NewRetryWatcherWithContext(watching, list.GetResourceVersion(), lw)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	f := &follower{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for event := range w.ResultChan() {
			obj, ok := event.Object.(client.Object)
			switch {
			case event.Type == watch.Error:
				f.mu.Lock()
				f.broken = errors.Join(f.broken, apierrors.FromObject(event.Object))
				f.mu.Unlock()
			case ok && (event.Type == watch.Added || event.Type == watch.Modified || event.Type == watch.Deleted):
				handle(event.Type, obj, time.Now())
			}
		}
	}()
	f.stop = sync.OnceFunc(func() {
		cancel()
		w.Stop()
		<-done
	})
	return f
}

type nodeSample struct {
	at     time.Time
	node   string
	ready  corev1.ConditionStatus
	taints []string
}

type holderSample struct {
	at       time.Time
	holder   string
	duration int32
}

type heldSample struct {
	at      time.Time
	status  metav1.ConditionStatus
	reason  string
	message string
	hold    *lodiav1alpha1.ControllerManagerHold
}

type podSample struct {
	uid                    types.UID
	name, node, app, owner string
	ready, deleting, gone  time.Time
}

func (s nodeSample) when() time.Time   { return s.at }
func (s holderSample) when() time.Time { return s.at }
func (s heldSample) when() time.Time   { return s.at }

func (s nodeSample) unsettledTaints() []string {
	var out []string
	for _, taint := range s.taints {
		if strings.HasPrefix(taint, corev1.TaintNodeUnreachable+":") || strings.HasPrefix(taint, corev1.TaintNodeNotReady+":") {
			out = append(out, taint)
		}
	}
	return out
}

func sampleOf(n *corev1.Node, at time.Time) nodeSample {
	sample := nodeSample{at: at, node: n.Name}
	for _, c := range n.Status.Conditions {
		if c.Type == corev1.NodeReady {
			sample.ready = c.Status
		}
	}
	for _, taint := range n.Spec.Taints {
		sample.taints = append(sample.taints, taint.Key+":"+string(taint.Effect))
	}
	slices.Sort(sample.taints)
	return sample
}

type holdRecord struct {
	mu      sync.Mutex
	nodes   []nodeSample
	holders []holderSample
	held    []heldSample
	pods    map[types.UID]*podSample
}

func (r *holdRecord) observeNode(n *corev1.Node, at time.Time) {
	next := sampleOf(n, at)
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, last := range slices.Backward(r.nodes) {
		if last.node == next.node {
			if last.ready == next.ready && slices.Equal(last.taints, next.taints) {
				return
			}
			break
		}
	}
	r.nodes = append(r.nodes, next)
}

func (r *holdRecord) observeLease(l *coordinationv1.Lease, at time.Time) {
	next := holderSample{at: at, holder: ptr.Deref(l.Spec.HolderIdentity, ""), duration: ptr.Deref(l.Spec.LeaseDurationSeconds, 0)}
	r.mu.Lock()
	defer r.mu.Unlock()
	if n := len(r.holders); n > 0 && r.holders[n-1].holder == next.holder && r.holders[n-1].duration == next.duration {
		return
	}
	r.holders = append(r.holders, next)
}

func (r *holdRecord) observeControlPlane(cp *lodiav1alpha1.ControlPlane, at time.Time) {
	next := heldSample{at: at, status: metav1.ConditionUnknown, reason: "Absent"}
	if c := meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionControllerManagerHeld); c != nil {
		next.status, next.reason, next.message = c.Status, c.Reason, c.Message
	}
	if cp.Status.Health != nil {
		next.hold = cp.Status.Health.ControllerManagerHold.DeepCopy()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if n := len(r.held); n > 0 {
		last := r.held[n-1]
		if last.status == next.status && last.reason == next.reason && last.message == next.message && sameHold(last.hold, next.hold) {
			return
		}
	}
	r.held = append(r.held, next)
}

func sameHold(a, b *lodiav1alpha1.ControllerManagerHold) bool {
	return (a == nil) == (b == nil) && (a == nil || a.SilentNodes == b.SilentNodes && a.CountedNodes == b.CountedNodes)
}

func (r *holdRecord) observePod(event watch.EventType, pod *corev1.Pod, at time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	p, ok := r.pods[pod.UID]
	if !ok {
		p = &podSample{uid: pod.UID, name: pod.Namespace + "/" + pod.Name}
		r.pods[pod.UID] = p
	}
	p.node, p.app = pod.Spec.NodeName, pod.Labels["app"]
	if owner := metav1.GetControllerOf(pod); owner != nil {
		p.owner = owner.Kind
	}
	if p.ready.IsZero() && running(pod) == nil {
		p.ready = at
	}
	if p.deleting.IsZero() && pod.DeletionTimestamp != nil {
		p.deleting = at
	}
	if p.gone.IsZero() && event == watch.Deleted {
		p.gone = at
	}
}

type timed interface{ when() time.Time }

func firstAfter[T timed](items []T, after time.Time, match func(T) bool) (T, bool) {
	for _, item := range items {
		if item.when().After(after) && match(item) {
			return item, true
		}
	}
	var none T
	return none, false
}

func (r *holdRecord) firstNode(after time.Time, match func(nodeSample) bool) (nodeSample, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return firstAfter(r.nodes, after, match)
}

func (r *holdRecord) firstHolder(after time.Time, match func(holderSample) bool) (holderSample, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return firstAfter(r.holders, after, match)
}

func (r *holdRecord) firstHeld(after time.Time, match func(heldSample) bool) (heldSample, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return firstAfter(r.held, after, match)
}

func (r *holdRecord) lastHolder() holderSample {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.holders) == 0 {
		return holderSample{}
	}
	return r.holders[len(r.holders)-1]
}

func (r *holdRecord) lastHeld() heldSample {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.held) == 0 {
		return heldSample{status: metav1.ConditionUnknown, reason: "Absent"}
	}
	return r.held[len(r.held)-1]
}

func (r *holdRecord) holdersBetween(from, to time.Time) []holderSample {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []holderSample
	for _, h := range r.holders {
		if h.at.After(from) && h.at.Before(to) {
			out = append(out, h)
		}
	}
	return out
}

func (r *holdRecord) heldAfter(from time.Time) []heldSample {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []heldSample
	for _, h := range r.held {
		if h.at.After(from) {
			out = append(out, h)
		}
	}
	return out
}

func (r *holdRecord) unsettled(from, to time.Time) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []string
	for i, s := range r.nodes {
		until := to
		for _, next := range r.nodes[i+1:] {
			if next.node == s.node {
				until = next.at
				break
			}
		}
		if s.at.After(to) || !until.After(from) {
			continue
		}
		if s.ready == corev1.ConditionUnknown || len(s.unsettledTaints()) > 0 {
			out = append(out, fmt.Sprintf("node %s was Ready=%s with taints %v from %s to %s", s.node, s.ready, s.taints, stamp(s.at), stamp(until)))
		}
	}
	return out
}

func (r *holdRecord) podsNow() map[types.UID]podSample {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[types.UID]podSample, len(r.pods))
	for uid, p := range r.pods {
		out[uid] = *p
	}
	return out
}

func podsOn(pods map[types.UID]podSample, node, app, owner string) []types.UID {
	var out []types.UID
	for uid, p := range pods {
		if p.node == node && (app == "" || p.app == app) && (owner == "" || p.owner == owner) && p.gone.IsZero() {
			out = append(out, uid)
		}
	}
	return out
}

func names(pods map[types.UID]podSample, uids []types.UID) []string {
	var out []string
	for _, uid := range uids {
		out = append(out, pods[uid].name)
	}
	slices.Sort(out)
	return out
}
