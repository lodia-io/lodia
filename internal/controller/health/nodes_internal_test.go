// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"maps"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

func TestStatusNodesFollowsTheKubeletsVersions(t *testing.T) {
	kubelets := func(ready corev1.ConditionStatus, versions ...string) []node {
		var nodes []node
		for _, v := range versions {
			nodes = append(nodes, node{ready: ready, kubelet: v})
		}
		return nodes
	}
	status := func(total, ready, outdated int32, oldest string) *lodiav1alpha1.NodesStatus {
		return &lodiav1alpha1.NodesStatus{Count: lodiav1alpha1.Count{Total: total, Ready: ready}, OutdatedKubelets: outdated, OldestKubelet: oldest}
	}
	for _, c := range []struct {
		name    string
		nodes   []node
		running string
		want    *lodiav1alpha1.NodesStatus
	}{
		{"no node", nil, "v1.35.1", status(0, 0, 0, "")},
		{"across minors", kubelets(corev1.ConditionTrue, "v1.35.0", "v1.33.12", "v1.34.9"), "", status(3, 3, 0, "v1.33.12")},
		{"across patches, not as strings", kubelets(corev1.ConditionTrue, "v1.35.10", "v1.35.2", "v1.35.9"), "", status(3, 3, 0, "v1.35.2")},
		{"a pre-release is older than its release", kubelets(corev1.ConditionTrue, "v1.35.0", "v1.35.0-rc.1"), "", status(2, 2, 0, "v1.35.0-rc.1")},
		{"a distribution's suffix", kubelets(corev1.ConditionTrue, "v1.34.2+k3s1", "v1.35.0-eks-5e0fdde"), "", status(2, 2, 0, "v1.34.2+k3s1")},
		{"an unparsable version is left out", kubelets(corev1.ConditionTrue, "not a version", "v1.35.0", ""), "", status(3, 3, 0, "v1.35.0")},
		{"no parsable version", kubelets(corev1.ConditionTrue, "garbage", ""), "", status(2, 2, 0, "")},
		{"only the nodes Ready=True are ready", append(kubelets(corev1.ConditionTrue, "v1.35.0"), append(kubelets(corev1.ConditionFalse, "v1.35.0"), kubelets(corev1.ConditionUnknown, "v1.35.0")...)...), "", status(3, 1, 0, "v1.35.0")},
		{"an older minor is outdated", kubelets(corev1.ConditionTrue, "v1.34.9", "v1.33.1"), "v1.35.1", status(2, 2, 2, "v1.33.1")},
		{"the same minor is not, whatever its patch", kubelets(corev1.ConditionTrue, "v1.35.0", "v1.35.1", "v1.35.2"), "v1.35.1", status(3, 3, 0, "v1.35.0")},
		{"nor is a pre-release of the same minor", kubelets(corev1.ConditionTrue, "v1.35.0-rc.1"), "v1.35.1", status(1, 1, 0, "v1.35.0-rc.1")},
		{"nor a newer minor", kubelets(corev1.ConditionTrue, "v1.36.0"), "v1.35.1", status(1, 1, 0, "v1.36.0")},
		{"an unparsable kubelet is not outdated", kubelets(corev1.ConditionTrue, "garbage"), "v1.35.1", status(1, 1, 0, "")},
		{"none is outdated while status.kubernetes.version is absent", kubelets(corev1.ConditionTrue, "v1.30.0"), "", status(1, 1, 0, "v1.30.0")},
		{"or unparsable", kubelets(corev1.ConditionTrue, "v1.30.0"), "garbage", status(1, 1, 0, "v1.30.0")},
	} {
		if got := nodesStatus(c.nodes, c.running, false); !equality.Semantic.DeepEqual(got, c.want) {
			t.Errorf("%s: status.nodes is %+v, want %+v", c.name, got, c.want)
		}
	}
}

func TestOnlyAChangeOfWhetherAccessReadyIsTrueBringsAPassOfTheNodes(t *testing.T) {
	access := func(status metav1.ConditionStatus, host string) *lodiav1alpha1.ControlPlane {
		return &lodiav1alpha1.ControlPlane{Status: lodiav1alpha1.ControlPlaneStatus{
			Conditions: []metav1.Condition{{Type: lodiav1alpha1.ConditionAccessReady, Status: status}},
			Endpoint:   &lodiav1alpha1.EndpointStatus{APIServer: lodiav1alpha1.HostPort{Host: host, Port: 443}},
		}}
	}
	for _, c := range []struct {
		name          string
		before, after *lodiav1alpha1.ControlPlane
		want          bool
	}{
		{"a new endpoint", access(metav1.ConditionTrue, "a.example.com"), access(metav1.ConditionTrue, "b.example.com"), false},
		{"False to Unknown", access(metav1.ConditionFalse, "a.example.com"), access(metav1.ConditionUnknown, "a.example.com"), false},
		{"True to False", access(metav1.ConditionTrue, "a.example.com"), access(metav1.ConditionFalse, "a.example.com"), true},
		{"missing to True", &lodiav1alpha1.ControlPlane{}, access(metav1.ConditionTrue, "a.example.com"), true},
	} {
		if got := accessChanged().Update(event.UpdateEvent{ObjectOld: c.before, ObjectNew: c.after}); got != c.want {
			t.Errorf("%s brings a pass: %t, want %t", c.name, got, c.want)
		}
	}
}

func TestANewGenerationBringsAPassOfTheNodes(t *testing.T) {
	before := &lodiav1alpha1.ControlPlane{ObjectMeta: metav1.ObjectMeta{Generation: 1}}
	after := before.DeepCopy()
	if passInputs().Update(event.UpdateEvent{ObjectOld: before, ObjectNew: after}) {
		t.Error("an update of neither the generation nor AccessReady brought a pass")
	}
	after.Generation = 2
	if !passInputs().Update(event.UpdateEvent{ObjectOld: before, ObjectNew: after}) {
		t.Error("a new generation brought no pass, so an unpaused ControlPlane is never sampled again")
	}
}

func TestADeletedControlPlaneIsForgotten(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := lodiav1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	key := types.NamespacedName{Namespace: "team-a", Name: "dev"}
	r := &NodesReconciler{
		Client:  fake.NewClientBuilder().WithScheme(scheme).Build(),
		Clock:   clock.RealClock{},
		samples: map[types.NamespacedName]*sampling{key: {uid: "gone"}},
		clients: map[types.NamespacedName]*tenantClients{key: {}},
	}
	result, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: key})
	if err != nil || result.RequeueAfter != 0 {
		t.Errorf("Reconcile = %+v, %v, want no requeue and no error", result, err)
	}
	if len(r.samples) != 0 || len(r.clients) != 0 {
		t.Errorf("kept %d samples and %d clients of a deleted ControlPlane", len(r.samples), len(r.clients))
	}
}

func TestOnlyTheLeasesListedAreRemembered(t *testing.T) {
	lease := func(name, resourceVersion string) leaseVersion {
		return leaseVersion{name, versionOf(resourceVersion)}
	}
	first := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	later := first.Add(time.Minute)
	known := seen(nil, []leaseVersion{lease("a", "1"), lease("b", "1")}, first)
	got := seen(known, []leaseVersion{lease("a", "1"), lease("c", "7")}, later)
	want := map[string]renewal{
		"a": {version: versionOf("1"), at: first},
		"c": {version: versionOf("7"), at: later},
	}
	if !maps.Equal(got, want) {
		t.Errorf("seen is %+v, want %+v", got, want)
	}
	if again := seen(got, []leaseVersion{lease("a", "2"), lease("c", "7")}, later.Add(time.Minute)); again["a"].at != later.Add(time.Minute) || again["c"].at != later {
		t.Errorf("a changed Lease and an unchanged one are seen at %+v", again)
	}
}

func memberOf(name, kubelet string, ready corev1.ConditionStatus) nodeObject {
	var n nodeObject
	n.Metadata.Name = name
	n.Status.NodeInfo.KubeletVersion = kubelet
	n.Status.Conditions.items = []nodeCondition{{Type: corev1.NodeReady, Status: ready}}
	return n
}

func TestALeaseOrANodeNamedBeyondTheBoundIsNotKept(t *testing.T) {
	sizedForTheBounds(t)
	atBound := strings.Repeat("a", maxName)
	lease := func(name string) leaseItem {
		var l leaseItem
		l.Metadata.Name, l.Metadata.ResourceVersion = name, "1"
		return l
	}
	leases := leaseVersions([]leaseItem{lease(atBound), lease(atBound + "a"), lease(strings.Repeat("a", 80000)), lease("short")})
	if len(leases) != 2 || leases[0].name != atBound || leases[1].name != "short" {
		t.Errorf("leaseVersions kept %d Leases, want the one of %d bytes and the short one", len(leases), maxName)
	}
	nodes, end := summarize([]nodeObject{memberOf(atBound, "", ""), memberOf(atBound+"a", "", ""), memberOf(strings.Repeat("a", 80000), "", ""), memberOf("short", "", "")})
	if end != listEnded || len(nodes) != 2 || nodes[0].name != atBound || nodes[1].name != "short" {
		t.Errorf("summarize kept %d Nodes, want the one of %d bytes and the short one", len(nodes), maxName)
	}
}

func TestAChangeOfAResourceVersionIsSeenWhateverItsFormat(t *testing.T) {
	long := strings.Repeat("9", 80000)
	first := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	later := first.Add(time.Minute)
	for _, c := range []struct {
		name          string
		before, after string
		changed       bool
	}{
		{"the same number", "7", "7", false},
		{"another number", "7", "8", true},
		{"a longer number", "9", "99", true},
		{"no version and one", "", "1", true},
		{"a version that is no number", "a/b", "a/b", false},
		{"another that is no number", "a/b", "a/c", true},
		{"the same version of 80 KB", long, long, false},
		{"versions of 80 KB that differ in their last byte", long, long[:len(long)-1] + "8", true},
	} {
		known := seen(nil, []leaseVersion{{"a", versionOf(c.before)}}, first)
		got := seen(known, []leaseVersion{{"a", versionOf(c.after)}}, later)
		if want := map[bool]time.Time{false: first, true: later}[c.changed]; !got["a"].at.Equal(want) {
			t.Errorf("%s: the Lease was first seen at %v, want %v", c.name, got["a"].at, want)
		}
	}
}

func TestASummaryKeepsNoKubeletVersionOrReadyStatusBeyondItsBound(t *testing.T) {
	sizedForTheBounds(t)
	atBound := "v1.30.0-" + strings.Repeat("x", maxKubeletVersion-len("v1.30.0-"))
	got, _ := summarize([]nodeObject{
		memberOf("long", "v1.30.0-"+strings.Repeat("x", 80000), corev1.ConditionTrue),
		memberOf("over", atBound+"x", corev1.ConditionTrue),
		memberOf("at", atBound, corev1.ConditionTrue),
		memberOf("odd", "v1.35.0", corev1.ConditionStatus(strings.Repeat("x", 80000))),
		memberOf("unknown", "v1.35.0", corev1.ConditionUnknown),
		memberOf("false", "v1.35.0", corev1.ConditionFalse),
	})
	for i, want := range []node{
		{name: "long", ready: corev1.ConditionTrue},
		{name: "over", ready: corev1.ConditionTrue},
		{name: "at", ready: corev1.ConditionTrue, kubelet: atBound},
		{name: "odd", kubelet: "v1.35.0"},
		{name: "unknown", ready: corev1.ConditionUnknown, kubelet: "v1.35.0"},
		{name: "false", ready: corev1.ConditionFalse, kubelet: "v1.35.0"},
	} {
		if got[i] != want {
			t.Errorf("summary %d keeps a name of %d bytes, a Ready status of %d and a kubelet version of %d, want %q, %q and %q", i, len(got[i].name), len(got[i].ready), len(got[i].kubelet), want.name, want.ready, want.kubelet)
		}
	}
}

func TestAKubeletVersionBeyondTheBoundCountsLikeAnUnparsableOne(t *testing.T) {
	sizedForTheBounds(t)
	counted := func(kubelet string) *lodiav1alpha1.NodesStatus {
		nodes, _ := summarize([]nodeObject{memberOf("a", kubelet, corev1.ConditionTrue), memberOf("b", "v1.34.2", corev1.ConditionTrue)})
		return nodesStatus(nodes, "v1.35.1", false)
	}
	if long, unparsable := counted("v1.30.0-"+strings.Repeat("x", 80000)), counted("garbage"); !equality.Semantic.DeepEqual(long, unparsable) {
		t.Errorf("a kubelet version of 80 KB counts %+v, one that does not parse %+v", *long, *unparsable)
	}
}
