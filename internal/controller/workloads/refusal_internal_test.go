// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package workloads

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

const (
	allReplicas   = "every replica of kube-apiserver, kube-controller-manager and kube-scheduler is ready"
	keptElsewhere = keeps + "ConfigMap dev-egress by ConfigMap other (UID 7a3c); Workloads writes none of them until they are gone: the garbage collector deletes a previous ControlPlane's, any other owner's need a person"
	refused       = refuses + "Deployment dev-kube-scheduler" + inTheLog
	invalid       = notWritten + "jwt[0].issuer.url: Invalid value"
)

func reporting(status metav1.ConditionStatus, reason, message string) *lodiav1alpha1.ControlPlane {
	cp := &lodiav1alpha1.ControlPlane{ObjectMeta: metav1.ObjectMeta{Generation: 2}}
	cp.Status.Target = &lodiav1alpha1.Target{OperatorRevision: "v0.0.0+granted"}
	cp.Status.Conditions = []metav1.Condition{{Type: lodiav1alpha1.ConditionAPIServerReady, Status: status, Reason: reason, Message: message, ObservedGeneration: 1}}
	return cp
}

func describing(cp *lodiav1alpha1.ControlPlane) *lodiav1alpha1.ControlPlane {
	cp.Status.Conditions[0].ObservedGeneration = cp.Generation
	return cp
}

func counting() (metav1.Condition, error) {
	return newCondition(metav1.ConditionTrue, lodiav1alpha1.ReasonReady, allReplicas), nil
}

// A wait that tries nothing sends nothing, so it keeps a condition that holds a refusal as it is, True or False, while the count agrees with its status, one that a pass past the wait tries again keeps a condition it counts that is not True, and a wait keeps a condition of an older generation that it does not count; otherwise it reports the count at the generation the condition names, or itself, and says which it did.
func TestAWaitKeepsARefusalWhileTheCountAgrees(t *testing.T) {
	waiting := newCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForDatastore, "DatastoreReady is not True for generation 2")
	down := func() (metav1.Condition, error) {
		return newCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonUnavailable, "kube-apiserver has 0 of 1 replicas ready"), nil
	}
	for _, c := range []struct {
		name     string
		previous *lodiav1alpha1.ControlPlane
		retried  bool
		count    func() (metav1.Condition, error)
		kept     bool
		waits    bool
	}{
		{"another owner's object", reporting(metav1.ConditionTrue, lodiav1alpha1.ReasonObjectsControlledElsewhere, allReplicas+"; "+keptElsewhere), false, counting, true, false},
		{"a refused object", reporting(metav1.ConditionTrue, lodiav1alpha1.ReasonRefused, allReplicas+"; "+refused), false, counting, true, false},
		{"a refused object that left no Deployment", reporting(metav1.ConditionFalse, lodiav1alpha1.ReasonRefused, refused), false, down, true, false},
		{"a refused object that left no Deployment at the generation it describes", describing(reporting(metav1.ConditionFalse, lodiav1alpha1.ReasonRefused, refused)), false, down, true, false},
		{"the authentication configuration", reporting(metav1.ConditionTrue, lodiav1alpha1.ReasonAuthenticationInvalid, allReplicas+"; "+invalid+servesNone), false, counting, true, false},
		{"a write no dry run can try", reporting(metav1.ConditionTrue, lodiav1alpha1.ReasonRefusedBeforeHold, allReplicas+"; the grant's write of Deployment dev-kube-scheduler cannot be tried"), false, counting, true, false},
		{"a refusal over what no longer serves", reporting(metav1.ConditionTrue, lodiav1alpha1.ReasonRefused, allReplicas+"; "+refused), false, down, false, false},
		{"availability", reporting(metav1.ConditionFalse, lodiav1alpha1.ReasonUnavailable, "kube-apiserver has 0 of 1 replicas ready"), false, counting, false, false},
		{"a wait of an older generation", reporting(metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForCertificates, "CertificatesReady is not True for generation 1"), true, counting, true, false},
		{"a wait at the generation it describes", describing(reporting(metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForCertificates, "CertificatesReady is not True for generation 2")), true, counting, false, true},
		{"a held wait over what does not serve", reporting(metav1.ConditionFalse, lodiav1alpha1.ReasonUnavailable, "kube-apiserver has 0 of 1 replicas ready; "+refused), true, counting, true, false},
		{"a held wait over a refused object that left no Deployment", reporting(metav1.ConditionFalse, lodiav1alpha1.ReasonRefused, refused), true, counting, true, false},
		{"a held wait over a refusal over what no longer serves", reporting(metav1.ConditionTrue, lodiav1alpha1.ReasonRefused, allReplicas+"; "+refused), true, down, false, false},
		{"a held wait over availability that no longer serves", reporting(metav1.ConditionTrue, lodiav1alpha1.ReasonReady, allReplicas), true, down, false, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, asIs, err := keep(c.previous, waiting, c.retried, c.count)
			if err != nil {
				t.Fatal(err)
			}
			want := c.previous.Status.Conditions[0]
			switch {
			case c.waits:
				want = waiting
			case !c.kept:
				want, _ = c.count()
				want.ObservedGeneration = 1
			}
			if *got != want || asIs != c.kept {
				t.Errorf("APIServerReady is %+v, kept as it is %t, want %+v, %t", *got, asIs, want, c.kept)
			}
		})
	}
}

// A held pass that tries nothing counts at the generation APIServerReady names, keeps one of an older generation that reports no availability but a refusal, whose object the pass created, and waits for the grant otherwise; a held wait that renders past its inputs creates nothing and counts over a refusal of an older generation too.
func TestAHeldPassReportsWhatRunsAndCarriesNothing(t *testing.T) {
	r := &Reconciler{Revision: "v0.0.1+running"}
	holding := r.holding(reporting(metav1.ConditionTrue, lodiav1alpha1.ReasonReady, allReplicas))
	for _, c := range []struct {
		name       string
		previous   *lodiav1alpha1.ControlPlane
		waited     bool
		want       metav1.Condition
		generation int64
		asIs       bool
	}{
		{"availability", reporting(metav1.ConditionTrue, lodiav1alpha1.ReasonReady, allReplicas), false, newCondition(metav1.ConditionTrue, lodiav1alpha1.ReasonReady, allReplicas), 1, false},
		{"availability under a refusal", reporting(metav1.ConditionTrue, lodiav1alpha1.ReasonRefused, allReplicas+"; "+refused), false, newCondition(metav1.ConditionTrue, lodiav1alpha1.ReasonReady, allReplicas), 1, false},
		{"a wait of an older generation", reporting(metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForDatastore, "DatastoreReady is not True for generation 1"), false,
			newCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForDatastore, "DatastoreReady is not True for generation 1"), 1, true},
		{"a wait at the generation it describes", describing(reporting(metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForDatastore, "DatastoreReady is not True for generation 2")), false, holding, 0, false},
		{"a refusal of an older generation that found no Deployment", reporting(metav1.ConditionFalse, lodiav1alpha1.ReasonRefused, refused), false, holding, 0, false},
		{"another owner's object of an older generation that left no Deployment", reporting(metav1.ConditionFalse, lodiav1alpha1.ReasonObjectsControlledElsewhere, keptElsewhere), false, holding, 0, false},
		{"a held wait over a refusal of an older generation that found no Deployment", reporting(metav1.ConditionFalse, lodiav1alpha1.ReasonRefused, refused), true, newCondition(metav1.ConditionTrue, lodiav1alpha1.ReasonReady, allReplicas), 1, false},
		{"the authentication configuration over the grant's wait", describing(reporting(metav1.ConditionFalse, lodiav1alpha1.ReasonAuthenticationInvalid, holding.Message+"; "+invalid)), false, holding, 0, false},
		{"a held wait over the authentication configuration over the grant's wait", describing(reporting(metav1.ConditionFalse, lodiav1alpha1.ReasonAuthenticationInvalid, holding.Message+"; "+invalid)), true, holding, 0, false},
		{"a held wait over a refused dry run over the grant's wait", describing(reporting(metav1.ConditionFalse, lodiav1alpha1.ReasonRefused, holding.Message+"; the grant's write of Deployment dev-kube-scheduler would be refused: the API server refuses the dry run"+inTheLog)), true, holding, 0, false},
		{"a held wait over a refused creation at the generation it describes", describing(reporting(metav1.ConditionFalse, lodiav1alpha1.ReasonRefused, refused)), true, holding, 0, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, asIs, err := r.heldAvailability(c.previous, counting, c.waited)
			if err != nil {
				t.Fatal(err)
			}
			c.want.ObservedGeneration = c.generation
			if *got != c.want || asIs != c.asIs {
				t.Errorf("APIServerReady is %+v, kept as it is %t, want %+v, %t", *got, asIs, c.want, c.asIs)
			}
		})
	}
}

// A trial ends a condition with the writes the API server refuses by dry run, under Refused, then those no dry run could try, under RefusedBeforeHold unless another refusal stands, and a False keeps its reason.
func TestATrialEndsTheConditionWithWhatItFound(t *testing.T) {
	const wouldBe = "the grant's write of ConfigMap dev-egress-selector, Deployment dev-kube-apiserver would be refused: the API server refuses the dry run" + inTheLog
	const untried = "the grant's write of NetworkPolicy dev-kube-scheduler cannot be tried: admission webhook \"probe.lodia.io\" does not support dry run, admission webhook \"other.lodia.io\" does not support dry run"
	ready := newCondition(metav1.ConditionTrue, lodiav1alpha1.ReasonReady, allReplicas)
	down := newCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonUnavailable, "kube-apiserver has 0 of 1 replicas ready")
	authenticating := newCondition(metav1.ConditionTrue, lodiav1alpha1.ReasonAuthenticationInvalid, allReplicas+"; "+invalid)
	refusing := &trial{refused: []string{"ConfigMap dev-egress-selector", "Deployment dev-kube-apiserver"}}
	untrying := &trial{untried: []string{"NetworkPolicy dev-kube-scheduler"}, webhooks: []string{"probe.lodia.io", "other.lodia.io"}}
	both := &trial{refused: refusing.refused, untried: untrying.untried, webhooks: untrying.webhooks}
	for _, c := range []struct {
		name  string
		trial *trial
		c     metav1.Condition
		want  metav1.Condition
	}{
		{"none", nil, ready, ready},
		{"nothing found", &trial{}, ready, ready},
		{"refused", refusing, ready, newCondition(metav1.ConditionTrue, lodiav1alpha1.ReasonRefused, allReplicas+"; "+wouldBe)},
		{"untried", untrying, ready, newCondition(metav1.ConditionTrue, lodiav1alpha1.ReasonRefusedBeforeHold, allReplicas+"; "+untried)},
		{"both", both, ready, newCondition(metav1.ConditionTrue, lodiav1alpha1.ReasonRefused, allReplicas+"; "+wouldBe+"; "+untried)},
		{"untried beside the authentication configuration", untrying, authenticating, newCondition(metav1.ConditionTrue, lodiav1alpha1.ReasonAuthenticationInvalid, allReplicas+"; "+invalid+"; "+untried)},
		{"refused over what does not serve", both, down, newCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonUnavailable, down.Message+"; "+wouldBe+"; "+untried)},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := c.trial.on(c.c); got != c.want {
				t.Errorf("the trial gives %+v, want %+v", got, c.want)
			}
		})
	}
}

// What a held pass or a held wait found ends a condition it builds anew: over the grant's wait a refusal takes its own reason under False, the authentication configuration's first, and a write no dry run can try alone leaves the wait; any other condition takes them as a trial gives them.
func TestOverTheGrantsWaitARefusalTakesItsOwnReason(t *testing.T) {
	r := &Reconciler{Revision: "v0.0.1+running"}
	holding := r.holding(reporting(metav1.ConditionTrue, lodiav1alpha1.ReasonReady, allReplicas))
	const wouldBe = "the grant's write of Deployment dev-kube-scheduler would be refused: the API server refuses the dry run" + inTheLog
	const untried = "the grant's write of NetworkPolicy dev-kube-scheduler cannot be tried: admission webhook \"probe.lodia.io\" does not support dry run"
	refusing := &trial{refused: []string{"Deployment dev-kube-scheduler"}}
	untrying := &trial{untried: []string{"NetworkPolicy dev-kube-scheduler"}, webhooks: []string{"probe.lodia.io"}}
	both := &trial{refused: refusing.refused, untried: untrying.untried, webhooks: untrying.webhooks}
	down := newCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonUnavailable, "kube-apiserver has 0 of 1 replicas ready")
	waiting := newCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForCertificates, "CertificatesReady is not True for generation 2")
	for _, c := range []struct {
		name    string
		trial   *trial
		refused string
		c       metav1.Condition
		want    metav1.Condition
	}{
		{"nothing found over the grant's wait", &trial{}, "", holding, holding},
		{"a refused dry run over the grant's wait", refusing, "", holding, newCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonRefused, holding.Message+"; "+wouldBe)},
		{"the authentication configuration over the grant's wait", &trial{}, invalid, holding, newCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonAuthenticationInvalid, holding.Message+"; "+invalid)},
		{"both over the grant's wait", both, invalid, holding, newCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonAuthenticationInvalid, holding.Message+"; "+invalid+"; "+wouldBe+"; "+untried)},
		{"a write no dry run can try over the grant's wait", untrying, "", holding, newCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForTarget, holding.Message+"; "+untried)},
		{"a refused dry run over what serves", refusing, "", newCondition(metav1.ConditionTrue, lodiav1alpha1.ReasonReady, allReplicas), newCondition(metav1.ConditionTrue, lodiav1alpha1.ReasonRefused, allReplicas+"; "+wouldBe)},
		{"a refused dry run over what does not serve", refusing, "", down, newCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonUnavailable, down.Message+"; "+wouldBe)},
		{"a refused dry run over another wait", refusing, "", waiting, newCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForCertificates, waiting.Message+"; "+wouldBe)},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := c.trial.over(c.c, c.refused); got != c.want {
				t.Errorf("the pass reports %+v, want %+v", got, c.want)
			}
		})
	}
}

// A refused Service this ControlPlane does not control, an orphan whose adoption the API server refuses, gives LoadBalancerReady the refusal alone, never its load balancer.
func TestARefusedServiceOfNoControlPlaneGivesTheRefusalAlone(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	cp := balancerPlane("", "", "10.96.0.0/16")
	cp.Name, cp.Namespace, cp.UID = "dev", "team-a", "uid-1"
	orphan := ingresses("203.0.113.10")
	orphan.Namespace = cp.Namespace
	refused := refusedObjects([]string{"Service dev-kube-apiserver"})
	r := &Reconciler{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(orphan).Build()}
	got, err := r.refusedBalancer(context.Background(), cp, refused)
	if err != nil {
		t.Fatal(err)
	}
	if want := as(lodiav1alpha1.ConditionLoadBalancerReady, refused); got == nil || *got != *want {
		t.Errorf("LoadBalancerReady is %+v, want %+v", got, want)
	}
}
