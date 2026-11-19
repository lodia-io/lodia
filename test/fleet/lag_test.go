// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package fleet

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"sigs.k8s.io/controller-runtime/pkg/client"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/operator"
	"go.lodia.io/lodia/internal/statusapply"
)

const lag = time.Second

var writtenBy = map[string][]string{
	"pki":         {lodiav1alpha1.ConditionCertificatesReady, lodiav1alpha1.ConditionCAValidityAcceptable, lodiav1alpha1.ConditionAccessReady},
	"datastore":   {lodiav1alpha1.ConditionDatastoreReady, lodiav1alpha1.ConditionBackupReady},
	"workloads":   {lodiav1alpha1.ConditionAPIServerReady, lodiav1alpha1.ConditionRoutesReady},
	"tenant":      {lodiav1alpha1.ConditionAddonsApplied, lodiav1alpha1.ConditionAddonsAvailable},
	"health":      {lodiav1alpha1.ConditionReachable, lodiav1alpha1.ConditionControllersLeading},
	"healthNodes": {lodiav1alpha1.ConditionControllerManagerHeld},
}

func fields(status *lodiav1alpha1.ControlPlaneStatus) map[string]any {
	return map[string]any{
		"pki credentials":      status.Credentials,
		"workloads endpoint":   status.Endpoint,
		"workloads components": status.Components,
		"tenant addons":        status.Addons,
		"datastore datastore":  status.Datastore,
		"lifecycle target":     status.Target,
	}
}

type older struct {
	last       map[string]any
	generation map[string]int64
	past       map[string]bool
	set        map[string]bool
	found      []string

	mu      sync.Mutex
	latest  *lodiav1alpha1.ControlPlane
	updated chan struct{}
}

func (o *older) changed(key string, value any) bool {
	before, seen := o.last[key]
	o.last[key] = value
	return !seen || !equality.Semantic.DeepEqual(before, value)
}

func (o *older) see(fresh *lodiav1alpha1.ControlPlane) {
	for writer, conditionTypes := range writtenBy {
		for _, conditionType := range conditionTypes {
			c := meta.FindStatusCondition(fresh.Status.Conditions, conditionType)
			if c == nil || !o.changed(conditionType, *c) {
				continue
			}
			first := statusapply.FirstPass(fresh, conditionType)
			atFirst := c.Reason == first.Reason && c.Message == first.Message
			switch {
			case c.ObservedGeneration < o.generation[conditionType]:
				o.found = append(o.found, fmt.Sprintf("resourceVersion %s: %s wrote %s at generation %d after it described %d: %s %s %q", fresh.ResourceVersion, writer, conditionType, c.ObservedGeneration, o.generation[conditionType], c.Status, c.Reason, c.Message))
			case atFirst && o.past[conditionType]:
				o.found = append(o.found, fmt.Sprintf("resourceVersion %s: %s wrote %s back to its first pass at generation %d", fresh.ResourceVersion, writer, conditionType, c.ObservedGeneration))
			}
			o.generation[conditionType] = max(o.generation[conditionType], c.ObservedGeneration)
			o.past[conditionType] = o.past[conditionType] || !atFirst
		}
	}
	for key, value := range fields(&fresh.Status) {
		if !o.changed(key, value) {
			continue
		}
		empty := equality.Semantic.DeepEqual(value, zeroOf(key))
		if empty && o.set[key] {
			o.found = append(o.found, fmt.Sprintf("resourceVersion %s: %s took back status.%s", fresh.ResourceVersion, strings.Fields(key)[0], strings.Fields(key)[1]))
		}
		o.set[key] = o.set[key] || !empty
	}
	o.mu.Lock()
	o.latest = fresh
	close(o.updated)
	o.updated = make(chan struct{})
	o.mu.Unlock()
}

// until waits for an update of the ControlPlane, as the API server holds it,
// that done takes.
func (o *older) until(t *testing.T, what string, done func(*lodiav1alpha1.ControlPlane) bool) {
	t.Helper()
	deadline := time.After(2 * time.Minute)
	for {
		o.mu.Lock()
		latest, updated := o.latest, o.updated
		o.mu.Unlock()
		if latest != nil && done(latest) {
			return
		}
		select {
		case <-updated:
		case <-deadline:
			t.Fatalf("no update of the ControlPlane within 2 minutes in which %s", what)
		}
	}
}

func zeroOf(key string) any {
	return fields(&lodiav1alpha1.ControlPlaneStatus{})[key]
}

func describes(conditionType string, generation int64) func(*lodiav1alpha1.ControlPlane) bool {
	return func(cp *lodiav1alpha1.ControlPlane) bool {
		c := meta.FindStatusCondition(cp.Status.Conditions, conditionType)
		return c != nil && c.ObservedGeneration == generation
	}
}

func reports(conditionType, reason string) func(*lodiav1alpha1.ControlPlane) bool {
	return func(cp *lodiav1alpha1.ControlPlane) bool {
		c := meta.FindStatusCondition(cp.Status.Conditions, conditionType)
		return c != nil && c.Reason == reason
	}
}

func TestNoControllerWritesAStatusOlderThanItsOwnLast(t *testing.T) {
	dir := t.TempDir()
	f := startFleet(t, dir)
	f.settings.Lag = lag
	f.op = startOperator(t, dir, f.cfg, f.settings)
	watching, err := client.NewWithWatch(f.cfg, client.Options{Scheme: operator.Scheme()})
	if err != nil {
		t.Fatal(err)
	}
	ns := namespaceOf(1)
	updates, err := watching.Watch(t.Context(), &lodiav1alpha1.ControlPlaneList{}, client.InNamespace(ns))
	if err != nil {
		t.Fatal(err)
	}
	defer updates.Stop()
	o := &older{last: map[string]any{}, generation: map[string]int64{}, past: map[string]bool{}, set: map[string]bool{}, updated: make(chan struct{})}
	seen := make(chan struct{})
	var stopped atomic.Bool
	go func() {
		defer close(seen)
		for event := range updates.ResultChan() {
			if event.Type == watch.Error && !stopped.Load() {
				t.Errorf("watch: %v", apierrors.FromObject(event.Object))
				return
			}
			if fresh, ok := event.Object.(*lodiav1alpha1.ControlPlane); ok && event.Type != watch.Deleted {
				o.see(fresh)
			}
		}
	}()

	f.createOnly(1, 1)
	cp := &lodiav1alpha1.ControlPlane{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: controlPlaneName}}
	hold := true
	edit := func() int64 {
		t.Helper()
		hold = !hold
		patch := fmt.Appendf(nil, `{"spec":{"health":{"holdControllerManager":%t}}}`, hold)
		if err := f.admin.Patch(t.Context(), cp, client.RawPatch(types.MergePatchType, patch)); err != nil {
			t.Fatal(err)
		}
		return cp.Generation
	}
	for i := range 40 {
		edit()
		time.Sleep(lag / 4 * time.Duration(1+i%4))
	}
	f.waitReady(time.Now(), 5*time.Minute)

	service := &corev1.Service{}
	if err := f.admin.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: lodiav1alpha1.APIServerServiceName(controlPlaneName)}, service); err != nil {
		t.Fatal(err)
	}
	owners := service.OwnerReferences
	setOwners := func(refs []metav1.OwnerReference) {
		t.Helper()
		body, err := json.Marshal(map[string]any{"metadata": map[string]any{"ownerReferences": refs}})
		if err != nil {
			t.Fatal(err)
		}
		if err := f.admin.Patch(t.Context(), service, client.RawPatch(types.MergePatchType, body)); err != nil {
			t.Fatal(err)
		}
	}
	for range 2 {
		generation := edit()
		o.until(t, fmt.Sprintf("Workloads writes RoutesReady at generation %d", generation), describes(lodiav1alpha1.ConditionRoutesReady, generation))
		setOwners([]metav1.OwnerReference{{APIVersion: "v1", Kind: "ConfigMap", Name: "elsewhere", UID: "00000000-0000-4000-8000-00000000e15e", Controller: new(true)}})
		o.until(t, "Workloads finds its Service controlled elsewhere", reports(lodiav1alpha1.ConditionAPIServerReady, lodiav1alpha1.ReasonObjectsControlledElsewhere))
		setOwners(owners)
		f.waitReady(time.Now(), 2*time.Minute)
	}

	stub := f.tenants.tenant(ns)
	stub.slow.Store(true)
	for range 2 {
		generation := edit()
		o.until(t, fmt.Sprintf("PKI writes AccessReady at generation %d", generation), describes(lodiav1alpha1.ConditionAccessReady, generation))
		edit()
		o.until(t, fmt.Sprintf("Tenant writes AddonsAvailable at generation %d", generation), describes(lodiav1alpha1.ConditionAddonsAvailable, generation))
		stub.refusing.Store(true)
		o.until(t, "Tenant is refused", reports(lodiav1alpha1.ConditionAddonsApplied, lodiav1alpha1.ReasonRefused))
		stub.refusing.Store(false)
		f.waitReady(time.Now(), 2*time.Minute)
	}
	stub.slow.Store(false)

	stopped.Store(true)
	updates.Stop()
	<-seen
	if err := f.admin.Get(t.Context(), client.ObjectKeyFromObject(cp), cp); err != nil {
		t.Fatal(err)
	}
	for _, conditionType := range []string{lodiav1alpha1.ConditionAccessReady, lodiav1alpha1.ConditionAPIServerReady, lodiav1alpha1.ConditionAddonsApplied} {
		if c := meta.FindStatusCondition(cp.Status.Conditions, conditionType); c == nil || c.ObservedGeneration != cp.Generation {
			t.Errorf("%s is %+v, not at the last generation %d", conditionType, c, cp.Generation)
		}
	}
	for _, found := range o.found {
		t.Error(found)
	}
}
