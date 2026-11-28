// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package tenant

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

// A held pass whose create the tenant refuses writes the refusal though the rollout read after it fails, and returns both.
func TestAHeldRefusedCreateIsWrittenThoughTheRolloutReadFails(t *testing.T) {
	listed, err := json.Marshal([]entry{configMap, dns})
	if err != nil {
		t.Fatal(err)
	}
	inventory, err := json.Marshal(map[string]any{"data": map[string]string{inventoryKey: string(listed)}})
	if err != nil {
		t.Fatal(err)
	}
	status := func(w http.ResponseWriter, code int, reason metav1.StatusReason) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(metav1.Status{TypeMeta: metav1.TypeMeta{Kind: "Status", APIVersion: "v1"}, Status: metav1.StatusFailure, Code: int32(code), Reason: reason, Message: "the test says so"})
	}
	var deploymentReads atomic.Int32
	api := standingIn(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/configmaps/"+inventoryName):
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(inventory)
		case strings.HasSuffix(r.URL.Path, "/configmaps/coredns") && r.Method == http.MethodGet:
			status(w, http.StatusNotFound, metav1.StatusReasonNotFound)
		case strings.HasSuffix(r.URL.Path, "/configmaps/coredns"):
			status(w, http.StatusUnprocessableEntity, metav1.StatusReasonInvalid)
		case deploymentReads.Add(1) == 1:
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"metadata":{"uid":"7a3c","resourceVersion":"42","managedFields":[{"manager":"` + FieldManager + `","operation":"Apply"}]}}`))
		default:
			http.Error(w, "no Status", http.StatusInternalServerError)
		}
	}))
	objects := []*unstructured.Unstructured{
		{Object: map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"namespace": "kube-system", "name": "coredns"}}},
		{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "metadata": map[string]any{"namespace": "kube-system", "name": "coredns"}}},
	}
	applied := metav1.Condition{Type: lodiav1alpha1.ConditionAddonsApplied, Status: metav1.ConditionTrue, Reason: lodiav1alpha1.ReasonApplied, Message: "2 objects are applied and the inventory is pruned", ObservedGeneration: 1}
	available := metav1.Condition{Type: lodiav1alpha1.ConditionAddonsAvailable, Status: metav1.ConditionTrue, Reason: lodiav1alpha1.ReasonAvailable, Message: "coredns are rolled out", ObservedGeneration: 1}
	cp := &lodiav1alpha1.ControlPlane{ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "dev", Generation: 2}}
	cp.Status.Conditions = []metav1.Condition{applied, available}
	scheme := runtime.NewScheme()
	if err := lodiav1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	management := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cp.DeepCopy()).WithStatusSubresource(cp).Build()
	var written *lodiav1alpha1.ControlPlane
	r := &Reconciler{Reader: management, Revision: "next", Client: interceptor.NewClient(management, interceptor.Funcs{
		SubResourceApply: func(_ context.Context, _ client.Client, _ string, obj runtime.ApplyConfiguration, _ ...client.SubResourceApplyOption) error {
			raw, err := json.Marshal(obj)
			if err != nil {
				return err
			}
			written = &lodiav1alpha1.ControlPlane{}
			return json.Unmarshal(raw, written)
		},
	})}

	if _, err := r.hold(t.Context(), cp, api, objects, applied, available); err == nil {
		t.Error("a held pass whose create the tenant refused returned no error")
	}
	if written == nil {
		t.Fatal("a held pass whose create the tenant refused wrote nothing")
	}
	got := meta.FindStatusCondition(written.Status.Conditions, lodiav1alpha1.ConditionAddonsApplied)
	const refused = "ConfigMap kube-system/coredns is refused by the tenant's API server; the reason is in Lodia's log"
	if got == nil || got.Status != metav1.ConditionFalse || got.Reason != lodiav1alpha1.ReasonRefused || got.Message != refused || got.ObservedGeneration != 1 {
		t.Errorf("AddonsApplied is %+v, want False Refused %q at generation 1", got, refused)
	}
}

func heldTrial(t *testing.T, applied metav1.Condition, entries []entry, manager string, dryRun http.HandlerFunc) (*Reconciler, *lodiav1alpha1.ControlPlane, tenantAPI, *atomic.Int32, *atomic.Int32, **lodiav1alpha1.ControlPlane) {
	t.Helper()
	listed, err := json.Marshal(entries)
	if err != nil {
		t.Fatal(err)
	}
	inventory, err := json.Marshal(map[string]any{"data": map[string]string{inventoryKey: string(listed)}})
	if err != nil {
		t.Fatal(err)
	}
	var requests, dryRuns atomic.Int32
	api := standingIn(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodDelete:
			dryRun(w, r)
		case r.URL.Query().Get("dryRun") == metav1.DryRunAll:
			dryRuns.Add(1)
			dryRun(w, r)
		case strings.HasSuffix(r.URL.Path, "/configmaps/"+inventoryName):
			_, _ = w.Write(inventory)
		default:
			_, _ = w.Write([]byte(`{"metadata":{"uid":"7a3c","resourceVersion":"42","generation":1,"managedFields":[{"manager":"` + manager + `","operation":"Apply"}]},"spec":{"replicas":1},"status":{"observedGeneration":1,"updatedReplicas":1,"availableReplicas":1}}`))
		}
	}))
	available := metav1.Condition{Type: lodiav1alpha1.ConditionAddonsAvailable, Status: metav1.ConditionTrue, Reason: lodiav1alpha1.ReasonAvailable, Message: "coredns are rolled out", ObservedGeneration: 1}
	cp := &lodiav1alpha1.ControlPlane{ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "dev", Generation: 2}}
	cp.Status.Conditions = []metav1.Condition{applied, available}
	cp.Status.Addons = &lodiav1alpha1.Rendered{Version: "v1.37.1", OperatorRevision: "previous"}
	scheme := runtime.NewScheme()
	if err := lodiav1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	management := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cp.DeepCopy()).WithStatusSubresource(cp).Build()
	written := new(*lodiav1alpha1.ControlPlane)
	r := &Reconciler{Reader: management, Revision: "next", Client: interceptor.NewClient(management, interceptor.Funcs{
		SubResourceApply: func(_ context.Context, _ client.Client, _ string, obj runtime.ApplyConfiguration, _ ...client.SubResourceApplyOption) error {
			raw, err := json.Marshal(obj)
			if err != nil {
				return err
			}
			*written = &lodiav1alpha1.ControlPlane{}
			return json.Unmarshal(raw, *written)
		},
	})}
	return r, cp, api, &requests, &dryRuns, written
}

func heldObjects() []*unstructured.Unstructured {
	return []*unstructured.Unstructured{
		{Object: map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"namespace": "kube-system", "name": "coredns"}}},
		{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "metadata": map[string]any{"namespace": "kube-system", "name": "coredns"}}},
	}
}

func heldApplied(status metav1.ConditionStatus, reason string) metav1.Condition {
	return metav1.Condition{Type: lodiav1alpha1.ConditionAddonsApplied, Status: status, Reason: reason, Message: "ConfigMap kube-system/coredns is refused by the tenant's API server; the reason is in Lodia's log", ObservedGeneration: 1}
}

// A held pass whose dry run of the grant's writes gets no answer returns the error and writes no status: no answer is no observation.
func TestAHeldDryRunWithoutAnAnswerChangesNothing(t *testing.T) {
	r, cp, api, _, _, written := heldTrial(t, heldApplied(metav1.ConditionTrue, lodiav1alpha1.ReasonRefused), []entry{configMap, dns}, FieldManager, func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "no Status", http.StatusInternalServerError)
	})
	if _, err := r.hold(t.Context(), cp, api, heldObjects(), cp.Status.Conditions[0], cp.Status.Conditions[1]); err == nil {
		t.Error("a held pass whose dry run got no answer returned no error")
	}
	if *written != nil {
		t.Errorf("a held pass whose dry run got no answer wrote %+v", (*written).Status.Conditions)
	}
}

// A held pass sends no dry run while AddonsApplied holds no refusal, and under one sends one dry run of each object of the render, beside its own reads, which the inventory's and a read of each listed object and of the rollout are.
func TestAHeldPassTriesTheGrantOnlyUnderARefusal(t *testing.T) {
	accept := func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{}`)) }
	for _, c := range []struct {
		reason            string
		requests, dryRuns int32
	}{
		{lodiav1alpha1.ReasonApplied, 4, 0},
		{lodiav1alpha1.ReasonRefused, 7, 2},
	} {
		t.Run(c.reason, func(t *testing.T) {
			r, cp, api, requests, dryRuns, written := heldTrial(t, heldApplied(metav1.ConditionTrue, c.reason), []entry{configMap, dns}, FieldManager, accept)
			if _, err := r.hold(t.Context(), cp, api, heldObjects(), cp.Status.Conditions[0], cp.Status.Conditions[1]); err != nil {
				t.Fatal(err)
			}
			if requests.Load() != c.requests || dryRuns.Load() != c.dryRuns {
				t.Errorf("a held pass over AddonsApplied %s sent %d requests, %d of them dry runs, want %d and %d", c.reason, requests.Load(), dryRuns.Load(), c.requests, c.dryRuns)
			}
			if *written != nil {
				if got := meta.FindStatusCondition((*written).Status.Conditions, lodiav1alpha1.ConditionAddonsApplied); got.Reason != lodiav1alpha1.ReasonApplied {
					t.Errorf("AddonsApplied is %+v, want Applied", got)
				}
			}
		})
	}
}

// A refused dry run of the inventory's first write, which the grant's pass sends before any apply when the render adds an entry, is a refusal of the grant's like any other, not a failure that stops the pass.
func TestAHeldPassReportsARefusedFirstWriteOfTheInventory(t *testing.T) {
	refuse := func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/configmaps/"+inventoryName) {
			_, _ = w.Write([]byte(`{}`))
			return
		}
		w.WriteHeader(http.StatusForbidden)
		_ = json.NewEncoder(w).Encode(metav1.Status{TypeMeta: metav1.TypeMeta{Kind: "Status", APIVersion: "v1"}, Status: metav1.StatusFailure, Code: http.StatusForbidden, Reason: metav1.StatusReasonForbidden, Message: "kept out"})
	}
	r, cp, api, _, _, written := heldTrial(t, heldApplied(metav1.ConditionTrue, lodiav1alpha1.ReasonRefused), []entry{configMap}, FieldManager, refuse)
	if _, err := r.hold(t.Context(), cp, api, heldObjects(), cp.Status.Conditions[0], cp.Status.Conditions[1]); err == nil {
		t.Error("a held pass whose dry run is refused returned no error")
	}
	const want = "the grant's write of ConfigMap kube-system/lodia-inventory would be refused: the dry run is refused by the tenant's API server; the reason is in Lodia's log"
	if *written == nil {
		t.Fatal("a held pass whose dry run is refused wrote nothing")
	}
	if got := meta.FindStatusCondition((*written).Status.Conditions, lodiav1alpha1.ConditionAddonsApplied); got.Status != metav1.ConditionTrue || got.Reason != lodiav1alpha1.ReasonRefused || got.Message != want {
		t.Errorf("AddonsApplied is %+v, want True Refused %q", got, want)
	}
}

// A held pass whose every dry run passes over a refusal ends it, as Applied while the condition was True and as a wait for the grant while it was False, when a listed object is not Lodia's apply.
func TestAHeldPassWhoseDryRunPassesKeepsTheStatusItFound(t *testing.T) {
	accept := func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{}`)) }
	for _, c := range []struct {
		status metav1.ConditionStatus
		reason string
	}{
		{metav1.ConditionTrue, lodiav1alpha1.ReasonApplied},
		{metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForTarget},
	} {
		t.Run(string(c.status), func(t *testing.T) {
			r, cp, api, _, _, written := heldTrial(t, heldApplied(c.status, lodiav1alpha1.ReasonRefused), []entry{configMap, dns}, "kubectl", accept)
			if _, err := r.hold(t.Context(), cp, api, heldObjects(), cp.Status.Conditions[0], cp.Status.Conditions[1]); err != nil {
				t.Fatal(err)
			}
			const want = "the tenant accepts by dry run every write of the running operator revision next; waiting for Lifecycle to grant it"
			if got := meta.FindStatusCondition((*written).Status.Conditions, lodiav1alpha1.ConditionAddonsApplied); got.Status != c.status || got.Reason != c.reason || got.Message != want || got.ObservedGeneration != 1 {
				t.Errorf("AddonsApplied is %+v, want %s %s %q at generation 1", got, c.status, c.reason, want)
			}
		})
	}
}

// A held pass tries the prune of an entry that left the render by a dry run of its delete, as the grant's pass would delete it, and deletes nothing.
func TestAHeldPassTriesAPruneByDryRun(t *testing.T) {
	left := entry{Version: "v1", Kind: "ConfigMap", Namespace: "kube-system", Name: "left-the-render"}
	var deletes []metav1.DeleteOptions
	r, cp, api, _, _, _ := heldTrial(t, heldApplied(metav1.ConditionTrue, lodiav1alpha1.ReasonRefused), []entry{configMap, dns, left}, FieldManager, func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodDelete {
			options := metav1.DeleteOptions{}
			if err := json.NewDecoder(req.Body).Decode(&options); err != nil {
				t.Error(err)
			}
			deletes = append(deletes, options)
			_, _ = w.Write([]byte(`{"kind":"Status","apiVersion":"v1","status":"Success"}`))
			return
		}
		_, _ = w.Write([]byte(`{}`))
	})
	if _, err := r.hold(t.Context(), cp, api, heldObjects(), cp.Status.Conditions[0], cp.Status.Conditions[1]); err != nil {
		t.Fatal(err)
	}
	if len(deletes) != 1 || len(deletes[0].DryRun) != 1 || deletes[0].DryRun[0] != metav1.DryRunAll {
		t.Errorf("the held pass sent the deletes %+v, want one by dry run", deletes)
	}
}

// A held pass whose creations of more than ten listed objects are refused names ten and counts the rest, and once it creates every one, and its dry run of the grant passes, reports them applied with no count left.
func TestAHeldPassThatCreatesEveryRefusedObjectEndsTheRefusal(t *testing.T) {
	var entries []entry
	var objects []*unstructured.Unstructured
	for i := range 12 {
		e := entry{Version: "v1", Kind: "ConfigMap", Namespace: "kube-system", Name: fmt.Sprintf("refused-%02d", i)}
		entries = append(entries, e)
		objects = append(objects, &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"namespace": e.Namespace, "name": e.Name}}})
	}
	listed, err := json.Marshal(entries)
	if err != nil {
		t.Fatal(err)
	}
	inventory, err := json.Marshal(map[string]any{"data": map[string]string{inventoryKey: string(listed)}})
	if err != nil {
		t.Fatal(err)
	}
	var refusing atomic.Bool
	refusing.Store(true)
	var mu sync.Mutex
	created := map[string]bool{}
	api := standingIn(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		name := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
		mu.Lock()
		defer mu.Unlock()
		switch {
		case name == inventoryName && r.Method == http.MethodGet:
			_, _ = w.Write(inventory)
		case r.Method == http.MethodGet && created[name]:
			_, _ = w.Write([]byte(`{"metadata":{"uid":"7a3c","resourceVersion":"42","managedFields":[{"manager":"` + FieldManager + `","operation":"Apply"}]}}`))
		case r.Method == http.MethodGet:
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(metav1.Status{TypeMeta: metav1.TypeMeta{Kind: "Status", APIVersion: "v1"}, Status: metav1.StatusFailure, Code: http.StatusNotFound, Reason: metav1.StatusReasonNotFound})
		case refusing.Load() && r.URL.Query().Get("dryRun") == "":
			w.WriteHeader(http.StatusForbidden)
			_ = json.NewEncoder(w).Encode(metav1.Status{TypeMeta: metav1.TypeMeta{Kind: "Status", APIVersion: "v1"}, Status: metav1.StatusFailure, Code: http.StatusForbidden, Reason: metav1.StatusReasonForbidden, Message: "kept out"})
		default:
			if r.URL.Query().Get("dryRun") == "" {
				created[name] = true
			}
			_, _ = w.Write([]byte(`{}`))
		}
	}))
	applied := metav1.Condition{Type: lodiav1alpha1.ConditionAddonsApplied, Status: metav1.ConditionTrue, Reason: lodiav1alpha1.ReasonApplied, Message: "12 objects are applied and the inventory is pruned", ObservedGeneration: 1}
	available := metav1.Condition{Type: lodiav1alpha1.ConditionAddonsAvailable, Status: metav1.ConditionTrue, Reason: lodiav1alpha1.ReasonAvailable, Message: "are rolled out", ObservedGeneration: 1}
	cp := &lodiav1alpha1.ControlPlane{ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "dev", Generation: 2}}
	cp.Status.Conditions = []metav1.Condition{applied, available}
	cp.Status.Addons = &lodiav1alpha1.Rendered{Version: "v1.37.1", OperatorRevision: "previous"}
	scheme := runtime.NewScheme()
	if err := lodiav1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	management := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cp.DeepCopy()).WithStatusSubresource(cp).Build()
	var written *lodiav1alpha1.ControlPlane
	r := &Reconciler{Reader: management, Revision: "next", Client: interceptor.NewClient(management, interceptor.Funcs{
		SubResourceApply: func(_ context.Context, _ client.Client, _ string, obj runtime.ApplyConfiguration, _ ...client.SubResourceApplyOption) error {
			raw, err := json.Marshal(obj)
			if err != nil {
				return err
			}
			written = &lodiav1alpha1.ControlPlane{}
			return json.Unmarshal(raw, written)
		},
	})}
	if _, err := r.hold(t.Context(), cp, api, objects, applied, available); err == nil {
		t.Fatal("a held pass whose creations are refused returned no error")
	}
	refused := *meta.FindStatusCondition(written.Status.Conditions, lodiav1alpha1.ConditionAddonsApplied)
	if refused.Status != metav1.ConditionFalse || refused.Reason != lodiav1alpha1.ReasonRefused || !strings.HasSuffix(refused.Message, "; 2 more objects are refused; the reason is in Lodia's log") {
		t.Fatalf("AddonsApplied is %+v, want False Refused naming ten and counting two", refused)
	}
	refusing.Store(false)
	if _, err := r.hold(t.Context(), cp, api, objects, refused, available); err != nil {
		t.Fatal(err)
	}
	const want = "the 12 objects the inventory lists are applied; waiting for Lifecycle to grant the running operator revision next"
	if got := meta.FindStatusCondition(written.Status.Conditions, lodiav1alpha1.ConditionAddonsApplied); got.Status != metav1.ConditionTrue || got.Reason != lodiav1alpha1.ReasonApplied || got.Message != want {
		t.Errorf("AddonsApplied is %+v, want True Applied %q", got, want)
	}
}

// A held pass whose running operator cannot render the tenant's version tries nothing, under a refusal or over a condition not True, and leaves AddonsApplied as it is.
func TestAHeldPassWithoutARenderTriesNothing(t *testing.T) {
	accept := func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{}`)) }
	for _, applied := range []metav1.Condition{heldApplied(metav1.ConditionTrue, lodiav1alpha1.ReasonRefused), heldApplied(metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAccess)} {
		t.Run(applied.Reason, func(t *testing.T) {
			r, cp, api, _, dryRuns, written := heldTrial(t, applied, []entry{configMap, dns}, FieldManager, accept)
			if _, err := r.hold(t.Context(), cp, api, nil, cp.Status.Conditions[0], cp.Status.Conditions[1]); err != nil {
				t.Fatal(err)
			}
			if n := dryRuns.Load(); n != 0 {
				t.Errorf("a held pass without a render sent %d dry runs", n)
			}
			if *written != nil {
				if got := meta.FindStatusCondition((*written).Status.Conditions, lodiav1alpha1.ConditionAddonsApplied); *got != applied {
					t.Errorf("AddonsApplied is %+v, want %+v as it was", got, applied)
				}
			}
		})
	}
}

// A held pass tries the grant's writes while AddonsApplied is not True, and reports a refused one True while every object the inventory lists is Lodia's and a pass has applied in full, over a wait and over a refused creation the tenant has since been given, and False otherwise.
func TestAHeldPassTriesTheGrantOverAConditionNotTrue(t *testing.T) {
	const want = "the grant's write of Deployment kube-system/coredns would be refused: the dry run is refused by the tenant's API server; the reason is in Lodia's log"
	refuse := func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/deployments/coredns") {
			_, _ = w.Write([]byte(`{}`))
			return
		}
		w.WriteHeader(http.StatusForbidden)
		_ = json.NewEncoder(w).Encode(metav1.Status{TypeMeta: metav1.TypeMeta{Kind: "Status", APIVersion: "v1"}, Status: metav1.StatusFailure, Code: http.StatusForbidden, Reason: metav1.StatusReasonForbidden, Message: "kept out"})
	}
	for _, c := range []struct {
		name, reason, manager string
		unreported            bool
		status                metav1.ConditionStatus
	}{
		{"a wait", lodiav1alpha1.ReasonWaitingForAccess, FieldManager, false, metav1.ConditionTrue},
		{"a refused creation", lodiav1alpha1.ReasonRefused, FieldManager, false, metav1.ConditionTrue},
		{"another manager's objects", lodiav1alpha1.ReasonWaitingForAccess, "kubectl", false, metav1.ConditionFalse},
		{"addons no pass applied in full", lodiav1alpha1.ReasonWaitingForAccess, FieldManager, true, metav1.ConditionFalse},
	} {
		t.Run(c.name, func(t *testing.T) {
			r, cp, api, _, dryRuns, written := heldTrial(t, heldApplied(metav1.ConditionFalse, c.reason), []entry{configMap, dns}, c.manager, refuse)
			if c.unreported {
				cp.Status.Addons = nil
			}
			if _, err := r.hold(t.Context(), cp, api, heldObjects(), cp.Status.Conditions[0], cp.Status.Conditions[1]); err == nil {
				t.Error("a held pass whose dry run is refused returned no error")
			}
			if n := dryRuns.Load(); n != 2 {
				t.Errorf("a held pass over AddonsApplied False %s sent %d dry runs, want 2", c.reason, n)
			}
			if *written == nil {
				t.Fatal("a held pass whose dry run is refused wrote nothing")
			}
			if got := meta.FindStatusCondition((*written).Status.Conditions, lodiav1alpha1.ConditionAddonsApplied); got.Status != c.status || got.Reason != lodiav1alpha1.ReasonRefused || got.Message != want || got.ObservedGeneration != 1 {
				t.Errorf("AddonsApplied is %+v, want %s Refused %q at generation 1", got, c.status, want)
			}
		})
	}
}

// A held pass whose dry run of the grant accepts every write over a condition not True that holds no refusal reports what a held pass reports without it: Applied over a wait while the inventory is whole, and an unfinished apply as it is.
func TestAHeldPassWhoseTrialFindsNothingReportsAsAtRest(t *testing.T) {
	accept := func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{}`)) }
	const applied = "the 2 objects the inventory lists are applied; waiting for Lifecycle to grant the running operator revision next"
	for _, c := range []struct {
		reason string
		want   *metav1.Condition
	}{
		{lodiav1alpha1.ReasonWaitingForAccess, &metav1.Condition{Type: lodiav1alpha1.ConditionAddonsApplied, Status: metav1.ConditionTrue, Reason: lodiav1alpha1.ReasonApplied, Message: applied, ObservedGeneration: 1}},
		{lodiav1alpha1.ReasonApplying, nil},
	} {
		t.Run(c.reason, func(t *testing.T) {
			r, cp, api, _, dryRuns, written := heldTrial(t, heldApplied(metav1.ConditionFalse, c.reason), []entry{configMap, dns}, FieldManager, accept)
			want := c.want
			if want == nil {
				want = new(cp.Status.Conditions[0])
			}
			if _, err := r.hold(t.Context(), cp, api, heldObjects(), cp.Status.Conditions[0], cp.Status.Conditions[1]); err != nil {
				t.Fatal(err)
			}
			if n := dryRuns.Load(); n != 2 {
				t.Errorf("a held pass over AddonsApplied False %s sent %d dry runs, want 2", c.reason, n)
			}
			if *written != nil {
				if got := meta.FindStatusCondition((*written).Status.Conditions, lodiav1alpha1.ConditionAddonsApplied); got.Status != want.Status || got.Reason != want.Reason || got.Message != want.Message || got.ObservedGeneration != want.ObservedGeneration {
					t.Errorf("AddonsApplied is %+v, want %+v", got, want)
				}
			} else if c.want != nil {
				t.Errorf("a held pass over AddonsApplied False %s wrote nothing, want %+v", c.reason, want)
			}
		})
	}
}

// A held pass whose trial reads back an inventory it cannot decode, one changed since the pass read it, stops with the error and writes no status.
func TestAHeldPassWhoseTrialCannotReadTheInventoryWritesNoStatus(t *testing.T) {
	r, cp, _, _, _, written := heldTrial(t, heldApplied(metav1.ConditionTrue, lodiav1alpha1.ReasonRefused), []entry{configMap, dns}, FieldManager, nil)
	listed, err := json.Marshal([]entry{configMap, dns})
	if err != nil {
		t.Fatal(err)
	}
	inventory, err := json.Marshal(map[string]any{"data": map[string]string{inventoryKey: string(listed)}})
	if err != nil {
		t.Fatal(err)
	}
	var reads atomic.Int32
	api := standingIn(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(req.URL.Path, "/configmaps/"+inventoryName) && req.Method == http.MethodGet && reads.Add(1) > 1:
			_, _ = w.Write([]byte(`{"data":{"` + inventoryKey + `":"not a list"}}`))
		case strings.HasSuffix(req.URL.Path, "/configmaps/"+inventoryName) && req.Method == http.MethodGet:
			_, _ = w.Write(inventory)
		default:
			_, _ = w.Write([]byte(`{"metadata":{"uid":"7a3c","resourceVersion":"42","generation":1,"managedFields":[{"manager":"` + FieldManager + `","operation":"Apply"}]},"spec":{"replicas":1},"status":{"observedGeneration":1,"updatedReplicas":1,"availableReplicas":1}}`))
		}
	}))
	if _, err := r.hold(t.Context(), cp, api, heldObjects(), cp.Status.Conditions[0], cp.Status.Conditions[1]); err == nil {
		t.Error("a held pass whose trial cannot read the inventory returned no error")
	}
	if *written != nil {
		t.Errorf("a held pass whose trial cannot read the inventory wrote %+v", (*written).Status.Conditions)
	}
}
