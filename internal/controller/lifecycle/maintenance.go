// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package lifecycle

import (
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/versions"
)

const (
	// maxToken is status.maintenance.lastHandled's MaxLength.
	maxToken = 256

	fleetRequeue = 30 * time.Second
)

// maintenance is what a pass grants of a pending change, and how the change
// Lodia started in the tenant's slot of the fleet stands
// (docs/decisions/upgrades-and-maintenance-windows.md).
type maintenance struct {
	// revision is target.operatorRevision after the pass.
	revision      string
	scheduledFor  *metav1.Time
	lastHandled   string
	settlingSince *metav1.Time
	// slotSince is status.maintenance.slotSince after the pass, present while
	// the tenant holds a slot, and takesSlot says the pass takes it.
	slotSince *metav1.Time
	takesSlot bool
	// unsupported, zone, failed and maintaining are Progressing's message for
	// Unsupported, TimeZoneUnsupported, MaintenanceFailed and Maintaining.
	unsupported, zone, failed, maintaining string
	// waiting is Progressing while a pending change waits its turn.
	waiting *metav1.Condition
	// note ends Progressing's message, whatever its reason.
	note    string
	requeue time.Duration
}

// member is another tenant that holds a slot of the fleet, or waits for one
// under key.
type member struct {
	slot bool
	key  queueKey
}

// queueKey orders the tenants that wait for a slot.
type queueKey struct {
	scheduledFor    time.Time
	namespace, name string
}

func (k queueKey) before(other queueKey) bool {
	switch {
	case !k.scheduledFor.Equal(other.scheduledFor):
		return k.scheduledFor.Before(other.scheduledFor)
	case k.namespace != other.namespace:
		return k.namespace < other.namespace
	}
	return k.name < other.name
}

// fleetOf is the members among cps, cp and tenants being deleted left out:
// those in a slot, paused ones included, so that unpausing one never takes the
// fleet past its limit, and those not paused that are queued, whose
// scheduledFor is at or before now, whatever Progressing says, since a wait
// for a slot can sit under a reason ranked above FleetLimit.
func fleetOf(cps []lodiav1alpha1.ControlPlane, cp *lodiav1alpha1.ControlPlane, now time.Time) []member {
	var fleet []member
	for i := range cps {
		other := &cps[i]
		if other.Namespace == cp.Namespace && other.Name == cp.Name || !other.DeletionTimestamp.IsZero() {
			continue
		}
		key := queueKey{namespace: other.Namespace, name: other.Name}
		switch s := scheduledOf(other); {
		case slotOf(other) != nil:
			fleet = append(fleet, member{slot: true, key: key})
		case other.Spec.Paused:
		case s != nil && !s.After(now):
			key.scheduledFor = s.Time
			fleet = append(fleet, member{key: key})
		}
	}
	return fleet
}

// pendingChanges is what the running operator may grant a tenant, each kind
// granted at once on its own paths of rule 3 and all of them in one slot by
// rule 7 (docs/decisions/upgrades-and-maintenance-windows.md).
type pendingChanges struct {
	revision bool
}

func (p pendingChanges) any() bool {
	return p.revision
}

// pending is what the running operator may grant after c.
func (f facts) pending(c change) pendingChanges {
	return pendingChanges{revision: revisionOf(f.cp) != f.revision}
}

// describe names what p holds, with the verb that follows.
func (p pendingChanges) describe(f facts) string {
	var kinds []string
	if p.revision {
		kinds = append(kinds, "operator revision "+f.revision)
	}
	if len(kinds) == 1 {
		return kinds[0] + " waits"
	}
	return strings.Join(kinds[:len(kinds)-1], ", ") + " and " + kinds[len(kinds)-1] + " wait"
}

func (f facts) take(m *maintenance, p pendingChanges) {
	if p.revision {
		m.revision = f.revision
	}
}

// maintain records a new lodia.io/maintain-requested-at, grants a pending
// change by the rules, and decides the change in the tenant's slot.
func (f facts) maintain(c change, ready *metav1.Condition) maintenance {
	cp := f.cp
	granted := revisionOf(cp)
	m := maintenance{revision: granted, lastHandled: lastHandledOf(cp), slotSince: slotOf(cp)}
	token := cp.Annotations[lodiav1alpha1.MaintainRequestedAtAnnotation]
	switch {
	case token == "":
	case utf8.RuneCountInString(token) > maxToken:
		m.note = fmt.Sprintf("%s holds more than %d characters and is not handled", lodiav1alpha1.MaintainRequestedAtAnnotation, maxToken)
		token = ""
	case token == m.lastHandled:
		token = ""
	default:
		m.lastHandled = token
	}
	w, err := windowOf(cp)
	if err != nil {
		m.zone = err.Error()
	}
	if p := f.pending(c); p.any() {
		f.grant(&m, c, p, w, token != "")
	}
	if m.slotSince != nil {
		f.settle(&m, c, ready, granted)
	}
	return m
}

// grant applies the rules to the pending changes, in their order: nothing
// while the running operator cannot render what runs; at once, with no slot,
// each kind where waiting would stall the owner or keep the tenant from
// Ready; then, unless the owner has something to fix, all that remain in the
// window, while the tenant is Ready with no cause that fails a change
// standing, in one free slot.
func (f facts) grant(m *maintenance, c change, p pendingChanges, w *window, requested bool) {
	cp := f.cp
	if what := unrenderable(cp, c.target); what != "" {
		m.unsupported = fmt.Sprintf("operator revision %s does not run %s, of a minor outside %s; the ControlPlane stays at revision %s", f.revision, what, strings.Join(versions.Minors(), ", "), m.revision)
		return
	}
	inSlot := m.slotSince != nil
	if p.revision && (inSlot || requested || f.atOnce(c)) {
		m.revision, p.revision = f.revision, false
	}
	if !p.any() {
		return
	}
	if c.unsupported != "" || c.failed(f, m.revision) != "" || m.zone != "" {
		return
	}
	if w != nil {
		if open, next := w.state(f.now); !open {
			spec := cp.Spec.Maintenance.Window
			m.scheduledFor = &metav1.Time{Time: next}
			m.waiting = newCondition(cp, lodiav1alpha1.ConditionProgressing, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForWindow,
				fmt.Sprintf("%s for the maintenance window %s-%s %s, which opens at %s", p.describe(f), spec.Begin, spec.End, w.zone, next.UTC().Format(time.RFC3339)))
			m.requeue = min(next.Sub(f.now), time.Hour)
			return
		}
	}
	if notReady := readyForMaintenance(cp); notReady != nil {
		reason, message := notReady.Reason, notReady.Message
		if notReady.Status == metav1.ConditionUnknown {
			// Ready's Unknown reasons, Reconciling and
			// LatestGenerationNotObserved, say something else under False.
			reason, message = lodiav1alpha1.ReasonWaitingForReady, fmt.Sprintf("Ready is Unknown: %s: %s", notReady.Reason, notReady.Message)
		}
		m.waiting = newCondition(cp, lodiav1alpha1.ConditionProgressing, metav1.ConditionFalse, reason, message)
		return
	}
	if stops := stopping(cp); stops != nil {
		message := stops.Type
		if stops.Message != "" {
			message += ": " + stops.Message
		}
		m.waiting = newCondition(cp, lodiav1alpha1.ConditionProgressing, metav1.ConditionFalse, stops.Reason, message)
		return
	}
	key := f.key()
	if f.free(key) {
		since := metav1.NewTime(f.now).Rfc3339Copy()
		f.take(m, p)
		m.slotSince, m.takesSlot = &since, true
		return
	}
	m.scheduledFor = &metav1.Time{Time: key.scheduledFor}
	m.waiting = newCondition(cp, lodiav1alpha1.ConditionProgressing, metav1.ConditionFalse, lodiav1alpha1.ReasonFleetLimit,
		fmt.Sprintf("%s for one of the fleet's %d slots, in the order of status.target.scheduledFor", p.describe(f), f.limit))
	m.requeue = fleetRequeue
}

// unrenderable names the target or a reported version whose minor the
// running operator does not run, empty when it runs them all.
func unrenderable(cp *lodiav1alpha1.ControlPlane, target string) string {
	named := []struct{ field, version string }{{"status.target.version", target}}
	if c := cp.Status.Components; c != nil {
		for _, component := range []struct {
			field  string
			status *lodiav1alpha1.ComponentStatus
		}{{"apiServer", c.APIServer}, {"controllerManager", c.ControllerManager}, {"scheduler", c.Scheduler}} {
			if component.status != nil {
				named = append(named, struct{ field, version string }{"status.components." + component.field + ".version", component.status.Version})
			}
		}
	}
	if a := cp.Status.Addons; a != nil {
		named = append(named, struct{ field, version string }{"status.addons.version", a.Version})
	}
	for _, n := range named {
		if _, ok := versions.For(n.version); n.version != "" && !ok {
			return n.field + " " + n.version
		}
	}
	return ""
}

// atOnce says whether a pending change is granted with no slot: the tenant has
// no target yet, its owner's change is not taken up or upgrades it, or only
// the grant brings it back to Ready (docs/decisions/upgrades-and-maintenance-windows.md).
func (f facts) atOnce(c change) bool {
	cp := f.cp
	progressing := meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionProgressing)
	addons := meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionAddonsApplied)
	api := meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionAPIServerReady)
	switch {
	case targetOf(cp) == "" || revisionOf(cp) == "":
	case progressing == nil || progressing.ObservedGeneration < cp.Generation:
	case c.upgrade != nil:
	case (addons == nil || addons.Status != metav1.ConditionTrue) &&
		(cp.Status.Addons == nil || cp.Status.Addons.Version == "" || addons != nil && slices.Contains(stoppedAddons, addons.Reason)):
	case api != nil && api.Status == metav1.ConditionFalse && api.Reason == lodiav1alpha1.ReasonWaitingForTarget && api.ObservedGeneration == cp.Generation:
	case cp.Spec.RestoreFrom != nil && (cp.Status.Components == nil || cp.Status.Components.ControllerManager == nil || cp.Status.Components.ControllerManager.Total == 0):
	default:
		return false
	}
	return true
}

// stoppedAddons are the reasons of an AddonsApplied whose apply stopped on an
// object refused or another owner's, or did not finish.
var stoppedAddons = []string{lodiav1alpha1.ReasonRefused, lodiav1alpha1.ReasonObjectsControlledElsewhere, lodiav1alpha1.ReasonApplying}

// readyForMaintenance is what keeps Ready from True, at whatever generation
// each input describes, or nil. AddonsApplied is left out while it waits: a
// held Tenant cannot bring it back, and the grant does. AccessReady comes
// before it, so one that is not True still names the reason.
func readyForMaintenance(cp *lodiav1alpha1.ControlPlane) *metav1.Condition {
	inputs := readiness(cp)
	addons := meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionAddonsApplied)
	if addons != nil && addons.Status == metav1.ConditionFalse && strings.HasPrefix(addons.Reason, "WaitingFor") {
		inputs = slices.DeleteFunc(inputs, func(conditionType string) bool { return conditionType == lodiav1alpha1.ConditionAddonsApplied })
	}
	if c := readyOf(cp, inputs); c.Status != metav1.ConditionTrue {
		return c
	}
	return nil
}

// key is cp's place in the queue for a slot: when it became eligible, which
// its scheduledFor keeps once written, then its namespace and name.
func (f facts) key() queueKey {
	key := queueKey{scheduledFor: metav1.NewTime(f.now).Rfc3339Copy().Time, namespace: f.cp.Namespace, name: f.cp.Name}
	if s := scheduledOf(f.cp); s != nil && !s.After(f.now) {
		key.scheduledFor = s.Time
	}
	return key
}

// free says whether fewer tenants than the limit hold a slot or wait for one
// before key.
func (f facts) free(key queueKey) bool {
	taken := 0
	for _, m := range f.fleet {
		if m.slot || m.key.before(key) {
			taken++
		}
	}
	return taken < f.limit
}

// settle decides the change Lodia started in cp's slot: failed by the rule
// both origins share, rolling, or rolled out and kept in the slot for the
// settling time from the later of settlingSince and Ready's last transition;
// then the slot is free while Ready is True, and the change failed while it
// is not.
func (f facts) settle(m *maintenance, c change, ready *metav1.Condition, granted string) {
	cp := f.cp
	rolling := c.rolling(f, m.revision)
	if m.revision == granted && len(rolling) == 0 {
		m.settlingSince = settlingOf(cp)
		if m.settlingSince == nil {
			since := metav1.NewTime(f.now).Rfc3339Copy()
			m.settlingSince = &since
		}
	}
	if cause := c.cause(cp); cause != "" {
		m.failed = "the change Lodia started failed: " + cause
		return
	}
	if m.settlingSince == nil {
		m.maintaining = "operator revision " + m.revision + " rolls"
		if len(rolling) > 0 {
			m.maintaining += ": " + strings.Join(rolling, "; ")
		}
		return
	}
	since := m.settlingSince.Time
	if changed := readyChanged(cp, ready, f.now); changed.After(since) {
		since = changed
	}
	deadline := since.Add(f.settling)
	switch {
	case f.now.Before(deadline):
		m.maintaining = fmt.Sprintf("operator revision %s has rolled out; the ControlPlane keeps its slot of the fleet until %s", m.revision, deadline.UTC().Format(time.RFC3339))
		m.requeue = deadline.Sub(f.now)
	case ready.Status == metav1.ConditionTrue:
		m.slotSince, m.settlingSince = nil, nil
	default:
		m.failed = fmt.Sprintf("Ready is not True %s after operator revision %s rolled out: %s: %s", f.settling, m.revision, ready.Reason, ready.Message)
	}
}

// readyChanged is when Ready took the status next has: its stored transition,
// or now on a pass that changes it, since the write stamps it.
func readyChanged(cp *lodiav1alpha1.ControlPlane, next *metav1.Condition, now time.Time) time.Time {
	if stored := meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionReady); stored != nil && stored.Status == next.Status {
		return stored.LastTransitionTime.Time
	}
	return now
}

// behind names the renderers whose report is neither the granted revision
// nor the running one: a change that has not reached them yet.
func behind(cp *lodiav1alpha1.ControlPlane, granted, running string) []string {
	type report struct{ name, revision string }
	var reports []report
	if c := cp.Status.Components; c != nil {
		for _, component := range []struct {
			name   string
			status *lodiav1alpha1.ComponentStatus
		}{{"kube-apiserver", c.APIServer}, {"kube-controller-manager", c.ControllerManager}, {"kube-scheduler", c.Scheduler}} {
			if component.status != nil {
				reports = append(reports, report{component.name, component.status.OperatorRevision})
			}
		}
	}
	if a := cp.Status.Addons; a != nil {
		reports = append(reports, report{"the addons", a.OperatorRevision})
	}
	if d := cp.Status.Datastore; d != nil {
		reports = append(reports, report{"etcd", d.OperatorRevision})
	}
	var out []string
	for _, r := range reports {
		switch r.revision {
		case granted, running:
		case "":
			out = append(out, r.name+": reports no operator revision")
		default:
			out = append(out, fmt.Sprintf("%s: reports operator revision %s, not %s", r.name, r.revision, granted))
		}
	}
	return out
}
