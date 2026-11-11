// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package lifecycle

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/statusapply"
	"go.lodia.io/lodia/internal/versions"
)

// facts is everything a pass decides from. backup is the pre-upgrade Backup of
// the minor step cp is at, by backupMinor: nil while it does not exist. fleet
// holds the other tenants that hold a slot of the fleet or wait for one, read
// only while a change is pending; limit is how many may hold one, and settling
// how long a change Lodia started keeps its slot once it has rolled out.
type facts struct {
	cp       *lodiav1alpha1.ControlPlane
	backup   *lodiav1alpha1.Backup
	now      time.Time
	revision string
	fleet    []member
	limit    int
	settling time.Duration
}

// decision holds every status field Lifecycle owns and no other, since an
// apply that leaves one out removes it, the name of the pre-upgrade Backup to
// create, when to pass again, and whether the write takes a slot of the fleet.
type decision struct {
	status       lodiav1alpha1.ControlPlaneStatus
	createBackup string
	requeue      time.Duration
	takesSlot    bool
}

// fieldsDiffer says whether a status field Lifecycle owns, the conditions
// aside, is not what cp holds.
func (d decision) fieldsDiffer(cp *lodiav1alpha1.ControlPlane) bool {
	return d.status.ObservedGeneration != cp.Status.ObservedGeneration ||
		!equality.Semantic.DeepEqual(d.status.Target, cp.Status.Target) ||
		!equality.Semantic.DeepEqual(d.status.Kubernetes, cp.Status.Kubernetes) ||
		!equality.Semantic.DeepEqual(d.status.Maintenance, cp.Status.Maintenance)
}

// differs says whether writing the decision would change cp.
func (d decision) differs(cp *lodiav1alpha1.ControlPlane) bool {
	return d.fieldsDiffer(cp) || slices.ContainsFunc(d.status.Conditions, func(c metav1.Condition) bool {
		return statusapply.Differs(meta.FindStatusCondition(cp.Status.Conditions, c.Type), c)
	})
}

// change is where a version change stands after a pass.
type change struct {
	target  string
	upgrade *lodiav1alpha1.UpgradeStatus
	// unsupported is why the target stays, for the owner to fix.
	unsupported string
	// preflight is a minor step's failed check, its own reason.
	preflight *metav1.Condition
	// waits says the next minor step waits for what every part reports.
	waits string
	// backup says how the pre-upgrade Backup stands, and backupFailed its failure.
	backup, backupFailed string
	createBackup         string
}

func decide(f facts) decision {
	cp := f.cp
	progressing := meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionProgressing)
	readyNow := meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionReady)
	status := lodiav1alpha1.ControlPlaneStatus{
		ObservedGeneration: cp.Status.ObservedGeneration,
		Target:             newTarget(targetOf(cp), revisionOf(cp), nil),
		Maintenance:        newMaintenance(lastHandledOf(cp), nil, nil),
	}
	upgrade := upgradeOf(cp)
	var d decision
	switch {
	case !cp.DeletionTimestamp.IsZero():
		readyNow = newCondition(cp, lodiav1alpha1.ConditionReady, metav1.ConditionFalse, lodiav1alpha1.ReasonDeleting, "the ControlPlane is being deleted")
	case cp.Spec.Paused:
		progressing = newCondition(cp, lodiav1alpha1.ConditionProgressing, metav1.ConditionFalse, lodiav1alpha1.ReasonPaused, "spec.paused is true: Lodia reconciles nothing")
		status.Maintenance = newMaintenance(lastHandledOf(cp), settlingOf(cp), slotOf(cp))
	default:
		c := f.versionChange()
		readyNow = ready(cp)
		m := f.maintain(c, readyNow)
		progressing = c.progressing(f, m)
		if observed(cp) {
			status.ObservedGeneration = cp.Generation
		}
		status.Target = newTarget(c.target, m.revision, m.scheduledFor)
		status.Maintenance = newMaintenance(m.lastHandled, m.settlingSince, m.slotSince)
		upgrade = c.upgrade
		d.createBackup, d.requeue, d.takesSlot = c.createBackup, m.requeue, m.takesSlot
	}
	if reported := oldestReported(cp); reported != "" || upgrade != nil {
		status.Kubernetes = &lodiav1alpha1.KubernetesStatus{Version: reported, Upgrade: upgrade}
	}
	for _, condition := range []*metav1.Condition{progressing, readyNow} {
		if condition != nil {
			status.Conditions = append(status.Conditions, *condition)
		}
	}
	d.status = status
	return d
}

func newTarget(version, revision string, scheduledFor *metav1.Time) *lodiav1alpha1.Target {
	if version == "" && revision == "" && scheduledFor == nil {
		return nil
	}
	return &lodiav1alpha1.Target{Version: version, OperatorRevision: revision, ScheduledFor: scheduledFor}
}

func newMaintenance(lastHandled string, settlingSince, slotSince *metav1.Time) *lodiav1alpha1.MaintenanceStatus {
	if lastHandled == "" && settlingSince == nil && slotSince == nil {
		return nil
	}
	return &lodiav1alpha1.MaintenanceStatus{LastHandled: lastHandled, SettlingSince: settlingSince, SlotSince: slotSince}
}

func targetOf(cp *lodiav1alpha1.ControlPlane) string {
	if cp.Status.Target != nil {
		return cp.Status.Target.Version
	}
	return ""
}

func revisionOf(cp *lodiav1alpha1.ControlPlane) string {
	if cp.Status.Target != nil {
		return cp.Status.Target.OperatorRevision
	}
	return ""
}

func scheduledOf(cp *lodiav1alpha1.ControlPlane) *metav1.Time {
	if cp.Status.Target != nil {
		return cp.Status.Target.ScheduledFor
	}
	return nil
}

func lastHandledOf(cp *lodiav1alpha1.ControlPlane) string {
	if cp.Status.Maintenance != nil {
		return cp.Status.Maintenance.LastHandled
	}
	return ""
}

func settlingOf(cp *lodiav1alpha1.ControlPlane) *metav1.Time {
	if cp.Status.Maintenance != nil {
		return cp.Status.Maintenance.SettlingSince
	}
	return nil
}

// slotOf is when the change Lodia started in cp took its slot of the fleet,
// nil while cp holds none; only that change settling and deletion end it, an
// owner's edit and a pause do not (docs/decisions/upgrades-and-maintenance-windows.md).
func slotOf(cp *lodiav1alpha1.ControlPlane) *metav1.Time {
	if cp.Status.Maintenance != nil {
		return cp.Status.Maintenance.SlotSince
	}
	return nil
}

func upgradeOf(cp *lodiav1alpha1.ControlPlane) *lodiav1alpha1.UpgradeStatus {
	if cp.Status.Kubernetes != nil {
		return cp.Status.Kubernetes.Upgrade
	}
	return nil
}

// versionChange moves the target and upgrade towards spec.version
// (docs/decisions/upgrades-and-maintenance-windows.md).
func (f facts) versionChange() change {
	cp := f.cp
	want := cp.Spec.Version
	c := change{target: targetOf(cp), upgrade: upgradeOf(cp)}
	if _, ok := versions.For(want); !ok {
		c.unsupported = fmt.Sprintf("spec.version %s is not of a minor Lodia runs: %s", want, strings.Join(versions.Minors(), ", "))
		c.settle(cp)
		return c
	}
	if c.target == "" {
		c.target = want
		return c
	}
	ahead, ok := versions.MinorsAhead(c.target, want)
	switch {
	// CEL keeps spec's minor from decreasing, so this is a hand-edited status,
	// and a minor is never rolled back.
	case !ok || ahead < 0:
		c.unsupported = fmt.Sprintf("spec.version %s is an older minor than the target %s, which stays", want, c.target)
		c.settle(cp)
	case ahead > 0:
		f.minorStep(&c)
	case want != c.target:
		c.upgrade = moved(c.upgrade, f.now)
		c.target = want
	default:
		c.settle(cp)
	}
	return c
}

// moved is the upgrade once the target moves: its next step is the API
// server, and a change under way keeps its start.
func moved(upgrade *lodiav1alpha1.UpgradeStatus, now time.Time) *lodiav1alpha1.UpgradeStatus {
	started := metav1.NewTime(now).Rfc3339Copy()
	if upgrade != nil {
		started = upgrade.StartedAt
	}
	return &lodiav1alpha1.UpgradeStatus{Step: lodiav1alpha1.UpgradeStepAPIServer, StartedAt: started}
}

// settle derives the step from what runs: the first part not at the target,
// and no upgrade once all four are. The Backup step alone is recorded.
func (c *change) settle(cp *lodiav1alpha1.ControlPlane) {
	if c.upgrade == nil || c.upgrade.Step == lodiav1alpha1.UpgradeStepBackup {
		return
	}
	for _, part := range parts(cp) {
		if part.version != c.target {
			c.upgrade = &lodiav1alpha1.UpgradeStatus{Step: part.step, StartedAt: c.upgrade.StartedAt}
			return
		}
	}
	c.upgrade = nil
}

func (f facts) minorStep(c *change) {
	cp := f.cp
	next, nextMinor, inTable := nextVersion(cp.Spec.Version, c.target)
	switch {
	case c.upgrade != nil && c.upgrade.Step == lodiav1alpha1.UpgradeStepBackup:
		if !inTable {
			c.unsupported = notInTable(nextMinor)
			return
		}
		f.takeBackup(c, next, nextMinor)
		return
	case c.upgrade != nil:
		c.settle(cp)
		if c.upgrade != nil {
			return
		}
	}
	if !allAtTarget(cp, c.target) {
		c.waits = fmt.Sprintf("the minor step towards %s waits until every part runs %s", cp.Spec.Version, c.target)
		return
	}
	switch {
	case !inTable:
		c.unsupported = notInTable(nextMinor)
		return
	case !statusapply.TrueAt(cp, lodiav1alpha1.ConditionDatastoreReady):
		c.preflight = newCondition(cp, lodiav1alpha1.ConditionProgressing, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForDatastore,
			fmt.Sprintf("the minor step to %s waits for DatastoreReady", next))
		return
	case cp.Status.Nodes == nil:
		c.preflight = newCondition(cp, lodiav1alpha1.ConditionProgressing, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForNodes,
			fmt.Sprintf("the minor step to %s waits: the nodes are not listed yet", next))
		return
	}
	if kubelet := cp.Status.Nodes.OldestKubelet; kubelet != "" {
		if behind, ok := versions.MinorsAhead(kubelet, nextMinor); ok && behind > maxKubeletSkew {
			c.preflight = newCondition(cp, lodiav1alpha1.ConditionProgressing, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForNodes,
				fmt.Sprintf("the oldest kubelet, %s, is more than %d minors older than %s: upgrade the nodes first", kubelet, maxKubeletSkew, nextMinor))
			return
		}
	}
	if meta.IsStatusConditionTrue(cp.Status.Conditions, lodiav1alpha1.ConditionControllerManagerHeld) {
		c.preflight = newCondition(cp, lodiav1alpha1.ConditionProgressing, metav1.ConditionFalse, lodiav1alpha1.ReasonControllerManagerHeld,
			fmt.Sprintf("the minor step to %s waits until the controller manager is no longer held", next))
		return
	}
	c.upgrade = &lodiav1alpha1.UpgradeStatus{Step: lodiav1alpha1.UpgradeStepBackup, StartedAt: metav1.NewTime(f.now).Rfc3339Copy()}
	f.takeBackup(c, next, nextMinor)
}

// maxKubeletSkew is the kubelet's skew the version skew policy allows
// (https://kubernetes.io/releases/version-skew-policy/#kubelet).
const maxKubeletSkew = 3

// clockSkew is how far the operator's clock, which writes upgrade.startedAt,
// may be from the API server's, which writes a Backup's creationTimestamp.
const clockSkew = time.Minute

func notInTable(minor string) string {
	return fmt.Sprintf("the next minor, %s, is not one Lodia runs: %s", minor, strings.Join(versions.Minors(), ", "))
}

// nextVersion is the version a minor step from target towards spec goes to:
// spec itself on the next minor, else that minor's newest patch.
func nextVersion(spec, target string) (version, minor string, inTable bool) {
	targetMinor, _ := versions.Minor(target)
	minor, inTable = versions.Next(targetMinor)
	if !inTable {
		return "", minor, false
	}
	if specMinor, _ := versions.Minor(spec); specMinor == minor {
		return spec, minor, true
	}
	version, _ = versions.Latest(minor)
	return version, minor, true
}

// takeBackup waits for the pre-upgrade Backup, created while it does not
// exist, and moves the target once it is done.
func (f facts) takeBackup(c *change, next, minor string) {
	name := lodiav1alpha1.PreUpgradeBackupName(f.cp.Name, minor, f.cp.UID)
	if f.backup == nil {
		c.createBackup = name
		c.backup = fmt.Sprintf("the Backup %s is being created before the target moves to %s", name, next)
		return
	}
	if !f.backup.DeletionTimestamp.IsZero() {
		c.backup = fmt.Sprintf("the Backup %s is being deleted, and another is taken once it is gone", name)
		return
	}
	ready := meta.FindStatusCondition(f.backup.Status.Conditions, lodiav1alpha1.ConditionReady)
	switch {
	case completed(ready), noBackupStore(ready):
		if c.failure(f.cp) == "" {
			c.target, c.upgrade = next, moved(c.upgrade, f.now)
		}
	case ready == nil:
		c.backup = fmt.Sprintf("the Backup %s has reported nothing yet", name)
	case ready.Status == metav1.ConditionFalse && slices.Contains(backupFailures, ready.Reason):
		c.backupFailed = fmt.Sprintf("the Backup %s: %s: %s; delete it and Lodia takes another", name, ready.Reason, ready.Message)
	default:
		c.backup = fmt.Sprintf("the Backup %s: %s: %s", name, ready.Reason, ready.Message)
	}
}

var backupFailures = []string{lodiav1alpha1.ReasonFailed, lodiav1alpha1.ReasonSealLost, lodiav1alpha1.ReasonUnsupported}

func completed(ready *metav1.Condition) bool {
	return ready != nil && ready.Status == metav1.ConditionTrue && ready.Reason == lodiav1alpha1.ReasonCompleted
}

func noBackupStore(ready *metav1.Condition) bool {
	return ready != nil && ready.Status == metav1.ConditionFalse && ready.Reason == lodiav1alpha1.ReasonNoBackupStore
}

// backupMinor is the minor whose pre-upgrade Backup concerns cp: the one an
// upgrade under way took, or the next minor when a minor step is at its
// Backup, or starts on this pass, which an upgrade whose last part just
// reached the target does.
func backupMinor(cp *lodiav1alpha1.ControlPlane) string {
	target, upgrade := targetOf(cp), upgradeOf(cp)
	if target == "" {
		return ""
	}
	targetMinor, _ := versions.Minor(target)
	ahead, ok := versions.MinorsAhead(target, cp.Spec.Version)
	later := ok && ahead > 0
	if upgrade != nil && upgrade.Step != lodiav1alpha1.UpgradeStepBackup && (!later || !allAtTarget(cp, target)) {
		return targetMinor
	}
	if !later {
		return ""
	}
	next, _ := versions.Next(targetMinor)
	return next
}

type part struct {
	name, version string
	step          lodiav1alpha1.UpgradeStep
}

// parts are the four reports of an upgrade, in the order the steps run; an
// unreported one has an empty version.
func parts(cp *lodiav1alpha1.ControlPlane) []part {
	var apiServer, controllerManager, scheduler *lodiav1alpha1.ComponentStatus
	if c := cp.Status.Components; c != nil {
		apiServer, controllerManager, scheduler = c.APIServer, c.ControllerManager, c.Scheduler
	}
	version := func(c *lodiav1alpha1.ComponentStatus) string {
		if c != nil {
			return c.Version
		}
		return ""
	}
	addons := ""
	if a := cp.Status.Addons; a != nil {
		addons = a.Version
	}
	return []part{
		{"kube-apiserver", version(apiServer), lodiav1alpha1.UpgradeStepAPIServer},
		{"kube-controller-manager", version(controllerManager), lodiav1alpha1.UpgradeStepControllerManager},
		{"kube-scheduler", version(scheduler), lodiav1alpha1.UpgradeStepScheduler},
		{"the addons", addons, lodiav1alpha1.UpgradeStepAddons},
	}
}

func allAtTarget(cp *lodiav1alpha1.ControlPlane, target string) bool {
	return !slices.ContainsFunc(parts(cp), func(p part) bool { return p.version != target })
}

// notAtTarget names the parts that do not report target.
func notAtTarget(cp *lodiav1alpha1.ControlPlane, target string) []string {
	var behind []string
	for _, p := range parts(cp) {
		switch {
		case p.version == "":
			behind = append(behind, p.name+": reports no version")
		case p.version != target:
			behind = append(behind, fmt.Sprintf("%s: reports %s, not %s", p.name, p.version, target))
		}
	}
	return behind
}

// upgradeFailures are the reasons of a condition Ready is built from that stop
// an upgrade: an object refused or kept elsewhere, or a rollout past its
// deadline (docs/decisions/upgrades-and-maintenance-windows.md).
var upgradeFailures = []string{
	lodiav1alpha1.ReasonRefused, lodiav1alpha1.ReasonObjectsControlledElsewhere, lodiav1alpha1.ReasonUnsupported, lodiav1alpha1.ReasonProgressDeadlineExceeded,
}

// failure is why a version change under way has stopped, empty while it has
// not.
func (c change) failure(cp *lodiav1alpha1.ControlPlane) string {
	if c.upgrade == nil {
		return ""
	}
	return c.cause(cp)
}

// failed is why a change its owner started has stopped: a cause while an
// upgrade exists or a renderer has not reached the granted revision, so that
// an idle tenant's standing refusal is Ready's to report, and an owner's or a
// token's grant of a revision that is refused is not.
func (c change) failed(f facts, granted string) string {
	if c.upgrade == nil && len(behind(f.cp, granted, f.revision)) == 0 {
		return ""
	}
	return c.cause(f.cp)
}

// cause is the first condition Ready is built from that stops a change, or
// the pre-upgrade Backup's failure.
func (c change) cause(cp *lodiav1alpha1.ControlPlane) string {
	if c.backupFailed != "" {
		return c.backupFailed
	}
	if condition := stopping(cp); condition != nil {
		return fmt.Sprintf("%s: %s: %s", condition.Type, condition.Reason, condition.Message)
	}
	return ""
}

// stopping is the first condition Ready is built from whose reason stops a
// change, or nil.
func stopping(cp *lodiav1alpha1.ControlPlane) *metav1.Condition {
	for _, conditionType := range readiness(cp) {
		if condition := meta.FindStatusCondition(cp.Status.Conditions, conditionType); condition != nil && slices.Contains(upgradeFailures, condition.Reason) {
			return condition
		}
	}
	return nil
}

// rolling names what keeps a change rolling: a component or an etcd member
// not updated, a part not at the target, and a renderer that has not reached
// the granted revision.
func (c change) rolling(f facts, granted string) []string {
	return slices.Concat(rollingOut(f.cp), notAtTarget(f.cp, c.target), behind(f.cp, granted, f.revision))
}

// progressing writes the first of Progressing's reasons that applies, in the
// order docs/api/controlplane.md gives, and m's note after its message.
func (c change) progressing(f facts, m maintenance) *metav1.Condition {
	condition := c.reason(f, m)
	if m.note != "" {
		condition.Message += "; " + m.note
	}
	return condition
}

func (c change) reason(f facts, m maintenance) *metav1.Condition {
	cp := f.cp
	progressing := func(status metav1.ConditionStatus, reason, message string) *metav1.Condition {
		return newCondition(cp, lodiav1alpha1.ConditionProgressing, status, reason, message)
	}
	switch {
	case c.unsupported != "":
		return progressing(metav1.ConditionFalse, lodiav1alpha1.ReasonUnsupported, c.unsupported)
	case m.unsupported != "":
		return progressing(metav1.ConditionFalse, lodiav1alpha1.ReasonUnsupported, m.unsupported)
	}
	if why := c.failed(f, m.revision); why != "" && m.slotSince == nil {
		return progressing(metav1.ConditionFalse, lodiav1alpha1.ReasonFailed, why)
	}
	switch {
	case m.failed != "":
		return progressing(metav1.ConditionFalse, lodiav1alpha1.ReasonMaintenanceFailed, m.failed)
	case m.zone != "":
		return progressing(metav1.ConditionFalse, lodiav1alpha1.ReasonTimeZoneUnsupported, m.zone)
	case m.maintaining != "":
		return progressing(metav1.ConditionTrue, lodiav1alpha1.ReasonMaintaining, m.maintaining)
	case c.preflight != nil:
		return c.preflight
	}
	if lag := lagging(cp); len(lag) > 0 {
		return progressing(metav1.ConditionTrue, lodiav1alpha1.ReasonLatestGenerationNotObserved, fmt.Sprintf("generation %d is not taken up yet: %s", cp.Generation, strings.Join(lag, ", ")))
	}
	rolling := c.rolling(f, m.revision)
	switch {
	case c.upgrade != nil:
		return progressing(metav1.ConditionTrue, lodiav1alpha1.ReasonUpgrading, c.upgradingMessage(f, rolling))
	case len(rolling) > 0:
		message := strings.Join(rolling, "; ")
		if c.waits != "" {
			message = c.waits + ": " + message
		}
		return progressing(metav1.ConditionTrue, lodiav1alpha1.ReasonRollingOut, message)
	case m.waiting != nil:
		return m.waiting
	}
	return progressing(metav1.ConditionFalse, lodiav1alpha1.ReasonIdle, "no upgrade, rotation or hibernation is running")
}

func (c change) upgradingMessage(f facts, rolling []string) string {
	if c.upgrade.Step == lodiav1alpha1.UpgradeStepBackup {
		return c.backup
	}
	message := fmt.Sprintf("upgrading to %s, step %s", c.target, c.upgrade.Step)
	if len(rolling) > 0 {
		message += ": " + strings.Join(rolling, "; ")
	}
	if ready := backupReady(f.backup); noBackupStore(ready) && !f.backup.CreationTimestamp.Time.Before(c.upgrade.StartedAt.Add(-clockSkew)) {
		message += "; no pre-upgrade Backup was taken: " + ready.Message
	}
	return message
}

func backupReady(backup *lodiav1alpha1.Backup) *metav1.Condition {
	if backup == nil {
		return nil
	}
	return meta.FindStatusCondition(backup.Status.Conditions, lodiav1alpha1.ConditionReady)
}
