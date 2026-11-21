// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

// Package druidtest writes what etcd-druid v0.39.2 writes, for tests that run
// no etcd-druid: an Etcd's status, the removal of its annotation, and its
// tasks' outcomes. It is Lodia's reading of etcd-druid in one place, and it
// belongs to test files.
package druidtest

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"time"

	druidapicommon "github.com/gardener/etcd-druid/api/common"
	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	"github.com/gardener/etcd-druid/api/core/v1alpha1/crds"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	"go.lodia.io/lodia/internal/contract"
)

// CRDs returns etcd-druid's CRDs for Kubernetes 1.35, the minor of
// ENVTEST_K8S_VERSION.
func CRDs() ([]*apiextensionsv1.CustomResourceDefinition, error) {
	manifests, err := crds.GetAll("1.35")
	if err != nil {
		return nil, err
	}
	var out []*apiextensionsv1.CustomResourceDefinition
	for name, manifest := range manifests {
		crd := &apiextensionsv1.CustomResourceDefinition{}
		if err := yaml.Unmarshal([]byte(manifest), crd); err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		out = append(out, crd)
	}
	return out, nil
}

// An Option changes the status SetStatus writes of the Etcd it names.
type Option func(etcd string, generation int64, s *druidv1alpha1.EtcdStatus)

// SetStatus writes the status of the Etcd of the ControlPlane cp as
// etcd-druid reports one whose spec it reconciled and whose one member, the
// leader, runs that spec and renews its Lease, then as each option says; the
// last operation stays as it is. etcd-druid stamps the generation it
// reconciled (internal/controller/etcd/reconcile_complete.go#L33-L41), counts
// the StatefulSet's pods (internal/controller/etcd/reconcile_status.go#L70-L92),
// lists the members from their Leases (internal/health/etcdmember/check_ready.go#L59-L111)
// and writes AllMembersUpdated from the StatefulSet's revisions
// (internal/health/condition/check_all_members_updated.go#L21-L52).
func SetStatus(ctx context.Context, c client.Client, namespace, cp string, options ...Option) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		etcd := &druidv1alpha1.Etcd{}
		if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: contract.EtcdName(cp)}, etcd); err != nil {
			return err
		}
		etcd.Status = druidv1alpha1.EtcdStatus{
			LastOperation:      etcd.Status.LastOperation,
			ObservedGeneration: new(etcd.Generation),
			Ready:              new(true),
			Replicas:           1,
			ReadyReplicas:      1,
			CurrentReplicas:    1,
			Conditions: []druidv1alpha1.Condition{{
				Type: druidv1alpha1.ConditionTypeAllMembersUpdated, Status: druidv1alpha1.ConditionTrue, Reason: "AllMembersUpdated",
				Message: "All members reflect latest desired spec", LastTransitionTime: metav1.Now(), LastUpdateTime: metav1.Now(),
			}},
			Members: []druidv1alpha1.EtcdMemberStatus{{
				Name: druidv1alpha1.GetOrdinalPodName(etcd.ObjectMeta, 0), ID: new("8e9e05c52164694d"), Role: new(druidv1alpha1.EtcdRoleLeader),
				Status: druidv1alpha1.EtcdMemberStatusReady, Reason: "LeaseSucceeded", LastTransitionTime: metav1.Now(),
			}},
		}
		for _, option := range options {
			option(etcd.Name, etcd.Generation, &etcd.Status)
		}
		return c.Status().Update(ctx, etcd)
	})
}

// Members counts replicas pods of the StatefulSet, ready of them ready, and
// the Etcd Ready while all are (internal/controller/etcd/reconcile_status.go#L73-L78).
func Members(replicas, ready int32) Option {
	return func(_ string, _ int64, s *druidv1alpha1.EtcdStatus) {
		s.Replicas, s.CurrentReplicas, s.ReadyReplicas, s.Ready = replicas, replicas, ready, new(ready == replicas)
	}
}

// Member lists the member of the ordinal given in that state, adding it to
// the members when it is not listed.
func Member(ordinal int, status druidv1alpha1.EtcdMemberConditionStatus, reason string) Option {
	return func(etcd string, _ int64, s *druidv1alpha1.EtcdStatus) {
		name := etcd + "-" + strconv.Itoa(ordinal)
		if i := slices.IndexFunc(s.Members, func(m druidv1alpha1.EtcdMemberStatus) bool { return m.Name == name }); i >= 0 {
			s.Members[i].Status, s.Members[i].Reason = status, reason
			return
		}
		s.Members = append(s.Members, druidv1alpha1.EtcdMemberStatus{Name: name, Status: status, Reason: reason, LastTransitionTime: metav1.Now()})
	}
}

// Unlisted leaves the member of the ordinal given out of the members, as for
// one whose Lease no member renewed yet (internal/health/etcdmember/check_ready.go#L70-L78).
func Unlisted(ordinal int) Option {
	return func(etcd string, _ int64, s *druidv1alpha1.EtcdStatus) {
		name := etcd + "-" + strconv.Itoa(ordinal)
		s.Members = slices.DeleteFunc(s.Members, func(m druidv1alpha1.EtcdMemberStatus) bool { return m.Name == name })
	}
}

// Unobserved stamps the generation before the Etcd's.
func Unobserved() Option {
	return func(_ string, generation int64, s *druidv1alpha1.EtcdStatus) {
		s.ObservedGeneration = new(generation - 1)
	}
}

// Observed stamps the generation given.
func Observed(generation int64) Option {
	return func(_ string, _ int64, s *druidv1alpha1.EtcdStatus) {
		s.ObservedGeneration = new(generation)
	}
}

// SyncedAt writes the time of a status sync into every condition, as
// etcd-druid's periodic sync does (internal/health/condition/builder.go#L84-L103).
func SyncedAt(at time.Time) Option {
	return func(_ string, _ int64, s *druidv1alpha1.EtcdStatus) {
		for i := range s.Conditions {
			s.Conditions[i].LastUpdateTime = metav1.NewTime(at)
		}
	}
}

// NotAllUpdated reports a member that does not run the Etcd's spec yet
// (internal/health/condition/check_all_members_updated.go#L48-L51).
func NotAllUpdated() Option {
	return func(_ string, _ int64, s *druidv1alpha1.EtcdStatus) {
		i := slices.IndexFunc(s.Conditions, func(c druidv1alpha1.Condition) bool {
			return c.Type == druidv1alpha1.ConditionTypeAllMembersUpdated
		})
		s.Conditions[i].Status, s.Conditions[i].Reason, s.Conditions[i].Message = druidv1alpha1.ConditionFalse, "NotAllMembersUpdated", "At least one member is not yet updated"
	}
}

// LastError reports an error of the last reconcile
// (internal/controller/utils/etcdstatus.go#L76-L94).
func LastError(code druidapicommon.ErrorCode, description string) Option {
	return func(_ string, _ int64, s *druidv1alpha1.EtcdStatus) {
		s.LastErrors = []druidapicommon.LastError{{Code: code, Description: description, ObservedAt: metav1.Now()}}
	}
}

// Scale reports a scale in flight for the reason given
// (internal/controller/etcd/reconcile_scale.go#L51-L96), and for an empty
// one a scale completed (internal/controller/etcd/reconcile_spec.go#L124-L144).
func Scale(reason string) Option {
	return func(_ string, _ int64, s *druidv1alpha1.EtcdStatus) {
		complete := druidv1alpha1.Condition{
			Type: druidv1alpha1.ConditionTypeScaleOperationComplete, Status: druidv1alpha1.ConditionTrue, Reason: druidv1alpha1.ScaleOperationReasonNoScaleOperation,
			LastTransitionTime: metav1.Now(), LastUpdateTime: metav1.Now(),
		}
		if reason != "" {
			complete.Status, complete.Reason = druidv1alpha1.ConditionFalse, reason
		}
		s.Conditions = append(s.Conditions, complete)
	}
}

// BackupReady reports etcd-druid's BackupReady
// (internal/health/condition/check_backup_ready.go#L34-L125).
func BackupReady(status druidv1alpha1.ConditionStatus, reason, message string) Option {
	return func(_ string, _ int64, s *druidv1alpha1.EtcdStatus) {
		s.Conditions = append(s.Conditions, druidv1alpha1.Condition{
			Type: druidv1alpha1.ConditionTypeBackupReady, Status: status, Reason: reason, Message: message, LastTransitionTime: metav1.Now(), LastUpdateTime: metav1.Now(),
		})
	}
}

// Ready is etcd-druid's Ready condition, a quorum of the members ready or
// not, turned so at transition and written at update
// (internal/health/condition/check_ready.go#L42-L56).
func Ready(quorate bool, transition, update time.Time) druidv1alpha1.Condition {
	ready := druidv1alpha1.Condition{
		Type: druidv1alpha1.ConditionTypeReady, Status: druidv1alpha1.ConditionFalse, Reason: "QuorumLost", Message: "The majority of ETCD members is not ready",
		LastTransitionTime: metav1.NewTime(transition), LastUpdateTime: metav1.NewTime(update),
	}
	if quorate {
		ready.Status, ready.Reason, ready.Message = druidv1alpha1.ConditionTrue, "Quorate", "The majority of ETCD members is ready"
	}
	return ready
}

// Condition adds a condition Ready gave.
func Condition(condition druidv1alpha1.Condition) Option {
	return func(_ string, _ int64, s *druidv1alpha1.EtcdStatus) {
		s.Conditions = append(s.Conditions, condition)
	}
}

// Run writes the last operation of a spec reconcile under run in state at, as
// etcd-druid writes one at the start and at the end of every spec reconcile
// (internal/controller/utils/etcdstatus.go#L45-L94).
func Run(ctx context.Context, c client.Client, namespace, cp, run string, state druidapicommon.LastOperationState, at time.Time) error {
	return lastOperation(ctx, c, namespace, cp, druidapicommon.LastOperation{RunID: run, Type: druidv1alpha1.LastOperationTypeReconcile, State: state, Description: "etcd-druid's stand-in", LastUpdateTime: metav1.NewTime(at)})
}

func lastOperation(ctx context.Context, c client.Client, namespace, cp string, last druidapicommon.LastOperation) error {
	content, err := json.Marshal(last)
	if err != nil {
		return err
	}
	etcd := &druidv1alpha1.Etcd{ObjectMeta: metav1.ObjectMeta{Name: contract.EtcdName(cp), Namespace: namespace}}
	return c.Status().Patch(ctx, etcd, client.RawPatch(types.MergePatchType, []byte(`{"status":{"lastOperation":`+string(content)+`}}`)), client.FieldOwner("etcd-druid"))
}

// Served removes etcd-druid's annotation, as the end of every spec reconcile
// that succeeded does (internal/controller/etcd/reconcile_complete.go#L44-L55).
func Served(ctx context.Context, c client.Client, namespace, cp string) error {
	etcd := &druidv1alpha1.Etcd{ObjectMeta: metav1.ObjectMeta{Name: contract.EtcdName(cp), Namespace: namespace}}
	patch := client.RawPatch(types.MergePatchType, []byte(`{"metadata":{"annotations":{"`+druidv1alpha1.DruidOperationAnnotation+`":null}}}`))
	return c.Patch(ctx, etcd, patch, client.FieldOwner("etcd-druid"))
}

// FinishSnapshot writes the state an EtcdOpsTask of name ended in at
// (internal/controller/etcdopstask/utils.go#L86-L97).
func FinishSnapshot(ctx context.Context, c client.Client, namespace, name string, state druidv1alpha1.TaskState, at time.Time) error {
	task := &druidv1alpha1.EtcdOpsTask{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, task); err != nil {
		return err
	}
	task.Status.State = new(state)
	task.Status.LastTransitionTime = new(metav1.NewTime(at))
	return c.Status().Update(ctx, task)
}

// StartSnapshot writes that etcd-druid took up an EtcdOpsTask of name at
// (internal/controller/etcdopstask/utils.go#L91-L94).
func StartSnapshot(ctx context.Context, c client.Client, namespace, name string, at time.Time) error {
	task := &druidv1alpha1.EtcdOpsTask{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, task); err != nil {
		return err
	}
	task.Status.State, task.Status.StartedAt = new(druidv1alpha1.TaskStateInProgress), new(metav1.NewTime(at))
	return c.Status().Update(ctx, task)
}

// FinishCopy writes the condition an EtcdCopyBackupsTask of name ended with
// at, Succeeded or Failed, carried over from its Job's
// (internal/controller/etcdcopybackupstask/reconciler.go#L249-L274).
func FinishCopy(ctx context.Context, c client.Client, namespace, name string, conditionType druidv1alpha1.ConditionType, message string, at time.Time) error {
	task := &druidv1alpha1.EtcdCopyBackupsTask{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, task); err != nil {
		return err
	}
	task.Status.Conditions = []druidv1alpha1.Condition{{
		Type: conditionType, Status: druidv1alpha1.ConditionTrue, Reason: "Job" + string(conditionType), Message: message,
		LastTransitionTime: metav1.NewTime(at), LastUpdateTime: metav1.NewTime(at),
	}}
	return c.Status().Update(ctx, task)
}
