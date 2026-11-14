// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package v1alpha1

// Condition types, each with its one writer.
const (
	ConditionCertificatesReady     = "CertificatesReady"     // PKI
	ConditionAccessReady           = "AccessReady"           // PKI
	ConditionCAValidityAcceptable  = "CAValidityAcceptable"  // PKI
	ConditionDatastoreReady        = "DatastoreReady"        // Datastore
	ConditionAPIServerReady        = "APIServerReady"        // Workloads
	ConditionRoutesReady           = "RoutesReady"           // Workloads, in gateway mode only
	ConditionAddonsApplied         = "AddonsApplied"         // Tenant
	ConditionAddonsAvailable       = "AddonsAvailable"       // Tenant
	ConditionReachable             = "Reachable"             // Health
	ConditionControllersLeading    = "ControllersLeading"    // Health
	ConditionControllerManagerHeld = "ControllerManagerHeld" // Health
	ConditionBackupReady           = "BackupReady"           // Datastore
	ConditionProgressing           = "Progressing"           // Lifecycle
	ConditionReady                 = "Ready"                 // Lifecycle
)

// Reasons several conditions share. Reconciling is a writer's own first
// pass, Unknown, at the start of its first visit. WaitingFor<X> is an input
// awaited, False: X is a condition type without "Ready", or Ready itself
// (WaitingForReady), a spec or status field (WaitingForWindow,
// WaitingForNodes), or the kind of a named object that is not ready yet.
// Unsupported is what this release does not build, the field named in the
// message; <Capability>Unsupported is an environment that cannot serve the
// spec. <Kind>NotFound is a named object that does not exist. Refused is the
// API server a controller writes to refusing one of its objects, False, the
// object named in the message.
// ObjectsControlledElsewhere is an object under a writer's names that another
// owner keeps, False, the object and its owner named in the message.
// DatastoreReady is True with either, or with SecretNotFound, while the etcd
// serves but its class's credentials copy cannot be made.
const (
	ReasonReconciling                = "Reconciling"
	ReasonReady                      = "Ready"
	ReasonUnsupported                = "Unsupported"
	ReasonDeleting                   = "Deleting"
	ReasonRefused                    = "Refused"
	ReasonObjectsControlledElsewhere = "ObjectsControlledElsewhere"
	ReasonWaitingForEndpoint         = "WaitingForEndpoint"
	ReasonWaitingForTarget           = "WaitingForTarget"
	ReasonWaitingForCertificates     = "WaitingForCertificates"
	ReasonWaitingForDatastore        = "WaitingForDatastore"
	ReasonWaitingForAPIServer        = "WaitingForAPIServer"
	ReasonWaitingForAccess           = "WaitingForAccess"
	ReasonWaitingForBackup           = "WaitingForBackup"
	ReasonNoBackupStore              = "NoBackupStore"
	ReasonFailed                     = "Failed"
)

// CertificatesReady.
const (
	ReasonIssued                     = "Issued"
	ReasonIssuing                    = "Issuing"
	ReasonCAMissing                  = "CAMissing"
	ReasonCAExpired                  = "CAExpired"
	ReasonSecretsControlledElsewhere = "SecretsControlledElsewhere"
	ReasonSecretUnreadable           = "SecretUnreadable"
	ReasonSecretsExist               = "SecretsExist"
)

// CAValidityAcceptable.
const (
	ReasonValid      = "Valid"
	ReasonCAExpiring = "CAExpiring"
)

// AccessReady.
const (
	ReasonBound   = "Bound"
	ReasonBinding = "Binding"
)

// DatastoreReady.
const (
	ReasonProvisioning            = "Provisioning"
	ReasonDatastoreClassNotFound  = "DatastoreClassNotFound"
	ReasonDatastoreClassAmbiguous = "DatastoreClassAmbiguous"
	ReasonVolumesExist            = "VolumesExist"
	ReasonQuorumLost              = "QuorumLost"
	ReasonRestoring               = "Restoring"
	ReasonBackupNotFound          = "BackupNotFound"
	ReasonBackupIncompatible      = "BackupIncompatible"
	ReasonRestoreFailed           = "RestoreFailed"
	ReasonCAPredatesControlPlane  = "CAPredatesControlPlane"
	ReasonSecretNotFound          = "SecretNotFound"
)

// APIServerReady.
const (
	ReasonUnavailable                     = "Unavailable"
	ReasonProgressDeadlineExceeded        = "ProgressDeadlineExceeded"
	ReasonLoadBalancerHostnameUnsupported = "LoadBalancerHostnameUnsupported"
)

// RoutesReady: Accepted, True, when every route of the ControlPlane is
// applied, Accepted and ResolvedRefs at its generation, a ResolvedRefs False
// with reason EndpointsNotFound counting as resolved; Refused,
// ObjectsControlledElsewhere and Reconciling as above.
const (
	ReasonAccepted           = "Accepted"
	ReasonGatewayNotFound    = "GatewayNotFound"
	ReasonGatewayUnsupported = "GatewayUnsupported"
	ReasonRouteNotAccepted   = "RouteNotAccepted"
	ReasonHostnameConflict   = "HostnameConflict"
)

// AddonsApplied and AddonsAvailable.
const (
	ReasonApplied        = "Applied"
	ReasonApplying       = "Applying"
	ReasonAvailable      = "Available"
	ReasonRolloutPending = "RolloutPending"
)

// Reachable.
const (
	ReasonReachable           = "Reachable"
	ReasonExternalProbeFailed = "ExternalProbeFailed"
	ReasonInternalProbeFailed = "InternalProbeFailed"
)

// ControllersLeading.
const (
	ReasonLeading                     = "Leading"
	ReasonHeld                        = "Held"
	ReasonControllerManagerNotLeading = "ControllerManagerNotLeading"
	ReasonSchedulerNotLeading         = "SchedulerNotLeading"
	ReasonAwaitingRelease             = "AwaitingRelease"
	ReasonSourceExists                = "SourceExists"
)

// BackupReady.
const (
	ReasonBackupSucceeded  = "BackupSucceeded"
	ReasonBackupFailed     = "BackupFailed"
	ReasonSnapshotsPending = "SnapshotsPending"
)

// A Backup's Ready.
const (
	ReasonControlPlaneNotFound = "ControlPlaneNotFound"
	ReasonSnapshotting         = "Snapshotting"
	ReasonCopying              = "Copying"
	ReasonCompleted            = "Completed"
	ReasonSealLost             = "SealLost"
)

// A KubeconfigRequest's Ready.
const (
	ReasonRequestInvalid    = "RequestInvalid"
	ReasonPolicyNotEnforced = "PolicyNotEnforced"
)

// ControllerManagerHeld.
const (
	ReasonNodesSilent  = "NodesSilent"
	ReasonNotHeld      = "NotHeld"
	ReasonHoldBlocked  = "HoldBlocked"
	ReasonDisabled     = "Disabled"
	ReasonTooManyNodes = "TooManyNodes"
)

// Progressing and Ready.
const (
	ReasonIdle                        = "Idle"
	ReasonPaused                      = "Paused"
	ReasonRollingOut                  = "RollingOut"
	ReasonUpgrading                   = "Upgrading"
	ReasonMaintaining                 = "Maintaining"
	ReasonWaitingForWindow            = "WaitingForWindow"
	ReasonWaitingForReady             = "WaitingForReady"
	ReasonFleetLimit                  = "FleetLimit"
	ReasonWaitingForNodes             = "WaitingForNodes"
	ReasonControllerManagerHeld       = "ControllerManagerHeld"
	ReasonTimeZoneUnsupported         = "TimeZoneUnsupported"
	ReasonMaintenanceFailed           = "MaintenanceFailed"
	ReasonLatestGenerationNotObserved = "LatestGenerationNotObserved"
)

// WarningGroup is what a reason of WarningReasons reports.
type WarningGroup string

const (
	// WarningRefusal is a spec, or an object a controller finds, that it does
	// not serve as it is.
	WarningRefusal WarningGroup = "Refusal"
	// WarningFailure is a change or an operation that did not complete.
	WarningFailure WarningGroup = "Failure"
	// WarningLoss is something that served and no longer does, or will not.
	WarningLoss WarningGroup = "Loss"
)

// WarningReasons are the reasons whose event can be Warning, by group; every
// other reason's event is Normal. A failure's event is Warning whatever it
// replaces; a refusal's or a loss's only when the condition it replaces is
// True, or has a reason that is neither a first pass, a wait nor another of
// these (docs/decisions/metrics-are-part-of-the-api.md).
var WarningReasons = map[string]WarningGroup{
	ReasonUnsupported:                     WarningRefusal,
	ReasonLoadBalancerHostnameUnsupported: WarningRefusal,
	ReasonGatewayUnsupported:              WarningRefusal,
	ReasonTimeZoneUnsupported:             WarningRefusal,
	ReasonRefused:                         WarningRefusal,
	ReasonObjectsControlledElsewhere:      WarningRefusal,
	ReasonSecretsControlledElsewhere:      WarningRefusal,
	ReasonSecretsExist:                    WarningRefusal,
	ReasonDatastoreClassNotFound:          WarningRefusal,
	ReasonBackupNotFound:                  WarningRefusal,
	ReasonGatewayNotFound:                 WarningRefusal,
	ReasonControlPlaneNotFound:            WarningRefusal,
	ReasonSecretNotFound:                  WarningRefusal,
	ReasonDatastoreClassAmbiguous:         WarningRefusal,
	ReasonVolumesExist:                    WarningRefusal,
	ReasonBackupIncompatible:              WarningRefusal,
	ReasonCAPredatesControlPlane:          WarningRefusal,
	ReasonRouteNotAccepted:                WarningRefusal,
	ReasonHostnameConflict:                WarningRefusal,
	ReasonTooManyNodes:                    WarningRefusal,

	ReasonFailed:                   WarningFailure,
	ReasonMaintenanceFailed:        WarningFailure,
	ReasonRestoreFailed:            WarningFailure,
	ReasonBackupFailed:             WarningFailure,
	ReasonProgressDeadlineExceeded: WarningFailure,
	ReasonSealLost:                 WarningFailure,
	ReasonHoldBlocked:              WarningFailure,
	ReasonRequestInvalid:           WarningFailure,
	ReasonPolicyNotEnforced:        WarningFailure,

	ReasonQuorumLost:                  WarningLoss,
	ReasonUnavailable:                 WarningLoss,
	ReasonControllerManagerNotLeading: WarningLoss,
	ReasonSchedulerNotLeading:         WarningLoss,
	ReasonNodesSilent:                 WarningLoss,
	ReasonExternalProbeFailed:         WarningLoss,
	ReasonInternalProbeFailed:         WarningLoss,
	ReasonCAMissing:                   WarningLoss,
	ReasonCAExpired:                   WarningLoss,
	ReasonSecretUnreadable:            WarningLoss,
	ReasonCAExpiring:                  WarningLoss,
}
