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
// awaited, False: X is a condition type without "Ready", a spec or status
// field (WaitingForWindow, WaitingForNodes), or the kind of a named object
// that is not ready yet. Unsupported is what this release does not build,
// the field named in the message; <Capability>Unsupported is an environment
// that cannot serve the spec. <Kind>NotFound is a named object that does not
// exist. Refused is the API server a controller writes to refusing one of
// its objects, False, the object named in the message.
// ObjectsControlledElsewhere is an object under a writer's names that another
// owner keeps, False, the object and its owner named in the message.
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
)

// APIServerReady.
const (
	ReasonUnavailable                     = "Unavailable"
	ReasonProgressDeadlineExceeded        = "ProgressDeadlineExceeded"
	ReasonLoadBalancerHostnameUnsupported = "LoadBalancerHostnameUnsupported"
	ReasonGatewayNotFound                 = "GatewayNotFound"
	ReasonGatewayUnsupported              = "GatewayUnsupported"
	ReasonRouteNotAccepted                = "RouteNotAccepted"
	ReasonHostnameConflict                = "HostnameConflict"
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
)

// ControllerManagerHeld.
const (
	ReasonNodesSilent = "NodesSilent"
	ReasonNotHeld     = "NotHeld"
	ReasonHoldBlocked = "HoldBlocked"
	ReasonDisabled    = "Disabled"
)

// Progressing and Ready.
const (
	ReasonIdle                        = "Idle"
	ReasonPaused                      = "Paused"
	ReasonRollingOut                  = "RollingOut"
	ReasonUpgrading                   = "Upgrading"
	ReasonMaintaining                 = "Maintaining"
	ReasonWaitingForWindow            = "WaitingForWindow"
	ReasonFleetLimit                  = "FleetLimit"
	ReasonWaitingForNodes             = "WaitingForNodes"
	ReasonControllerManagerHeld       = "ControllerManagerHeld"
	ReasonTimeZoneUnsupported         = "TimeZoneUnsupported"
	ReasonMaintenanceFailed           = "MaintenanceFailed"
	ReasonLatestGenerationNotObserved = "LatestGenerationNotObserved"
)
