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
	ConditionProgressing           = "Progressing"           // Lifecycle
	ConditionReady                 = "Ready"                 // Lifecycle
)

// Reasons several conditions share. Reconciling is a writer's own first
// pass, Unknown. WaitingFor<X> is an input awaited, False: X is a condition
// type without "Ready", or a status field. Unsupported is what this release
// does not build, the field named in the message; <Capability>Unsupported is
// an environment that cannot serve the spec. <Kind>NotFound is a named
// object that does not exist. Refused is the API server a controller writes
// to refusing one of its objects, False, the object named in the message.
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
)

// CertificatesReady.
const (
	ReasonIssued                     = "Issued"
	ReasonIssuing                    = "Issuing"
	ReasonCAMissing                  = "CAMissing"
	ReasonCAExpired                  = "CAExpired"
	ReasonSecretsControlledElsewhere = "SecretsControlledElsewhere"
	ReasonSecretUnreadable           = "SecretUnreadable"
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
)

// APIServerReady.
const (
	ReasonUnavailable                     = "Unavailable"
	ReasonProgressDeadlineExceeded        = "ProgressDeadlineExceeded"
	ReasonLoadBalancerHostnameUnsupported = "LoadBalancerHostnameUnsupported"
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
	ReasonLatestGenerationNotObserved = "LatestGenerationNotObserved"
)
