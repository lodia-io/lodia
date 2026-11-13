// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/jsonscan"
)

// Every response Health reads from a tenant is projected onto the types below
// by internal/jsonscan, which keeps only the fields the read uses.

const (
	coordinationPath = "/apis/coordination.k8s.io/v1/namespaces/"
	nodeLeasesPath   = coordinationPath + corev1.NamespaceNodeLease + "/leases"
	nodesPath        = "/api/v1/nodes"

	// maxConditions, maxWebhooks and maxCauses bound the arrays inside one
	// object: a Node's conditions, a webhook configuration's webhooks and a
	// Status's causes.
	maxConditions = 64
	maxWebhooks   = 256
	maxCauses     = 64
)

type listOf[T any] struct {
	Metadata struct{ Continue string }
	Items    array[T]
}

type (
	leaseList = listOf[leaseItem]
	nodeList  = listOf[nodeObject]
)

func listFrom[T any](data []byte, most int, element func([]byte) (T, error)) (*listOf[T], error) {
	out := new(listOf[T])
	err := jsonscan.Walk("the list", data, func(key, value []byte) (err error) {
		switch string(key) {
		case "metadata":
			err = jsonscan.Walk("metadata", value, func(key, value []byte) (err error) {
				if string(key) == "continue" {
					out.Metadata.Continue, err = jsonscan.Text("metadata.continue", value, maxContinue)
				}
				return err
			})
		case "items":
			out.Items, err = arrayOf("items", value, most, element)
		}
		return err
	})
	return out, err
}

// metadata reads the name, the namespace and the resourceVersion of an object
// into the places it is given, and skips the rest.
func metadata(data []byte, name, namespace, resourceVersion *string) error {
	return jsonscan.Walk("metadata", data, func(key, value []byte) (err error) {
		switch {
		case string(key) == "name" && name != nil:
			*name, err = objectName("metadata.name", value)
		case string(key) == "namespace" && namespace != nil:
			*namespace, err = objectName("metadata.namespace", value)
		case string(key) == "resourceVersion" && resourceVersion != nil:
			*resourceVersion, err = jsonscan.Text("metadata.resourceVersion", value, maxVersionRaw)
		}
		return err
	})
}

type leaseItem struct {
	Metadata struct{ Name, ResourceVersion string }
}

func leaseItemOf(data []byte) (l leaseItem, err error) {
	err = jsonscan.Walk("a Lease", data, func(key, value []byte) error {
		if string(key) == "metadata" {
			return metadata(value, &l.Metadata.Name, nil, &l.Metadata.ResourceVersion)
		}
		return nil
	})
	return l, err
}

type leaderLease struct {
	Metadata struct{ ResourceVersion string }
	Spec     struct {
		HolderIdentity         *string
		LeaseDurationSeconds   *int32
		AcquireTime, RenewTime *metav1.MicroTime
		LeaseTransitions       *int32
	}
}

func leaderLeaseFrom(data []byte) (*leaderLease, error) {
	l := new(leaderLease)
	err := jsonscan.Walk("the Lease", data, func(key, value []byte) (err error) {
		switch string(key) {
		case "metadata":
			err = metadata(value, nil, nil, &l.Metadata.ResourceVersion)
		case "spec":
			err = jsonscan.Walk("spec", value, func(key, value []byte) (err error) {
				switch string(key) {
				case "holderIdentity":
					l.Spec.HolderIdentity, err = optionalText("spec.holderIdentity", value, maxHolderRaw)
				case "leaseDurationSeconds":
					l.Spec.LeaseDurationSeconds, err = jsonscan.Integer("spec.leaseDurationSeconds", value, maxNumberRaw)
				case "acquireTime":
					l.Spec.AcquireTime, err = optionalMicroTime("spec.acquireTime", value)
				case "renewTime":
					l.Spec.RenewTime, err = optionalMicroTime("spec.renewTime", value)
				case "leaseTransitions":
					l.Spec.LeaseTransitions, err = jsonscan.Integer("spec.leaseTransitions", value, maxNumberRaw)
				}
				return err
			})
		}
		return err
	})
	return l, err
}

type nodeObject struct {
	Metadata struct{ Name string }
	Status   struct {
		NodeInfo   struct{ KubeletVersion string }
		Conditions array[nodeCondition]
	}
}

func nodeOf(data []byte) (n nodeObject, err error) {
	err = jsonscan.Walk("a Node", data, func(key, value []byte) (err error) {
		switch string(key) {
		case "metadata":
			err = metadata(value, &n.Metadata.Name, nil, nil)
		case "status":
			err = jsonscan.Walk("status", value, func(key, value []byte) (err error) {
				switch string(key) {
				case "nodeInfo":
					err = jsonscan.Walk("status.nodeInfo", value, func(key, value []byte) (err error) {
						if string(key) == "kubeletVersion" {
							var version string
							version, err = jsonscan.Text("status.nodeInfo.kubeletVersion", value, maxKubeletRaw)
							n.Status.NodeInfo.KubeletVersion = cut(version, maxKubeletVersion)
						}
						return err
					})
				case "conditions":
					n.Status.Conditions, err = arrayOf("status.conditions", value, maxConditions, nodeConditionOf)
				}
				return err
			})
		}
		return err
	})
	return n, err
}

type nodeCondition struct {
	Type               corev1.NodeConditionType
	Status             corev1.ConditionStatus
	LastTransitionTime metav1.Time
}

// nodeConditionOf reads a Ready condition's status and time and keeps nothing
// of a condition of another type, which it counts and no more. A Node's writer
// chooses the type, and the API server accepts one of any length, so a type past
// maxWordRaw is another type and is not decoded.
func nodeConditionOf(data []byte) (nodeCondition, error) {
	var c nodeCondition
	var status, since []byte
	err := jsonscan.Walk("a condition", data, func(key, value []byte) (err error) {
		switch string(key) {
		case "type":
			var s string
			s, err = jsonscan.Word("status.conditions.type", value, maxWordRaw)
			c.Type = corev1.NodeConditionType(s)
		case "status":
			status = value
		case "lastTransitionTime":
			since = value
		}
		return err
	})
	if err != nil || c.Type != corev1.NodeReady {
		return nodeCondition{}, err
	}
	if status != nil {
		var s string
		if s, err = jsonscan.Word("status.conditions.status", status, maxWordRaw); err != nil {
			return nodeCondition{}, err
		}
		c.Status = corev1.ConditionStatus(s)
	}
	if since != nil {
		var t time.Time
		t, err = timeOf("status.conditions.lastTransitionTime", since, time.RFC3339)
		c.LastTransitionTime = metav1.Time{Time: t}
	}
	return c, err
}

type (
	named      struct{ Name string }
	namespaced struct{ Namespace, Name string }
)

type service struct {
	Metadata namespaced
	Spec     struct{ Type corev1.ServiceType }
}

func serviceOf(data []byte) (s service, err error) {
	err = jsonscan.Walk("a Service", data, func(key, value []byte) error {
		switch string(key) {
		case "metadata":
			return metadata(value, &s.Metadata.Name, &s.Metadata.Namespace, nil)
		case "spec":
			return jsonscan.Walk("spec", value, func(key, value []byte) (err error) {
				if string(key) == "type" {
					var t string
					t, err = jsonscan.Word("spec.type", value, maxWordRaw)
					s.Spec.Type = corev1.ServiceType(t)
				}
				return err
			})
		}
		return nil
	})
	return s, err
}

type persistentVolume struct {
	Metadata named
	Spec     struct {
		PersistentVolumeReclaimPolicy corev1.PersistentVolumeReclaimPolicy
	}
}

func persistentVolumeOf(data []byte) (v persistentVolume, err error) {
	err = jsonscan.Walk("a PersistentVolume", data, func(key, value []byte) error {
		switch string(key) {
		case "metadata":
			return metadata(value, &v.Metadata.Name, nil, nil)
		case "spec":
			return jsonscan.Walk("spec", value, func(key, value []byte) (err error) {
				if string(key) == "persistentVolumeReclaimPolicy" {
					var p string
					p, err = jsonscan.Word("spec.persistentVolumeReclaimPolicy", value, maxWordRaw)
					v.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimPolicy(p)
				}
				return err
			})
		}
		return nil
	})
	return v, err
}

type persistentVolumeClaim struct {
	Metadata namespaced
	Status   struct {
		Phase corev1.PersistentVolumeClaimPhase
	}
}

func persistentVolumeClaimOf(data []byte) (c persistentVolumeClaim, err error) {
	err = jsonscan.Walk("a PersistentVolumeClaim", data, func(key, value []byte) error {
		switch string(key) {
		case "metadata":
			return metadata(value, &c.Metadata.Name, &c.Metadata.Namespace, nil)
		case "status":
			return jsonscan.Walk("status", value, func(key, value []byte) (err error) {
				if string(key) == "phase" {
					var p string
					p, err = jsonscan.Word("status.phase", value, maxWordRaw)
					c.Status.Phase = corev1.PersistentVolumeClaimPhase(p)
				}
				return err
			})
		}
		return nil
	})
	return c, err
}

type webhookConfiguration struct {
	Metadata named
	Webhooks array[webhook]
}

func webhookConfigurationOf(data []byte) (c webhookConfiguration, err error) {
	err = jsonscan.Walk("a webhook configuration", data, func(key, value []byte) (err error) {
		switch string(key) {
		case "metadata":
			err = metadata(value, &c.Metadata.Name, nil, nil)
		case "webhooks":
			c.Webhooks, err = arrayOf("webhooks", value, maxWebhooks, webhookOf)
		}
		return err
	})
	return c, err
}

type webhook struct {
	Name          string
	ClientConfig  struct{ URL present }
	FailurePolicy *admissionregistrationv1.FailurePolicyType
}

func webhookOf(data []byte) (w webhook, err error) {
	err = jsonscan.Walk("a webhook", data, func(key, value []byte) (err error) {
		switch string(key) {
		case "name":
			w.Name, err = objectName("webhooks.name", value)
		case "clientConfig":
			err = jsonscan.Walk("webhooks.clientConfig", value, func(key, value []byte) error {
				if string(key) == "url" {
					w.ClientConfig.URL.set(value)
				}
				return nil
			})
		case "failurePolicy":
			if !jsonscan.Null(value) {
				var p string
				p, err = jsonscan.Word("webhooks.failurePolicy", value, maxWordRaw)
				w.FailurePolicy = new(admissionregistrationv1.FailurePolicyType(p))
			}
		}
		return err
	})
	return w, err
}

// actsOutside says whether a webhook calls a URL, which may be outside the
// cluster, or fails closed, Fail being admissionregistration/v1's default
// (https://github.com/kubernetes/api/blob/v0.37.1/admissionregistration/v1/types.go#L814-L817).
func (w webhook) actsOutside() bool {
	return bool(w.ClientConfig.URL) || ptr.Deref(w.FailurePolicy, admissionregistrationv1.Fail) == admissionregistrationv1.Fail
}

type apiService struct {
	Metadata named
	Spec     struct{ Service present }
}

func apiServiceOf(data []byte) (a apiService, err error) {
	err = jsonscan.Walk("an APIService", data, func(key, value []byte) error {
		switch string(key) {
		case "metadata":
			return metadata(value, &a.Metadata.Name, nil, nil)
		case "spec":
			return jsonscan.Walk("spec", value, func(key, value []byte) error {
				if string(key) == "service" {
					a.Spec.Service.set(value)
				}
				return nil
			})
		}
		return nil
	})
	return a, err
}

type cronJob struct {
	Metadata namespaced
	Spec     struct{ Suspend *bool }
}

func cronJobOf(data []byte) (j cronJob, err error) {
	err = jsonscan.Walk("a CronJob", data, func(key, value []byte) error {
		switch string(key) {
		case "metadata":
			return metadata(value, &j.Metadata.Name, &j.Metadata.Namespace, nil)
		case "spec":
			return jsonscan.Walk("spec", value, func(key, value []byte) (err error) {
				if string(key) == "suspend" {
					j.Spec.Suspend, err = jsonscan.Boolean("spec.suspend", value)
				}
				return err
			})
		}
		return nil
	})
	return j, err
}

// tenant reads inside a control plane as Lodia, over one transport. The pages
// of the two lists the pass samples are fields so that a test can watch what
// they decode.
type tenant struct {
	api       *rest.RESTClient
	leasePage func(context.Context, metav1.ListOptions) (*leaseList, error)
	nodePage  func(context.Context, metav1.ListOptions) (*nodeList, error)
}

func newTenant(api *rest.RESTClient) *tenant {
	t := &tenant{api: api}
	t.leasePage = func(ctx context.Context, options metav1.ListOptions) (*leaseList, error) {
		return readList(ctx, t, nodeLeasesPath, options, leasePage, leaseItemOf)
	}
	t.nodePage = func(ctx context.Context, options metav1.ListOptions) (*nodeList, error) {
		return readList(ctx, t, nodesPath, options, nodePage, nodeOf)
	}
	return t
}

var (
	errOverBound = errors.New("the response has more elements in an array than Health reads")
	errNotJSON   = errors.New("the response is not JSON")

	// longMessage is a Status's message past maxMessageRaw.
	longMessage = "the server's message is longer than " + strconv.Itoa(maxMessageRaw) + " bytes"
)

func readList[T any](ctx context.Context, t *tenant, path string, options metav1.ListOptions, most int, element func([]byte) (T, error)) (*listOf[T], error) {
	body, err := t.do(ctx, t.api.Get().AbsPath(path).VersionedParams(&options, metav1.ParameterCodec))
	return decode(body, err, func(body []byte) (*listOf[T], error) { return listFrom(body, most, element) })
}

// firstPage reads the first page of the list at path, which is no answer of a
// kube-apiserver when it holds more than listLimit items.
func firstPage[T any](ctx context.Context, t *tenant, path string, element func([]byte) (T, error)) (*listOf[T], error) {
	page, err := readList(ctx, t, path, metav1.ListOptions{Limit: listLimit}, listLimit, element)
	if err == nil && page.Items.over {
		err = errOverBound
	}
	return page, err
}

func (t *tenant) getLease(ctx context.Context, name string) (*leaderLease, error) {
	body, err := t.do(ctx, t.api.Get().AbsPath(coordinationPath, lodiav1alpha1.LeaderElectionNamespace, "leases", name))
	return decode(body, err, leaderLeaseFrom)
}

func (t *tenant) createLease(ctx context.Context, body []byte) (*leaderLease, error) {
	answer, err := t.do(ctx, t.api.Post().AbsPath(coordinationPath, lodiav1alpha1.LeaderElectionNamespace, "leases").SetHeader("Content-Type", "application/json").Body(body))
	return decode(answer, err, leaderLeaseFrom)
}

func (t *tenant) patchLease(ctx context.Context, patch []byte) (*leaderLease, error) {
	body, err := t.do(ctx, t.api.Patch(types.MergePatchType).AbsPath(coordinationPath, lodiav1alpha1.LeaderElectionNamespace, "leases", lodiav1alpha1.ControllerManagerLease).Body(patch))
	return decode(body, err, leaderLeaseFrom)
}

// do sends the request once, which asks for JSON, and returns the body of the
// answer: client-go sends a request again up to ten times on a 429 or a 5xx
// that carries Retry-After, and the next pass is Health's retry. A body cut at
// maxResponseBytes is errResponseTooLong whatever the status. A Status of
// Failure is the error of a failed request, as client-go's Result.Error reads
// it, but into the fields Health reads. Any other body of a failed answer is
// the tenant's text, which client-go puts in its error, and which no log or
// message of Health may carry: see noStatus.
func (t *tenant) do(ctx context.Context, request *rest.Request) ([]byte, error) {
	var cut bool
	body, err := request.MaxRetries(0).DoRaw(context.WithValue(ctx, cutKey{}, &cut))
	if cut {
		return nil, errResponseTooLong
	}
	if apierrors.IsUnexpectedServerError(err) {
		if failure, ok := failureOf(body); ok {
			return nil, &apierrors.StatusError{ErrStatus: failure}
		}
		return nil, noStatus(err)
	}
	return body, err
}

const noStatusMessage = "the answer is no Status Health can read"

// noStatus is the error client-go built of a failed answer that is no Status
// within Health's bounds, without the tenant's text: its code, reason and
// Retry-After, and a message of Health's own, in the place of the body.
func noStatus(err error) error {
	var answered apierrors.APIStatus
	errors.As(err, &answered)
	status := answered.Status()
	return &apierrors.StatusError{ErrStatus: metav1.Status{
		Status:  metav1.StatusFailure,
		Code:    status.Code,
		Reason:  status.Reason,
		Message: noStatusMessage,
		Details: &metav1.StatusDetails{
			Causes:            []metav1.StatusCause{{Type: metav1.CauseTypeUnexpectedServerResponse}},
			RetryAfterSeconds: status.Details.RetryAfterSeconds,
		},
	}}
}

type statusBody struct {
	Status, Message string
	Reason          metav1.StatusReason
	Code            int32
	Details         struct {
		Causes array[metav1.CauseType]
	}
}

func statusFrom(data []byte) (s statusBody, err error) {
	err = jsonscan.Walk("the Status", data, func(key, value []byte) (err error) {
		switch string(key) {
		case "status":
			s.Status, err = jsonscan.Word("status", value, maxWordRaw)
		case "message":
			s.Message, err = jsonscan.Text("message", value, maxMessageRaw)
			if tooLong := new(jsonscan.TooLong); errors.As(err, &tooLong) {
				s.Message, err = longMessage, nil
			}
		case "reason":
			var reason string
			reason, err = jsonscan.Text("reason", value, maxWordRaw)
			s.Reason = metav1.StatusReason(reason)
		case "code":
			var code *int32
			if code, err = jsonscan.Integer("code", value, maxNumberRaw); code != nil {
				s.Code = *code
			}
		case "details":
			err = jsonscan.Walk("details", value, func(key, value []byte) (err error) {
				if string(key) == "causes" {
					s.Details.Causes, err = arrayOf("details.causes", value, maxCauses, causeOf)
				}
				return err
			})
		}
		return err
	})
	return s, err
}

// causeOf reads a cause's type, which a kube-apiserver writes under the key
// reason (metav1.StatusCause).
func causeOf(data []byte) (c metav1.CauseType, err error) {
	err = jsonscan.Walk("a cause", data, func(key, value []byte) (err error) {
		if string(key) == "reason" {
			var reason string
			reason, err = jsonscan.Text("details.causes.reason", value, maxWordRaw)
			c = metav1.CauseType(reason)
		}
		return err
	})
	return c, err
}

func failureOf(body []byte) (metav1.Status, bool) {
	if !json.Valid(body) {
		return metav1.Status{}, false
	}
	s, err := statusFrom(body)
	if err != nil || s.Status != metav1.StatusFailure {
		return metav1.Status{}, false
	}
	failure := metav1.Status{Status: s.Status, Message: s.Message, Reason: s.Reason, Code: s.Code}
	for _, cause := range s.Details.Causes.items {
		if failure.Details == nil {
			failure.Details = &metav1.StatusDetails{}
		}
		failure.Details.Causes = append(failure.Details.Causes, metav1.StatusCause{Type: cause})
	}
	return failure, true
}

// decode reads a body, which encoding/json validates and nothing decodes, with
// the projection of its kind. A projection that fails returns what it read
// before the failure, with the error.
func decode[T any](body []byte, err error, project func([]byte) (*T, error)) (*T, error) {
	if err != nil {
		return nil, err
	}
	if !json.Valid(body) {
		return nil, errNotJSON
	}
	out, err := project(body)
	if err != nil {
		return out, fmt.Errorf("read the response: %w", err)
	}
	return out, nil
}
