// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"context"
	"errors"
	"time"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	"go.lodia.io/lodia/internal/contract"
	"go.lodia.io/lodia/internal/jsonscan"
	"go.lodia.io/lodia/internal/tenantclient"
)

// Every response Health reads from a tenant is projected onto the types below
// by internal/jsonscan, which keeps only the fields the read uses.

const (
	coordinationPath = "/apis/coordination.k8s.io/v1/namespaces/"
	nodeLeasesPath   = coordinationPath + corev1.NamespaceNodeLease + "/leases"
	nodesPath        = "/api/v1/nodes"

	// maxConditions and maxWebhooks bound the arrays inside one object: a
	// Node's conditions and a webhook configuration's webhooks.
	maxConditions = 64
	maxWebhooks   = 256
)

type listOf[T any] struct {
	Metadata struct{ Continue string }
	Items    tenantclient.Array[T]
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
			out.Items, err = tenantclient.ArrayOf("items", value, most, element)
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
			*name, err = tenantclient.ObjectName("metadata.name", value)
		case string(key) == "namespace" && namespace != nil:
			*namespace, err = tenantclient.ObjectName("metadata.namespace", value)
		case string(key) == "resourceVersion" && resourceVersion != nil:
			*resourceVersion, err = jsonscan.Text("metadata.resourceVersion", value, tenantclient.MaxVersionRaw)
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
					l.Spec.LeaseDurationSeconds, err = jsonscan.Integer("spec.leaseDurationSeconds", value, tenantclient.MaxNumberRaw)
				case "acquireTime":
					l.Spec.AcquireTime, err = optionalMicroTime("spec.acquireTime", value)
				case "renewTime":
					l.Spec.RenewTime, err = optionalMicroTime("spec.renewTime", value)
				case "leaseTransitions":
					l.Spec.LeaseTransitions, err = jsonscan.Integer("spec.leaseTransitions", value, tenantclient.MaxNumberRaw)
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
		Conditions tenantclient.Array[nodeCondition]
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
							n.Status.NodeInfo.KubeletVersion = tenantclient.Cut(version, maxKubeletVersion)
						}
						return err
					})
				case "conditions":
					n.Status.Conditions, err = tenantclient.ArrayOf("status.conditions", value, maxConditions, nodeConditionOf)
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
// tenantclient.MaxWordRaw is another type and is not decoded.
func nodeConditionOf(data []byte) (nodeCondition, error) {
	var c nodeCondition
	var status, since []byte
	err := jsonscan.Walk("a condition", data, func(key, value []byte) (err error) {
		switch string(key) {
		case "type":
			var s string
			s, err = jsonscan.Word("status.conditions.type", value, tenantclient.MaxWordRaw)
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
		if s, err = jsonscan.Word("status.conditions.status", status, tenantclient.MaxWordRaw); err != nil {
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
					t, err = jsonscan.Word("spec.type", value, tenantclient.MaxWordRaw)
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
					p, err = jsonscan.Word("spec.persistentVolumeReclaimPolicy", value, tenantclient.MaxWordRaw)
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
					p, err = jsonscan.Word("status.phase", value, tenantclient.MaxWordRaw)
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
	Webhooks tenantclient.Array[webhook]
}

func webhookConfigurationOf(data []byte) (c webhookConfiguration, err error) {
	err = jsonscan.Walk("a webhook configuration", data, func(key, value []byte) (err error) {
		switch string(key) {
		case "metadata":
			err = metadata(value, &c.Metadata.Name, nil, nil)
		case "webhooks":
			c.Webhooks, err = tenantclient.ArrayOf("webhooks", value, maxWebhooks, webhookOf)
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
			w.Name, err = tenantclient.ObjectName("webhooks.name", value)
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
				p, err = jsonscan.Word("webhooks.failurePolicy", value, tenantclient.MaxWordRaw)
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
	client    *tenantclient.Client
	leasePage func(context.Context, metav1.ListOptions) (*leaseList, error)
	nodePage  func(context.Context, metav1.ListOptions) (*nodeList, error)
}

func newTenant(client *tenantclient.Client) *tenant {
	t := &tenant{client: client}
	t.leasePage = func(ctx context.Context, options metav1.ListOptions) (*leaseList, error) {
		return readList(ctx, t, nodeLeasesPath, options, leasePage, leaseItemOf)
	}
	t.nodePage = func(ctx context.Context, options metav1.ListOptions) (*nodeList, error) {
		return readList(ctx, t, nodesPath, options, nodePage, nodeOf)
	}
	return t
}

var errOverBound = errors.New("the response has more elements in an array than Health reads")

func readList[T any](ctx context.Context, t *tenant, path string, options metav1.ListOptions, most int, element func([]byte) (T, error)) (*listOf[T], error) {
	body, err := t.client.Do(ctx, t.client.Get().AbsPath(path).VersionedParams(&options, metav1.ParameterCodec))
	return tenantclient.Decode(body, err, func(body []byte) (*listOf[T], error) { return listFrom(body, most, element) })
}

// firstPage reads the first page of the list at path, which is no answer of a
// kube-apiserver when it holds more than listLimit items.
func firstPage[T any](ctx context.Context, t *tenant, path string, element func([]byte) (T, error)) (*listOf[T], error) {
	page, err := readList(ctx, t, path, metav1.ListOptions{Limit: listLimit}, listLimit, element)
	if err == nil && page.Items.Over {
		err = errOverBound
	}
	return page, err
}

func (t *tenant) getLease(ctx context.Context, name string) (*leaderLease, error) {
	body, err := t.client.Do(ctx, t.client.Get().AbsPath(coordinationPath, contract.LeaderElectionNamespace, "leases", name))
	return tenantclient.Decode(body, err, leaderLeaseFrom)
}

func (t *tenant) createLease(ctx context.Context, body []byte) (*leaderLease, error) {
	answer, err := t.client.Do(ctx, t.client.Post().AbsPath(coordinationPath, contract.LeaderElectionNamespace, "leases").SetHeader("Content-Type", "application/json").Body(body))
	return tenantclient.Decode(answer, err, leaderLeaseFrom)
}

func (t *tenant) patchLease(ctx context.Context, patch []byte) (*leaderLease, error) {
	body, err := t.client.Do(ctx, t.client.Patch(types.MergePatchType).AbsPath(coordinationPath, contract.LeaderElectionNamespace, "leases", contract.ControllerManagerLease).Body(patch))
	return tenantclient.Decode(body, err, leaderLeaseFrom)
}
