// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

// Every response Health reads from a tenant decodes into a type below, which
// holds only the fields the read uses: the administrator of a tenant can have
// any server answer, so a response decodes into nothing the API defines
// (docs/review-checklist.md, item 10). encoding/json skips a field a type lacks
// without keeping it, and matches a key to the field of that name without
// regard to case, so the fields carry no tags. An array decodes one element at
// a time, up to the bound of its array type.

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

type bound interface{ most() int }

type (
	leaseItems     struct{}
	nodeItems      struct{}
	listItems      struct{}
	nodeConditions struct{}
	webhooks       struct{}
	statusCauses   struct{}
)

func (leaseItems) most() int     { return leasePage }
func (nodeItems) most() int      { return nodePage }
func (listItems) most() int      { return listLimit }
func (nodeConditions) most() int { return maxConditions }
func (webhooks) most() int       { return maxWebhooks }
func (statusCauses) most() int   { return maxCauses }

// array is a JSON array decoded one element at a time. It keeps the first
// B.most() elements, and over says there was one more, which it does not
// decode: the elements after the bound cost a scan and nothing else.
type array[T any, B bound] struct {
	items []T
	over  bool
}

func (a *array[T, B]) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		return nil
	}
	if data[0] != '[' {
		return errors.New("not an array")
	}
	var b B
	for element := range elements(data) {
		if len(a.items) == b.most() {
			a.over = true
			return nil
		}
		var item T
		if err := json.Unmarshal(element, &item); err != nil {
			return err
		}
		a.items = append(a.items, item)
	}
	return nil
}

// elements yields the elements of a JSON array that encoding/json has
// validated, each a slice of it: a json.Decoder would copy each element into a
// buffer of its own, and an element may be most of the response.
func elements(array []byte) iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		i := skipSpace(array, 1)
		for array[i] != ']' {
			end := endOfValue(array, i)
			if !yield(array[i:end]) {
				return
			}
			i = skipSpace(array, end)
			if array[i] == ',' {
				i = skipSpace(array, i+1)
			}
		}
	}
}

func skipSpace(data []byte, i int) int {
	for data[i] == ' ' || data[i] == '\t' || data[i] == '\n' || data[i] == '\r' {
		i++
	}
	return i
}

func endOfValue(data []byte, i int) int {
	switch data[i] {
	case '"':
		return endOfString(data, i)
	case '{', '[':
		for depth := 0; ; i++ {
			switch data[i] {
			case '"':
				i = endOfString(data, i) - 1
			case '{', '[':
				depth++
			case '}', ']':
				if depth--; depth == 0 {
					return i + 1
				}
			}
		}
	}
	for data[i] != ',' && data[i] != ']' && data[i] != ' ' && data[i] != '\t' && data[i] != '\n' && data[i] != '\r' {
		i++
	}
	return i
}

func endOfString(data []byte, i int) int {
	for i++; data[i] != '"'; i++ {
		if data[i] == '\\' {
			i++
		}
	}
	return i + 1
}

// present says only that a field is there and not null: the webhook's URL and
// the APIService's Service are read for that, not for their content.
type present bool

func (p *present) UnmarshalJSON(data []byte) error {
	*p = string(data) != "null"
	return nil
}

type listOf[T any, B bound] struct {
	Metadata struct{ Continue string }
	Items    array[T, B]
}

type (
	leaseList = listOf[leaseItem, leaseItems]
	nodeList  = listOf[nodeObject, nodeItems]
)

type leaseItem struct {
	Metadata struct{ Name, ResourceVersion string }
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

type nodeObject struct {
	Metadata struct{ Name string }
	Status   struct {
		NodeInfo   struct{ KubeletVersion string }
		Conditions array[nodeCondition, nodeConditions]
	}
}

type nodeCondition struct {
	Type               corev1.NodeConditionType
	Status             corev1.ConditionStatus
	LastTransitionTime metav1.Time
}

type (
	named      struct{ Name string }
	namespaced struct{ Namespace, Name string }
)

type service struct {
	Metadata namespaced
	Spec     struct{ Type corev1.ServiceType }
}

type persistentVolume struct {
	Metadata named
	Spec     struct {
		PersistentVolumeReclaimPolicy corev1.PersistentVolumeReclaimPolicy
	}
}

type persistentVolumeClaim struct {
	Metadata namespaced
	Status   struct {
		Phase corev1.PersistentVolumeClaimPhase
	}
}

type webhookConfiguration struct {
	Metadata named
	Webhooks array[webhook, webhooks]
}

type webhook struct {
	Name          string
	ClientConfig  struct{ URL present }
	FailurePolicy *admissionregistrationv1.FailurePolicyType
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

type cronJob struct {
	Metadata namespaced
	Spec     struct{ Suspend *bool }
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
		return readList[leaseItem, leaseItems](ctx, t, nodeLeasesPath, options)
	}
	t.nodePage = func(ctx context.Context, options metav1.ListOptions) (*nodeList, error) {
		return readList[nodeObject, nodeItems](ctx, t, nodesPath, options)
	}
	return t
}

var errOverBound = errors.New("the response has more elements in an array than Health reads")

func readList[T any, B bound](ctx context.Context, t *tenant, path string, options metav1.ListOptions) (*listOf[T, B], error) {
	body, err := t.do(ctx, t.api.Get().AbsPath(path).VersionedParams(&options, metav1.ParameterCodec))
	return decode[listOf[T, B]](body, err)
}

// firstPage reads the first page of the list at path, which is no answer of a
// kube-apiserver when it holds more than listLimit items.
func firstPage[T any](ctx context.Context, t *tenant, path string) (*listOf[T, listItems], error) {
	page, err := readList[T, listItems](ctx, t, path, metav1.ListOptions{Limit: listLimit})
	if err == nil && page.Items.over {
		err = errOverBound
	}
	return page, err
}

func (t *tenant) getLease(ctx context.Context, name string) (*leaderLease, error) {
	body, err := t.do(ctx, t.api.Get().AbsPath(coordinationPath, lodiav1alpha1.LeaderElectionNamespace, "leases", name))
	return decode[leaderLease](body, err)
}

func (t *tenant) createLease(ctx context.Context, body []byte) (*leaderLease, error) {
	answer, err := t.do(ctx, t.api.Post().AbsPath(coordinationPath, lodiav1alpha1.LeaderElectionNamespace, "leases").SetHeader("Content-Type", "application/json").Body(body))
	return decode[leaderLease](answer, err)
}

func (t *tenant) patchLease(ctx context.Context, patch []byte) (*leaderLease, error) {
	body, err := t.do(ctx, t.api.Patch(types.MergePatchType).AbsPath(coordinationPath, lodiav1alpha1.LeaderElectionNamespace, "leases", lodiav1alpha1.ControllerManagerLease).Body(patch))
	return decode[leaderLease](body, err)
}

// do sends the request, which asks for JSON, and returns the body of the
// answer. A Status of Failure is the error of a failed request, as client-go's
// Result.Error reads it, but into the fields Health reads.
func (t *tenant) do(ctx context.Context, request *rest.Request) ([]byte, error) {
	body, err := request.DoRaw(ctx)
	if apierrors.IsUnexpectedServerError(err) {
		if failure, ok := failureOf(body); ok {
			return nil, &apierrors.StatusError{ErrStatus: failure}
		}
	}
	return body, err
}

type statusBody struct {
	Status, Message string
	Reason          metav1.StatusReason
	Code            int32
	Details         struct {
		Causes array[struct{ Type metav1.CauseType }, statusCauses]
	}
}

func failureOf(body []byte) (metav1.Status, bool) {
	var s statusBody
	if json.Unmarshal(body, &s) != nil || s.Status != metav1.StatusFailure {
		return metav1.Status{}, false
	}
	failure := metav1.Status{Status: s.Status, Message: s.Message, Reason: s.Reason, Code: s.Code}
	for _, cause := range s.Details.Causes.items {
		if failure.Details == nil {
			failure.Details = &metav1.StatusDetails{}
		}
		failure.Details.Causes = append(failure.Details.Causes, metav1.StatusCause{Type: cause.Type})
	}
	return failure, true
}

func decode[T any](body []byte, err error) (*T, error) {
	if err != nil {
		return nil, err
	}
	out := new(T)
	if err := json.Unmarshal(body, out); err != nil {
		return nil, fmt.Errorf("decode the response: %w", err)
	}
	return out, nil
}
