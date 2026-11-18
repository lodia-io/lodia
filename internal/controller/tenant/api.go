// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package tenant

import (
	"context"
	"encoding/json"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"

	"go.lodia.io/lodia/internal/tenantclient"
)

type tenantAPI struct {
	client  *tenantclient.Client
	timeout time.Duration
}

func (a tenantAPI) do(ctx context.Context, request *rest.Request) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, a.timeout)
	defer cancel()
	return a.client.Do(ctx, request)
}

func (a tenantAPI) read(ctx context.Context, e entry) ([]byte, error) {
	request, ok := addressed(a.client.Get(), e)
	if !ok {
		return nil, nil
	}
	body, err := a.do(ctx, request)
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	return body, err
}

func (a tenantAPI) readObject(ctx context.Context, e entry) (*object, error) {
	body, err := a.read(ctx, e)
	if err != nil || body == nil {
		return nil, err
	}
	return tenantclient.Decode(body, nil, objectOf)
}

func (a tenantAPI) apply(ctx context.Context, e entry, body []byte) error {
	request, ok := addressed(a.client.Patch(types.ApplyPatchType), e)
	if !ok {
		return &failure{entry: e, err: errNotInTable}
	}
	_, err := a.do(ctx, request.
		VersionedParams(&metav1.PatchOptions{FieldManager: FieldManager, Force: new(true)}, metav1.ParameterCodec).
		Body(body))
	return err
}

func (a tenantAPI) delete(ctx context.Context, e entry, o *object) error {
	request, ok := addressed(a.client.Delete(), e)
	if !ok {
		return nil
	}
	policy := metav1.DeletePropagationBackground
	body, err := json.Marshal(metav1.DeleteOptions{
		TypeMeta:          metav1.TypeMeta{Kind: "DeleteOptions", APIVersion: "v1"},
		Preconditions:     &metav1.Preconditions{UID: new(types.UID(o.uid)), ResourceVersion: &o.resourceVersion},
		PropagationPolicy: &policy,
	})
	if err != nil {
		return err
	}
	_, err = a.do(ctx, request.SetHeader("Content-Type", "application/json").Body(body))
	return err
}
