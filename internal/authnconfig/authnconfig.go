// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

// Package authnconfig holds Lodia's copy of kube-apiserver's JWT
// authenticators to upstream's type.
package authnconfig

import (
	"encoding/json"
	"errors"

	apiserverv1 "k8s.io/apiserver/pkg/apis/apiserver/v1"
	sigsjson "sigs.k8s.io/json"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

// Upstream converts Lodia's JWT authenticators into kube-apiserver's
// apiserver.config.k8s.io/v1 type through their JSON, decoded as strictly as
// kube-apiserver decodes its file: a field that type lacks fails.
func Upstream(jwt []lodiav1alpha1.JWTAuthenticator) ([]apiserverv1.JWTAuthenticator, error) {
	data, err := json.Marshal(jwt)
	if err != nil {
		return nil, err
	}
	var out []apiserverv1.JWTAuthenticator
	strict, err := sigsjson.UnmarshalStrict(data, &out)
	if err != nil {
		return nil, err
	}
	if err := errors.Join(strict...); err != nil {
		return nil, err
	}
	return out, nil
}
