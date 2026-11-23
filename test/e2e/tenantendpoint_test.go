// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

//go:build e2e && (gateway || loadbalancer)

package e2e

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"

	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

func conditionAt(ctx context.Context, k8s client.Client, key client.ObjectKey, conditionType string, status metav1.ConditionStatus, reason string) (*lodiav1alpha1.ControlPlane, error) {
	cp := &lodiav1alpha1.ControlPlane{}
	if err := k8s.Get(ctx, key, cp); err != nil {
		return nil, err
	}
	c := meta.FindStatusCondition(cp.Status.Conditions, conditionType)
	if c == nil || c.Status != status || c.Reason != reason || c.ObservedGeneration != cp.Generation {
		return nil, fmt.Errorf("%s is %+v at generation %d", conditionType, c, cp.Generation)
	}
	return cp, nil
}

func sliceAddresses(slice *discoveryv1.EndpointSlice) []string {
	var out []string
	for _, e := range slice.Endpoints {
		for _, address := range e.Addresses {
			for _, port := range slice.Ports {
				out = append(out, net.JoinHostPort(address, strconv.Itoa(int(*port.Port))))
			}
		}
	}
	return out
}

func onLoopback(ctx context.Context, node string) error {
	out, err := docker(ctx, "exec", node, "ip", "-4", "-o", "addr", "show", "dev", "lo", "label", "lo:lodia")
	if err != nil {
		return err
	}
	if !strings.Contains(out, " "+lodiav1alpha1.DefaultAdvertiseAddress+"/32 ") {
		return fmt.Errorf("lo holds under lo:lodia %q", strings.TrimSpace(out))
	}
	return nil
}
