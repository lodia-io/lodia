// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

//go:build e2e && (gateway || hold || loadbalancer)

package e2e

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/distribution/reference"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
	kindcluster "sigs.k8s.io/kind/pkg/cluster"
)

const (
	gatewayZone     = "lodia.test"
	gatewayDNSImage = "registry.k8s.io/coredns/coredns:v1.13.1@sha256:9b9128672209474da07c91439bf15ed704ae05ad918dd6454e5b6ae14e35fee6"
	apiserverProxy  = "apiserver-proxy"
)

func tunnelImage(ctx context.Context, t *testing.T, access *tenantAccess) reference.NamedTagged {
	t.Helper()
	ds := &appsv1.DaemonSet{}
	if err := access.client.Get(ctx, client.ObjectKey{Namespace: metav1.NamespaceSystem, Name: apiserverProxy}, ds); err != nil {
		t.Fatal(err)
	}
	named, err := reference.ParseNamed(ds.Spec.Template.Spec.Containers[0].Image)
	if err != nil {
		t.Fatal(err)
	}
	tagged, ok := named.(reference.NamedTagged)
	if !ok {
		t.Fatalf("apiserver-proxy's image %s has no tag", named)
	}
	return tagged
}

func pull(ctx context.Context, cluster string, images ...string) error {
	nodes, err := kindcluster.NewProvider().ListNodes(cluster)
	if err != nil {
		return err
	}
	for _, node := range nodes {
		for _, image := range images {
			var last error
			for attempt := range 5 {
				if _, last = docker(ctx, "exec", node.String(), "crictl", "pull", image); last == nil {
					break
				}
				time.Sleep(time.Duration(attempt+1) * 5 * time.Second)
			}
			if last != nil {
				return fmt.Errorf("pull %s into %s: %w", image, node, last)
			}
		}
	}
	return nil
}

func dnsContainer(cluster string) string {
	return cluster + "-dns"
}

func runDNS(ctx context.Context, cluster string, files map[string]string, probe string, want ...string) (string, error) {
	name := dnsContainer(cluster)
	dir, err := os.MkdirTemp("", name)
	if err != nil {
		return "", err
	}
	defer func() { _ = os.RemoveAll(dir) }()
	steps := [][]string{
		{"rm", "--force", "--volumes", name},
		dnsCreateArgs(cluster, name, gatewayDNSImage),
	}
	for path, content := range files {
		local := filepath.Join(dir, filepath.Base(path))
		if err := os.WriteFile(local, []byte(content), 0o644); err != nil {
			return "", err
		}
		steps = append(steps, []string{"cp", local, name + ":" + path})
	}
	for _, args := range append(steps, []string{"start", name}) {
		if _, err := docker(ctx, args...); err != nil {
			return "", err
		}
	}
	out, err := docker(ctx, "inspect", "--format", `{{(index .NetworkSettings.Networks "kind").IPAddress}}`, name)
	if err != nil {
		return "", err
	}
	address := strings.TrimSpace(out)
	return address, eventually(ctx, time.Minute, "the DNS container "+name+" at "+address+" answers", func(ctx context.Context) error {
		zone, err := resolverAt(address).LookupHost(ctx, probe)
		if err != nil {
			return err
		}
		if !slices.Equal(zone, want) {
			return fmt.Errorf("%s is %v", probe, zone)
		}
		_, err = resolverAt(address).LookupHost(ctx, "registry.k8s.io")
		return err
	})
}

func resolverAt(address string) *net.Resolver {
	return &net.Resolver{PreferGo: true, Dial: func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, net.JoinHostPort(address, "53"))
	}}
}

func forwardZone(ctx context.Context, c client.Client, dns string) error {
	coreDNS := &corev1.ConfigMap{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: metav1.NamespaceSystem, Name: "coredns"}, coreDNS); err != nil {
		return err
	}
	kind, _, _ := strings.Cut(coreDNS.Data["Corefile"], gatewayZone+":53 {")
	coreDNS.Data["Corefile"] = strings.TrimRight(kind, "\n") + fmt.Sprintf("\n%s:53 {\n    forward . %s\n}\n", gatewayZone, dns)
	if err := c.Update(ctx, coreDNS); err != nil {
		return err
	}
	deployment := &appsv1.Deployment{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(coreDNS), deployment); err != nil {
		return err
	}
	patch := client.MergeFrom(deployment.DeepCopy())
	if deployment.Spec.Template.Annotations == nil {
		deployment.Spec.Template.Annotations = map[string]string{}
	}
	deployment.Spec.Template.Annotations["lodia.io/e2e-zone"] = gatewayZone + " " + dns
	if err := c.Patch(ctx, deployment, patch); err != nil {
		return err
	}
	return eventually(ctx, 2*time.Minute, "the management cluster's CoreDNS forwards "+gatewayZone+" to "+dns, func(ctx context.Context) error {
		return coreDNSAvailable(ctx, c)
	})
}

func eventually(ctx context.Context, timeout time.Duration, what string, check func(context.Context) error) error {
	var last error
	err := wait.PollUntilContextTimeout(ctx, time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		err := check(ctx)
		if ctx.Err() == nil {
			last = err
		}
		return err == nil, nil
	})
	if err != nil {
		return fmt.Errorf("%s: %w", what, errors.Join(err, last))
	}
	return nil
}
