// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

//go:build e2e && loadbalancer

package e2e

import (
	"context"
	"encoding/binary"
	"fmt"
	"maps"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/third_party/helm"
)

const (
	metallbNamespace     = "metallb-system"
	metallbChart         = "oci://quay.io/metallb/chart/metallb@sha256:3980cbdddacec6e55c47b787461e057280e69808ad6f5190d2603d8c39733860"
	metallbControllerTag = "v0.16.2@sha256:34a6f107f29c4fc6e9d084a7f3bdb781aae7b5ee2b534195a2775ab68c09c503"
	metallbSpeakerTag    = "v0.16.2@sha256:01666518b091a6651d68891c802db84b2219495d7e0e41c2e61c46f9976be231"
	metallbAnnotation    = "metallb.io/loadBalancerIPs"
	metallbPool          = "e2e"
	unservedClass        = "e2e.lodia.io/none"
	konnectivityPort     = 8132
	zoneFile             = gatewayZone + ".zone"
)

var (
	poolFirst, poolLast netip.Addr
	records             = map[string]string{}
	zoneSerial          = uint32(1)
)

func init() {
	beforeLodia = append(beforeLodia, installLoadBalancer)
}

func installLoadBalancer(ctx context.Context, cfg *envconf.Config) (context.Context, error) {
	start := time.Now()
	cluster, err := kindCluster(cfg.KubeconfigFile())
	if err != nil {
		return ctx, err
	}
	if poolFirst, poolLast, err = poolRange(ctx); err != nil {
		return ctx, err
	}
	controller, speaker := "quay.io/metallb/controller:"+metallbControllerTag, "quay.io/metallb/speaker:"+metallbSpeakerTag
	if err := pull(ctx, cluster, controller, speaker); err != nil {
		return ctx, err
	}
	err = helm.New(cfg.KubeconfigFile()).RunUpgrade(
		helm.WithName("metallb"), helm.WithChart(metallbChart), helm.WithNamespace(metallbNamespace), helm.WithContext(ctx),
		helm.WithArgs("--install", "--create-namespace", "--set", "frrk8s.enabled=false",
			"--set", "controller.image.tag="+metallbControllerTag, "--set", "speaker.image.tag="+metallbSpeakerTag),
		helm.WithWait(), helm.WithTimeout("5m"))
	if err != nil {
		return ctx, fmt.Errorf("install MetalLB: %w", err)
	}
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return ctx, err
	}
	c, err := client.New(cfg.Client().RESTConfig(), client.Options{Scheme: scheme})
	if err != nil {
		return ctx, err
	}
	corefile := fmt.Sprintf("%s:53 {\n    file /%s {\n        reload 1s\n    }\n}\n.:53 {\n    forward . /etc/resolv.conf\n}\n", gatewayZone, zoneFile)
	if nodeDNS, err = runDNS(ctx, cluster, map[string]string{"/Corefile": corefile, "/" + zoneFile: zoneContent()}, "probe."+gatewayZone, "127.0.0.1"); err != nil {
		return ctx, err
	}
	if err := forwardZone(ctx, c, nodeDNS); err != nil {
		return ctx, err
	}
	fmt.Printf("MetalLB %s runs with no pool yet, which will be %s-%s of kind's network, and %s serves %s from its own zone file, after %s\n",
		metallbControllerTag, poolFirst, poolLast, nodeDNS, gatewayZone, time.Since(start).Round(time.Second))
	return ctx, nil
}

func poolRange(ctx context.Context) (netip.Addr, netip.Addr, error) {
	out, err := docker(ctx, "network", "inspect", "kind", "--format", "{{range .IPAM.Config}}{{.Subnet}} {{end}}")
	if err != nil {
		return netip.Addr{}, netip.Addr{}, err
	}
	for subnet := range strings.FieldsSeq(out) {
		prefix, err := netip.ParsePrefix(subnet)
		if err != nil || !prefix.Addr().Is4() || prefix.Bits() > 24 {
			continue
		}
		network := prefix.Masked().Addr().As4()
		last := binary.BigEndian.Uint32(network[:]) | ^uint32(0)>>prefix.Bits()
		return addrFrom(last - 55), addrFrom(last - 5), nil
	}
	return netip.Addr{}, netip.Addr{}, fmt.Errorf("docker network kind has no IPv4 subnet of at least 256 addresses: %q", out)
}

func addrFrom(v uint32) netip.Addr {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], v)
	return netip.AddrFrom4(b)
}

func poolAddress(n int) string {
	v := binary.BigEndian.Uint32(poolFirst.AsSlice()) + uint32(n)
	return addrFrom(v).String()
}

func poolObjects() []*unstructured.Unstructured {
	meta := func(kind string) map[string]any {
		return map[string]any{"apiVersion": "metallb.io/v1beta1", "kind": kind, "metadata": map[string]any{"name": metallbPool, "namespace": metallbNamespace}}
	}
	pool, advertisement := meta("IPAddressPool"), meta("L2Advertisement")
	pool["spec"] = map[string]any{"addresses": []any{poolFirst.String() + "-" + poolLast.String()}}
	advertisement["spec"] = map[string]any{"ipAddressPools": []any{metallbPool}}
	return []*unstructured.Unstructured{{Object: pool}, {Object: advertisement}}
}

func createPool(ctx context.Context, t *testing.T, k8s client.Client) {
	t.Helper()
	until(ctx, t, time.Minute, "MetalLB takes the pool "+poolFirst.String()+"-"+poolLast.String(), func(ctx context.Context) error {
		for _, obj := range poolObjects() {
			if err := k8s.Create(ctx, obj); err != nil && !apierrors.IsAlreadyExists(err) {
				return err
			}
		}
		return nil
	})
}

func deletePool(ctx context.Context, t *testing.T, k8s client.Client) {
	t.Helper()
	for _, obj := range poolObjects() {
		if err := client.IgnoreNotFound(k8s.Delete(ctx, obj)); err != nil {
			t.Fatal(err)
		}
	}
}

func zoneContent() string {
	var zone strings.Builder
	fmt.Fprintf(&zone, "$ORIGIN %s.\n@ 5 IN SOA ns.%s. hostmaster.%s. %d 5 5 5 5\n@ 5 IN NS ns.%s.\nns 5 IN A 127.0.0.1\nprobe 5 IN A 127.0.0.1\n",
		gatewayZone, gatewayZone, gatewayZone, zoneSerial, gatewayZone)
	for _, name := range slices.Sorted(maps.Keys(records)) {
		fmt.Fprintf(&zone, "%s. 5 IN A %s\n", name, records[name])
	}
	return zone.String()
}

func publish(ctx context.Context, t *testing.T, cfg *envconf.Config, name, address string) {
	t.Helper()
	cluster, err := kindCluster(cfg.KubeconfigFile())
	if err != nil {
		t.Fatal(err)
	}
	records[name] = address
	zoneSerial++
	local := filepath.Join(t.TempDir(), zoneFile)
	if err := os.WriteFile(local, []byte(zoneContent()), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := docker(ctx, "cp", local, dnsContainer(cluster)+":/"+zoneFile); err != nil {
		t.Fatal(err)
	}
	until(ctx, t, time.Minute, fmt.Sprintf("the DNS container answers %s with %s", name, address), func(ctx context.Context) error {
		got, err := resolverAt(nodeDNS).LookupHost(ctx, name)
		if err != nil {
			return err
		}
		if !slices.Equal(got, []string{address}) {
			return fmt.Errorf("%s is %v", name, got)
		}
		return nil
	})
}
