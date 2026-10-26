// Copyright 2026 The Brygga Authors.
// SPDX-License-Identifier: Apache-2.0

package tenant

import (
	"bufio"
	"bytes"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"text/template"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"sigs.k8s.io/yaml"

	bryggav1alpha1 "github.com/segla-io/brygga/api/v1alpha1"
	"github.com/segla-io/brygga/internal/versions"
)

//go:embed manifests/*.yaml
var manifests embed.FS

var templates = template.Must(template.New("").Funcs(template.FuncMap{
	"indent": func(n int, s string) string {
		pad := strings.Repeat(" ", n)
		return pad + strings.ReplaceAll(strings.TrimSuffix(s, "\n"), "\n", "\n"+pad)
	},
	"quote": func(s string) (string, error) {
		quoted, err := json.Marshal(s)
		return string(quoted), err
	},
}).ParseFS(manifests, "manifests/*.yaml"))

type values struct {
	Name, Version, Domain    string
	Server, Endpoint         string
	ClusterInfo              string
	ServiceSubnet, PodSubnet string
	DNSIP                    string
	KubeProxyImage           string
	ProxyMode, ProxyHealthz  string
	CoreDNSImage             string
	CoreDNSReplicas          int32
	AgentImage               string
	KonnectivityHost         string
	KonnectivityPort         int32
	AgentDeployment          bool
	AgentReplicas            int32
	kubeProxy, coreDNS       bool
}

// newValues also returns the reason and the message of what a render still
// waits for, empty when nothing.
func newValues(cp *bryggav1alpha1.ControlPlane, clusterCA []byte) (*values, string, string, error) {
	endpoint := cp.Status.Endpoint
	switch {
	case cp.Status.Target == nil || cp.Status.Target.Version == "":
		return nil, bryggav1alpha1.ReasonWaitingForTarget, "status.target has no version", nil
	case endpoint == nil:
		return nil, bryggav1alpha1.ReasonWaitingForEndpoint, "status.endpoint is not published", nil
	}
	images, ok := versions.For(cp.Status.Target.Version)
	if !ok {
		return nil, "", "", fmt.Errorf("the version table has no minor of Kubernetes %s", cp.Status.Target.Version)
	}
	v := &values{
		Name:             cp.Name,
		Version:          cp.Status.Target.Version,
		Domain:           string(cp.Spec.Network.ClusterDomain),
		Endpoint:         net.JoinHostPort(endpoint.APIServer.Host, strconv.Itoa(int(endpoint.APIServer.Port))),
		KonnectivityHost: endpoint.Konnectivity.Host,
		KonnectivityPort: endpoint.Konnectivity.Port,
		KubeProxyImage:   images.KubeProxy,
		ProxyMode:        "iptables",
		ProxyHealthz:     "127.0.0.1:10256",
		CoreDNSImage:     images.CoreDNS,
		CoreDNSReplicas:  2,
		AgentImage:       images.KonnectivityAgent,
		AgentReplicas:    1,
		kubeProxy:        true,
		coreDNS:          true,
	}
	if v.Domain == "" {
		v.Domain = "cluster.local"
	}
	v.Server = "https://" + v.Endpoint
	var services, pods []string
	for _, cidr := range cp.Spec.Network.ServiceCIDRs {
		services = append(services, string(cidr))
	}
	for _, cidr := range cp.Spec.Network.PodCIDRs {
		pods = append(pods, string(cidr))
	}
	v.ServiceSubnet, v.PodSubnet = strings.Join(services, ","), strings.Join(pods, ",")
	dns, err := tenthAddress(services[0])
	if err != nil {
		return nil, "", "", err
	}
	v.DNSIP = dns.String()
	if addons := cp.Spec.Addons; addons != nil {
		if p := addons.KubeProxy; p != nil {
			v.kubeProxy = p.Enabled == nil || *p.Enabled
			if p.Mode != "" {
				v.ProxyMode = string(p.Mode)
			}
			if p.HealthzBindAddress != "" {
				v.ProxyHealthz = net.JoinHostPort(p.HealthzBindAddress, "10256")
			}
		}
		if d := addons.CoreDNS; d != nil {
			v.coreDNS = d.Enabled == nil || *d.Enabled
			if d.Replicas != nil {
				v.CoreDNSReplicas = *d.Replicas
			}
		}
		if k := addons.Konnectivity; k != nil {
			if k.Version != "" {
				v.AgentImage = v.AgentImage[:strings.LastIndexByte(v.AgentImage, ':')+1] + k.Version
			}
			if a := k.Agent; a != nil && a.Mode == "Deployment" {
				v.AgentDeployment = true
				if a.Replicas != nil {
					v.AgentReplicas = *a.Replicas
				}
			}
		}
	}
	info, err := clientcmd.Write(clientcmdapi.Config{Clusters: map[string]*clientcmdapi.Cluster{"": {Server: v.Server, CertificateAuthorityData: clusterCA}}})
	if err != nil {
		return nil, "", "", err
	}
	v.ClusterInfo = string(info)
	return v, "", "", nil
}

// tenthAddress is CoreDNS's ClusterIP, as kubeadm takes it (kubernetes@v1.35.0
// cmd/kubeadm/app/constants/constants.go, GetDNSIP).
func tenthAddress(cidr string) (netip.Addr, error) {
	prefix, err := netip.ParsePrefix(cidr)
	if err != nil {
		return netip.Addr{}, err
	}
	addr := prefix.Masked().Addr()
	for range 10 {
		addr = addr.Next()
	}
	if !prefix.Contains(addr) {
		return netip.Addr{}, fmt.Errorf("the service range %s has no tenth address", cidr)
	}
	return addr, nil
}

func render(v *values) ([]*unstructured.Unstructured, error) {
	files := []string{"bootstrap.yaml", "konnectivity.yaml"}
	if v.kubeProxy {
		files = append(files, "kube-proxy.yaml")
	}
	if v.coreDNS {
		files = append(files, "coredns.yaml")
	}
	var objects []*unstructured.Unstructured
	for _, file := range files {
		var b bytes.Buffer
		if err := templates.ExecuteTemplate(&b, file, v); err != nil {
			return nil, err
		}
		docs := utilyaml.NewYAMLReader(bufio.NewReader(&b))
		for {
			doc, err := docs.Read()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return nil, fmt.Errorf("%s: %w", file, err)
			}
			obj := &unstructured.Unstructured{}
			if err := yaml.Unmarshal(doc, &obj.Object); err != nil {
				return nil, fmt.Errorf("%s: %w", file, err)
			}
			if len(obj.Object) == 0 {
				continue
			}
			labels := obj.GetLabels()
			if labels == nil {
				labels = map[string]string{}
			}
			labels[bryggav1alpha1.ManagedByLabel] = bryggav1alpha1.ManagedBy
			obj.SetLabels(labels)
			objects = append(objects, obj)
		}
	}
	return objects, nil
}
