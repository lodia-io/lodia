// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package tenant

import (
	"bufio"
	"bytes"
	"embed"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"text/template"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"sigs.k8s.io/yaml"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/versions"
)

// apiserverProxyClientSecret is the Secret in the tenant's kube-system that
// manifests/apiserver-proxy.yaml mounts.
const apiserverProxyClientSecret = "apiserver-proxy-client"

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
	ConntrackMaxPerCore      *int32
	CoreDNSImage             string
	CoreDNSReplicas          int32
	AgentImage               string
	KonnectivityHost         string
	KonnectivityPort         int32
	InClusterHost            string
	InClusterPort            int32
	AddressType              string
	AgentDeployment          bool
	AgentReplicas            int32
	Gateway                  bool
	TunnelImage              string
	TunnelListen             string
	TunnelUpstream           string
	tunnelCert, tunnelKey    []byte
	kubeProxy, coreDNS       bool
}

// newValues also returns the reason and the message of what a render still
// waits for, empty when nothing.
func newValues(cp *lodiav1alpha1.ControlPlane, clusterCA []byte) (*values, string, string, error) {
	endpoint := cp.Status.Endpoint
	switch {
	case cp.Status.Target == nil || cp.Status.Target.Version == "":
		return nil, lodiav1alpha1.ReasonWaitingForTarget, "status.target has no version", nil
	case endpoint == nil:
		return nil, lodiav1alpha1.ReasonWaitingForEndpoint, "status.endpoint is not published", nil
	case cp.Spec.Endpoint.Gateway != nil && endpoint.Tunnel == nil:
		return nil, lodiav1alpha1.ReasonWaitingForEndpoint, "status.endpoint has no tunnel", nil
	}
	oldest, ok := versions.Oldest(runningVersions(cp)...)
	var images versions.Images
	if ok {
		images, ok = versions.For(oldest)
	}
	if !ok {
		return nil, lodiav1alpha1.ReasonWaitingForTarget, fmt.Sprintf("the oldest version the components run, %q, is not a version Lodia runs", oldest), nil
	}
	inCluster, err := netip.ParseAddr(endpoint.InCluster.Host)
	if err != nil {
		return nil, "", "", err
	}
	addressType := discoveryv1.AddressTypeIPv4
	if inCluster.Is6() {
		addressType = discoveryv1.AddressTypeIPv6
	}
	v := &values{
		Name:             cp.Name,
		Version:          oldest,
		Domain:           string(cp.Spec.Network.ClusterDomain),
		Endpoint:         net.JoinHostPort(endpoint.APIServer.Host, strconv.Itoa(int(endpoint.APIServer.Port))),
		KonnectivityHost: endpoint.Konnectivity.Host,
		KonnectivityPort: endpoint.Konnectivity.Port,
		InClusterHost:    inCluster.String(),
		InClusterPort:    endpoint.InCluster.Port,
		AddressType:      string(addressType),
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
	if cp.Spec.Endpoint.Gateway != nil {
		v.Gateway = true
		v.TunnelListen = net.JoinHostPort(v.InClusterHost, strconv.Itoa(int(endpoint.InCluster.Port)))
		v.TunnelUpstream = net.JoinHostPort(endpoint.Tunnel.Host, strconv.Itoa(int(endpoint.Tunnel.Port)))
	}
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
			if p.Conntrack != nil {
				v.ConntrackMaxPerCore = p.Conntrack.MaxPerCore
			}
		}
		if d := addons.CoreDNS; d != nil {
			v.coreDNS = d.Enabled == nil || *d.Enabled
			if d.Replicas != nil {
				v.CoreDNSReplicas = *d.Replicas
			}
		}
		if k := addons.Konnectivity; k != nil {
			pinned, err := versions.PinKonnectivity(images, k.Version)
			if err != nil {
				return nil, "", "", err
			}
			v.AgentImage = pinned.KonnectivityAgent
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

// reported are the versions the API server, the controller manager and the
// scheduler report, empty for one that reports none.
func reported(cp *lodiav1alpha1.ControlPlane) [3]string {
	var out [3]string
	if c := cp.Status.Components; c != nil {
		for i, s := range []*lodiav1alpha1.ComponentStatus{c.APIServer, c.ControllerManager, c.Scheduler} {
			if s != nil {
				out[i] = s.Version
			}
		}
	}
	return out
}

// runningVersions are the versions of the three components, an absent report
// counting as the target: no earlier full rollout exists to stay behind.
func runningVersions(cp *lodiav1alpha1.ControlPlane) []string {
	out := reported(cp)
	for i := range out {
		if out[i] == "" {
			out[i] = cp.Status.Target.Version
		}
	}
	return out[:]
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
	files := []string{"bootstrap.yaml", "endpoints.yaml", "konnectivity.yaml"}
	if v.kubeProxy {
		files = append(files, "kube-proxy.yaml")
	}
	if v.coreDNS {
		files = append(files, "coredns.yaml")
	}
	if v.Gateway {
		files = append(files, "apiserver-proxy.yaml")
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
			labels[lodiav1alpha1.ManagedByLabel] = lodiav1alpha1.ManagedBy
			obj.SetLabels(labels)
			objects = append(objects, obj)
		}
	}
	if v.Gateway {
		objects = append(objects, apiserverProxyClient(v.tunnelCert, v.tunnelKey))
	}
	return objects, nil
}

// apiserverProxyClient is the whole Secret apiserver-proxy reads its client
// certificate from: render and deliverTunnelClient share a field manager, so an
// apply without type would be refused, and one without a field would undo the
// other's write every pass.
func apiserverProxyClient(cert, key []byte) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "Secret",
		"metadata": map[string]any{
			"name":      apiserverProxyClientSecret,
			"namespace": metav1.NamespaceSystem,
			"labels":    map[string]any{lodiav1alpha1.ManagedByLabel: lodiav1alpha1.ManagedBy},
		},
		"type": string(corev1.SecretTypeTLS),
		"data": map[string]any{
			corev1.TLSCertKey:       base64.StdEncoding.EncodeToString(cert),
			corev1.TLSPrivateKeyKey: base64.StdEncoding.EncodeToString(key),
		},
	}}
}
