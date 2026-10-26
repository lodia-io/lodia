// Copyright 2026 The Brygga Authors.
// SPDX-License-Identifier: Apache-2.0

package workloads

import (
	"maps"
	"net"
	"slices"
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	apiserverv1 "k8s.io/apiserver/pkg/apis/apiserver/v1"
	apiserverv1beta1 "k8s.io/apiserver/pkg/apis/apiserver/v1beta1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/yaml"

	bryggav1alpha1 "github.com/segla-io/brygga/api/v1alpha1"
	"github.com/segla-io/brygga/internal/certs"
	"github.com/segla-io/brygga/internal/versions"
)

const (
	apiServer         = "kube-apiserver"
	controllerManager = "kube-controller-manager"
	scheduler         = "kube-scheduler"

	kubernetesPort   = 6443
	konnectivityPort = 8132

	restartHashAnnotation = "brygga.segla.io/restart-hash"
	signerSocket          = "/run/signer/signer.sock"
	konnectivitySocket    = "/run/konnectivity/konnectivity-server.socket"
	egressFile            = "egress-selector.yaml"
	authenticationFile    = "authentication-config.yaml"
)

var components = []string{apiServer, controllerManager, scheduler}

// restartOnly are the PKI Secrets each component reads once at start; it
// reloads the rest (docs/README.md, A tenant namespace).
var restartOnly = map[string][]bryggav1alpha1.Secret{
	apiServer: {
		bryggav1alpha1.EtcdCASecret, bryggav1alpha1.FrontProxyClientSecret,
		bryggav1alpha1.KonnectivityServerSecret, bryggav1alpha1.KonnectivityKubeconfigSecret,
	},
	controllerManager: {bryggav1alpha1.CABundleSecret},
}

// anonymousPaths are what kubeadm join reads before it has credentials, and
// the health paths.
var anonymousPaths = []string{"/healthz", "/livez", bryggav1alpha1.APIServerReadyzPath, "/api/v1/namespaces/kube-public/configmaps/cluster-info"}

func deploymentName(cp, component string) string { return cp + "-" + component }

func podLabels(cp, component string) map[string]string {
	return map[string]string{bryggav1alpha1.ControlPlaneLabel: cp, "app.kubernetes.io/name": component}
}

func objectMeta(cp *bryggav1alpha1.ControlPlane, name string) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Name:            name,
		Namespace:       cp.Namespace,
		Labels:          map[string]string{bryggav1alpha1.ControlPlaneLabel: cp.Name},
		OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(cp, bryggav1alpha1.GroupVersion.WithKind("ControlPlane"))},
	}
}

func service(cp *bryggav1alpha1.ControlPlane) *corev1.Service {
	svc := &corev1.Service{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Service"},
		ObjectMeta: objectMeta(cp, bryggav1alpha1.APIServerServiceName(cp.Name)),
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeNodePort,
			Selector: podLabels(cp.Name, apiServer),
			Ports: []corev1.ServicePort{
				{Name: "https", Protocol: corev1.ProtocolTCP, Port: bryggav1alpha1.APIServerServicePort, TargetPort: intstr.FromString("https")},
				{Name: "kubernetes", Protocol: corev1.ProtocolTCP, Port: kubernetesPort, TargetPort: intstr.FromString("https")},
				{Name: "konnectivity", Protocol: corev1.ProtocolTCP, Port: konnectivityPort, TargetPort: intstr.FromString("konnectivity")},
			},
		},
	}
	if lb := cp.Spec.Endpoint.LoadBalancer; lb != nil {
		svc.Spec.Type = corev1.ServiceTypeLoadBalancer
		svc.Annotations = lb.Annotations
		if lb.Class != "" {
			svc.Spec.LoadBalancerClass = new(lb.Class)
		}
		for _, cidr := range lb.SourceRanges {
			svc.Spec.LoadBalancerSourceRanges = append(svc.Spec.LoadBalancerSourceRanges, string(cidr))
		}
	}
	return svc
}

func egressSelector() ([]byte, error) {
	return yaml.Marshal(apiserverv1beta1.EgressSelectorConfiguration{
		TypeMeta: metav1.TypeMeta{APIVersion: apiserverv1beta1.SchemeGroupVersion.String(), Kind: "EgressSelectorConfiguration"},
		EgressSelections: []apiserverv1beta1.EgressSelection{{
			Name: "cluster",
			Connection: apiserverv1beta1.Connection{
				ProxyProtocol: apiserverv1beta1.ProtocolGRPC,
				Transport:     &apiserverv1beta1.Transport{UDS: &apiserverv1beta1.UDSTransport{UDSName: konnectivitySocket}},
			},
		}},
	})
}

func authentication() ([]byte, error) {
	anonymous := &apiserverv1.AnonymousAuthConfig{Enabled: true}
	for _, path := range anonymousPaths {
		anonymous.Conditions = append(anonymous.Conditions, apiserverv1.AnonymousAuthCondition{Path: path})
	}
	return yaml.Marshal(apiserverv1.AuthenticationConfiguration{
		TypeMeta:  metav1.TypeMeta{APIVersion: apiserverv1.SchemeGroupVersion.String(), Kind: "AuthenticationConfiguration"},
		JWT:       []apiserverv1.JWTAuthenticator{},
		Anonymous: anonymous,
	})
}

func configMap(cp *bryggav1alpha1.ControlPlane, suffix, key string, content []byte) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
		ObjectMeta: objectMeta(cp, cp.Name+"-"+suffix),
		Data:       map[string]string{key: string(content)},
	}
}

type rendering struct {
	cp          *bryggav1alpha1.ControlPlane
	images      versions.Images
	signerImage string
	advertise   string
	securePort  int32
	issuer      string
	etcd        []bryggav1alpha1.HostPort
	hashes      map[string]string
}

func (r rendering) deployments() []*appsv1.Deployment {
	return []*appsv1.Deployment{r.apiServer(), r.controllerManager(), r.scheduler()}
}

func (r rendering) apiServer() *appsv1.Deployment {
	cp := r.cp
	settings := ptr.Deref(cp.Spec.APIServer, bryggav1alpha1.APIServer{})
	etcd := make([]string, 0, len(r.etcd))
	for _, e := range r.etcd {
		etcd = append(etcd, "https://"+net.JoinHostPort(e.Host, strconv.Itoa(int(e.Port))))
	}
	flags := map[string]string{
		"advertise-address":                           r.advertise,
		"allow-privileged":                            "true",
		"authentication-config":                       "/etc/brygga/authentication/" + authenticationFile,
		"authorization-mode":                          "Node,RBAC",
		"client-ca-file":                              "/etc/brygga/client-ca/" + bryggav1alpha1.CACertKey,
		"egress-selector-config-file":                 "/etc/brygga/egress/" + egressFile,
		"enable-admission-plugins":                    "NodeRestriction",
		"enable-bootstrap-token-auth":                 "true",
		"encryption-provider-config":                  "/etc/brygga/encryption/" + bryggav1alpha1.EncryptionConfigKey,
		"encryption-provider-config-automatic-reload": "true",
		"etcd-cafile":                                 "/etc/brygga/etcd-ca/" + bryggav1alpha1.CACertKey,
		"etcd-certfile":                               "/etc/brygga/etcd-client/" + bryggav1alpha1.TLSCertKey,
		"etcd-keyfile":                                "/etc/brygga/etcd-client/" + bryggav1alpha1.TLSPrivateKeyKey,
		"etcd-servers":                                strings.Join(etcd, ","),
		"kubelet-client-certificate":                  "/etc/brygga/kubelet-client/" + bryggav1alpha1.TLSCertKey,
		"kubelet-client-key":                          "/etc/brygga/kubelet-client/" + bryggav1alpha1.TLSPrivateKeyKey,
		"kubelet-preferred-address-types":             "InternalIP,ExternalIP,Hostname",
		"proxy-client-cert-file":                      "/etc/brygga/front-proxy-client/" + bryggav1alpha1.TLSCertKey,
		"proxy-client-key-file":                       "/etc/brygga/front-proxy-client/" + bryggav1alpha1.TLSPrivateKeyKey,
		"requestheader-allowed-names":                 bryggav1alpha1.FrontProxyClientCommonName,
		"requestheader-client-ca-file":                "/etc/brygga/front-proxy-ca/" + bryggav1alpha1.CACertKey,
		"requestheader-extra-headers-prefix":          "X-Remote-Extra-",
		"requestheader-group-headers":                 "X-Remote-Group",
		"requestheader-username-headers":              "X-Remote-User",
		"secure-port":                                 strconv.Itoa(int(r.securePort)),
		"service-account-issuer":                      r.issuer,
		"service-account-signing-endpoint":            signerSocket,
		"service-cluster-ip-range":                    joined(cp.Spec.Network.ServiceCIDRs),
		"tls-cert-file":                               "/etc/brygga/serving/" + bryggav1alpha1.TLSCertKey,
		"tls-private-key-file":                        "/etc/brygga/serving/" + bryggav1alpha1.TLSPrivateKeyKey,
	}
	container := corev1.Container{
		Name:    apiServer,
		Image:   r.images.APIServer,
		Command: command(apiServer, flags, settings.ExtraArgs, settings.FeatureGates),
		Ports:   []corev1.ContainerPort{{Name: "https", ContainerPort: r.securePort, Protocol: corev1.ProtocolTCP}},
		ReadinessProbe: &corev1.Probe{
			ProbeHandler:  httpsGet("/readyz", intstr.FromString("https")),
			PeriodSeconds: 5,
		},
		LivenessProbe: &corev1.Probe{
			ProbeHandler:        httpsGet("/livez", intstr.FromString("https")),
			InitialDelaySeconds: 15,
			PeriodSeconds:       10,
			FailureThreshold:    8,
		},
		Resources: resources(settings.Resources, "100m", "256Mi"),
		VolumeMounts: mounts(
			"serving", "client-ca", "kubelet-client", "front-proxy-ca", "front-proxy-client",
			"etcd-ca", "etcd-client", "encryption", "authentication", "egress",
		),
	}
	container.VolumeMounts = append(container.VolumeMounts,
		corev1.VolumeMount{Name: "konnectivity-socket", MountPath: "/run/konnectivity"},
		corev1.VolumeMount{Name: "signer-socket", MountPath: "/run/signer"},
	)
	konnectivity := corev1.Container{
		Name:  "konnectivity-server",
		Image: r.konnectivityImage(),
		Command: command("/proxy-server", map[string]string{
			"uds-name":                 konnectivitySocket,
			"delete-existing-uds-file": "true",
			"server-port":              "0",
			"mode":                     "grpc",
			"agent-port":               strconv.Itoa(konnectivityPort),
			"admin-port":               "8133",
			"health-port":              "8134",
			"cluster-cert":             "/etc/brygga/konnectivity-server/" + bryggav1alpha1.TLSCertKey,
			"cluster-key":              "/etc/brygga/konnectivity-server/" + bryggav1alpha1.TLSPrivateKeyKey,
			"kubeconfig":               "/etc/brygga/konnectivity-kubeconfig/" + bryggav1alpha1.KubeconfigKey,
			"agent-namespace":          "kube-system",
			"agent-service-account":    "konnectivity-agent",
			"authentication-audience":  "system:konnectivity-server",
			"proxy-strategies":         "destHost,default",
			"enable-lease-controller":  "true",
		}, nil, nil),
		Ports: []corev1.ContainerPort{{Name: "konnectivity", ContainerPort: konnectivityPort, Protocol: corev1.ProtocolTCP}},
		LivenessProbe: &corev1.Probe{
			ProbeHandler:        corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/healthz", Port: intstr.FromInt32(8134)}},
			InitialDelaySeconds: 10,
			PeriodSeconds:       10,
		},
		Resources:    resources(nil, "20m", "32Mi"),
		VolumeMounts: append(mounts("konnectivity-server", "konnectivity-kubeconfig"), corev1.VolumeMount{Name: "konnectivity-socket", MountPath: "/run/konnectivity"}),
	}
	signer := corev1.Container{
		Name:          "brygga-signer",
		Image:         r.signerImage,
		RestartPolicy: new(corev1.ContainerRestartPolicyAlways),
		Args:          []string{"--socket=" + signerSocket, "--keys-dir=/keys"},
		StartupProbe: &corev1.Probe{
			ProbeHandler:     corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/readyz", Port: intstr.FromInt32(8081)}},
			PeriodSeconds:    1,
			FailureThreshold: 30,
		},
		LivenessProbe: &corev1.Probe{
			ProbeHandler:  corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/healthz", Port: intstr.FromInt32(8081)}},
			PeriodSeconds: 10,
		},
		Resources: resources(nil, "10m", "16Mi"),
		VolumeMounts: []corev1.VolumeMount{
			{Name: "signer-socket", MountPath: "/run/signer"},
			{Name: "sa-keys", MountPath: "/keys", ReadOnly: true},
		},
	}
	volumes := append(secretVolumes(cp.Name, map[string]bryggav1alpha1.Secret{
		"serving":                 bryggav1alpha1.APIServerServingSecret,
		"client-ca":               bryggav1alpha1.ClientCASecret,
		"kubelet-client":          bryggav1alpha1.APIServerKubeletClientSecret,
		"front-proxy-ca":          bryggav1alpha1.FrontProxyCASecret,
		"front-proxy-client":      bryggav1alpha1.FrontProxyClientSecret,
		"etcd-ca":                 bryggav1alpha1.EtcdCASecret,
		"etcd-client":             bryggav1alpha1.APIServerEtcdClientSecret,
		"encryption":              bryggav1alpha1.EncryptionConfigSecret,
		"konnectivity-server":     bryggav1alpha1.KonnectivityServerSecret,
		"konnectivity-kubeconfig": bryggav1alpha1.KonnectivityKubeconfigSecret,
		"sa-keys":                 bryggav1alpha1.ServiceAccountKeysSecret,
	}),
		configMapVolume("authentication", cp.Name+"-authentication-config"),
		configMapVolume("egress", cp.Name+"-egress-selector"),
		corev1.Volume{Name: "konnectivity-socket", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
		corev1.Volume{Name: "signer-socket", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
	)
	return r.deployment(apiServer, ptr.Deref(settings.Replicas, 1), corev1.PodSpec{
		InitContainers: []corev1.Container{signer},
		Containers:     []corev1.Container{container, konnectivity},
		Volumes:        volumes,
	})
}

func (r rendering) konnectivityImage() string {
	if k := r.cp.Spec.Addons; k != nil && k.Konnectivity != nil && k.Konnectivity.Version != "" {
		image, _, _ := strings.Cut(r.images.KonnectivityServer, ":")
		return image + ":" + k.Konnectivity.Version
	}
	return r.images.KonnectivityServer
}

func (r rendering) controllerManager() *appsv1.Deployment {
	cp := r.cp
	settings := ptr.Deref(cp.Spec.ControllerManager, bryggav1alpha1.Component{})
	kubeconfig := "/etc/brygga/kubeconfig/" + bryggav1alpha1.KubeconfigKey
	flags := map[string]string{
		"allocate-node-cidrs":                      "true",
		"authentication-kubeconfig":                kubeconfig,
		"authorization-kubeconfig":                 kubeconfig,
		"bind-address":                             "0.0.0.0",
		"client-ca-file":                           "/etc/brygga/client-ca/" + bryggav1alpha1.CACertKey,
		"cluster-cidr":                             joined(cp.Spec.Network.PodCIDRs),
		"cluster-name":                             cp.Name,
		"cluster-signing-duration":                 certs.KubeletClientLifetime.String(),
		"cluster-signing-kubelet-client-cert-file": "/etc/brygga/kubelet-signer/" + bryggav1alpha1.TLSCertKey,
		"cluster-signing-kubelet-client-key-file":  "/etc/brygga/kubelet-signer/" + bryggav1alpha1.TLSPrivateKeyKey,
		"controllers":                              "*,bootstrap-signer-controller,token-cleaner-controller",
		"kubeconfig":                               kubeconfig,
		"leader-elect":                             "true",
		"requestheader-client-ca-file":             "/etc/brygga/front-proxy-ca/" + bryggav1alpha1.CACertKey,
		"root-ca-file":                             "/etc/brygga/ca-bundle/" + bryggav1alpha1.CACertKey,
		"service-cluster-ip-range":                 joined(cp.Spec.Network.ServiceCIDRs),
		"use-service-account-credentials":          "true",
	}
	container := corev1.Container{
		Name:           controllerManager,
		Image:          r.images.ControllerManager,
		Command:        command(controllerManager, flags, settings.ExtraArgs, settings.FeatureGates),
		ReadinessProbe: &corev1.Probe{ProbeHandler: httpsGet("/healthz", intstr.FromInt32(10257)), PeriodSeconds: 5},
		LivenessProbe:  &corev1.Probe{ProbeHandler: httpsGet("/healthz", intstr.FromInt32(10257)), InitialDelaySeconds: 15, PeriodSeconds: 10},
		Resources:      resources(settings.Resources, "50m", "64Mi"),
		VolumeMounts:   mounts("kubeconfig", "client-ca", "front-proxy-ca", "kubelet-signer", "ca-bundle"),
	}
	return r.deployment(controllerManager, 1, corev1.PodSpec{
		Containers: []corev1.Container{container},
		Volumes: secretVolumes(cp.Name, map[string]bryggav1alpha1.Secret{
			"kubeconfig":     bryggav1alpha1.ControllerManagerKubeconfigSecret,
			"client-ca":      bryggav1alpha1.ClientCASecret,
			"front-proxy-ca": bryggav1alpha1.FrontProxyCASecret,
			"kubelet-signer": bryggav1alpha1.KubeletSignerSecret,
			"ca-bundle":      bryggav1alpha1.CABundleSecret,
		}),
	})
}

func (r rendering) scheduler() *appsv1.Deployment {
	cp := r.cp
	settings := ptr.Deref(cp.Spec.Scheduler, bryggav1alpha1.Scheduler{})
	kubeconfig := "/etc/brygga/kubeconfig/" + bryggav1alpha1.KubeconfigKey
	flags := map[string]string{
		"authentication-kubeconfig": kubeconfig,
		"authorization-kubeconfig":  kubeconfig,
		"bind-address":              "0.0.0.0",
		"kubeconfig":                kubeconfig,
		"leader-elect":              "true",
	}
	container := corev1.Container{
		Name:           scheduler,
		Image:          r.images.Scheduler,
		Command:        command(scheduler, flags, settings.ExtraArgs, settings.FeatureGates),
		ReadinessProbe: &corev1.Probe{ProbeHandler: httpsGet("/readyz", intstr.FromInt32(10259)), PeriodSeconds: 5},
		LivenessProbe:  &corev1.Probe{ProbeHandler: httpsGet("/livez", intstr.FromInt32(10259)), InitialDelaySeconds: 15, PeriodSeconds: 10},
		Resources:      resources(settings.Resources, "20m", "32Mi"),
		VolumeMounts:   mounts("kubeconfig"),
	}
	return r.deployment(scheduler, 1, corev1.PodSpec{
		Containers: []corev1.Container{container},
		Volumes:    secretVolumes(cp.Name, map[string]bryggav1alpha1.Secret{"kubeconfig": bryggav1alpha1.SchedulerKubeconfigSecret}),
	})
}

// deployment mounts no ServiceAccount token: the components talk to the
// tenant's API server, never to the management cluster.
func (r rendering) deployment(component string, replicas int32, pod corev1.PodSpec) *appsv1.Deployment {
	cp := r.cp
	labels := podLabels(cp.Name, component)
	pod.AutomountServiceAccountToken = new(false)
	pod.EnableServiceLinks = new(false)
	if p := cp.Spec.Placement; p != nil {
		pod.NodeSelector, pod.Tolerations, pod.PriorityClassName = p.NodeSelector, p.Tolerations, p.PriorityClassName
	}
	template := corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels}, Spec: pod}
	if hash, ok := r.hashes[component]; ok {
		template.Annotations = map[string]string{restartHashAnnotation: hash}
	}
	d := &appsv1.Deployment{
		TypeMeta:   metav1.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"},
		ObjectMeta: objectMeta(cp, deploymentName(cp.Name, component)),
		Spec: appsv1.DeploymentSpec{
			Replicas: new(replicas),
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: template,
		},
	}
	maps.Copy(d.Labels, map[string]string{"app.kubernetes.io/name": component})
	return d
}

// networkPolicies isolate the ingress of each component's pods: the API
// server's two ports are the public endpoint, and the controllers take
// nothing; the kubelet's probes come from the node, which a NetworkPolicy
// does not block
// (https://kubernetes.io/docs/concepts/services-networking/network-policies/).
func networkPolicies(cp *bryggav1alpha1.ControlPlane) []*networkingv1.NetworkPolicy {
	var out []*networkingv1.NetworkPolicy
	for _, component := range components {
		p := &networkingv1.NetworkPolicy{
			TypeMeta:   metav1.TypeMeta{APIVersion: "networking.k8s.io/v1", Kind: "NetworkPolicy"},
			ObjectMeta: objectMeta(cp, deploymentName(cp.Name, component)),
			Spec: networkingv1.NetworkPolicySpec{
				PodSelector: metav1.LabelSelector{MatchLabels: podLabels(cp.Name, component)},
				PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress},
			},
		}
		if component == apiServer {
			var ports []networkingv1.NetworkPolicyPort
			for _, name := range []string{"https", "konnectivity"} {
				ports = append(ports, networkingv1.NetworkPolicyPort{Protocol: new(corev1.ProtocolTCP), Port: new(intstr.FromString(name))})
			}
			p.Spec.Ingress = []networkingv1.NetworkPolicyIngressRule{{Ports: ports}}
		}
		out = append(out, p)
	}
	return out
}

// command puts the flags Brygga sets and the tenant's extra ones in a fixed
// order, so an unchanged ControlPlane renders the same pod template.
func command(binary string, flags, extra map[string]string, gates map[string]bool) []string {
	all := maps.Clone(flags)
	maps.Copy(all, extra)
	if len(gates) > 0 {
		var pairs []string
		for _, gate := range slices.Sorted(maps.Keys(gates)) {
			pairs = append(pairs, gate+"="+strconv.FormatBool(gates[gate]))
		}
		all["feature-gates"] = strings.Join(pairs, ",")
	}
	out := []string{binary}
	for _, name := range slices.Sorted(maps.Keys(all)) {
		out = append(out, "--"+name+"="+all[name])
	}
	return out
}

func httpsGet(path string, port intstr.IntOrString) corev1.ProbeHandler {
	return corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Scheme: corev1.URISchemeHTTPS, Path: path, Port: port}}
}

func resources(override *corev1.ResourceRequirements, cpu, memory string) corev1.ResourceRequirements {
	if override != nil {
		return *override
	}
	return corev1.ResourceRequirements{Requests: corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse(cpu),
		corev1.ResourceMemory: resource.MustParse(memory),
	}}
}

func mounts(names ...string) []corev1.VolumeMount {
	out := make([]corev1.VolumeMount, 0, len(names))
	for _, name := range names {
		out = append(out, corev1.VolumeMount{Name: name, MountPath: "/etc/brygga/" + name, ReadOnly: true})
	}
	return out
}

func secretVolumes(cp string, byVolume map[string]bryggav1alpha1.Secret) []corev1.Volume {
	out := make([]corev1.Volume, 0, len(byVolume))
	for _, name := range slices.Sorted(maps.Keys(byVolume)) {
		out = append(out, corev1.Volume{Name: name, VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: byVolume[name].Name(cp)}}})
	}
	return out
}

func configMapVolume(name, configMap string) corev1.Volume {
	return corev1.Volume{Name: name, VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
		LocalObjectReference: corev1.LocalObjectReference{Name: configMap},
	}}}
}

func joined(cidrs []bryggav1alpha1.CIDR) string {
	out := make([]string, 0, len(cidrs))
	for _, cidr := range cidrs {
		out = append(out, string(cidr))
	}
	return strings.Join(out, ",")
}
