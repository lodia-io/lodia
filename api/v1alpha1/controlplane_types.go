// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// ControlPlane is one tenant cluster's control plane: an API server with its
// konnectivity server, a controller manager, a scheduler and a datastore,
// running as pods in this namespace.
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=cp
// +kubebuilder:validation:XValidation:rule="self.metadata.name.size() <= 32 && self.metadata.name.matches('^[a-z]([-a-z0-9]*[a-z0-9])?$')",message="the name must be a DNS-1035 label of at most 32 characters"
// +kubebuilder:printcolumn:name="Version",type=string,JSONPath=".spec.version"
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="Reason",type=string,JSONPath=".status.conditions[?(@.type=='Ready')].reason"
// +kubebuilder:printcolumn:name="Endpoint",type=string,JSONPath=".status.endpoint.apiServer.host",priority=1
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"
type ControlPlane struct {
	metav1.TypeMeta `json:",inline"`

	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +required
	Spec ControlPlaneSpec `json:"spec"`

	// +optional
	Status ControlPlaneStatus `json:"status,omitzero"`
}

// ControlPlaneList is a list of ControlPlanes.
// +kubebuilder:object:root=true
type ControlPlaneList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ControlPlane `json:"items"`
}

// ControlPlaneSpec is what the owner of the cluster wants.
// +kubebuilder:validation:XValidation:rule="!has(self.highAvailability) || !has(self.apiServer) || !has(self.apiServer.replicas) || self.apiServer.replicas >= 2",message="highAvailability runs at least two API servers"
// +kubebuilder:validation:XValidation:rule="!has(self.apiServer) || !has(self.apiServer.advertiseAddress) || has(self.endpoint.gateway)",message="apiServer.advertiseAddress is set only with endpoint.gateway"
// +kubebuilder:validation:XValidation:rule="!has(self.apiServer) || !has(self.apiServer.advertiseAddress) || !isIP(self.apiServer.advertiseAddress) || !isCIDR(self.network.serviceCIDRs[0]) || ip(self.apiServer.advertiseAddress).family() == cidr(self.network.serviceCIDRs[0]).ip().family()",message="apiServer.advertiseAddress must be of the family of the first service range"
// +kubebuilder:validation:XValidation:rule="!has(self.endpoint.gateway) || has(self.apiServer) && has(self.apiServer.advertiseAddress) || !isCIDR(self.network.serviceCIDRs[0]) || cidr(self.network.serviceCIDRs[0]).ip().family() == 4",message="an IPv6 first service range in gateway mode needs apiServer.advertiseAddress, which has no IPv6 default"
// +kubebuilder:validation:XValidation:rule="(has(self.apiServer) && has(self.apiServer.advertiseAddress) ? [self.apiServer.advertiseAddress] : has(self.endpoint.gateway) ? ['240.0.0.1'] : []).all(a, !isIP(a) || !(self.network.serviceCIDRs + self.network.podCIDRs).exists(c, isCIDR(c) && cidr(c).containsIP(a)))",message="the advertise address, apiServer.advertiseAddress or 240.0.0.1 in gateway mode, must lie outside the service and pod ranges"
// +kubebuilder:validation:XValidation:rule="!has(self.endpoint.gateway) || (has(self.apiServer) && has(self.apiServer.advertiseAddress) ? has(oldSelf.apiServer) && has(oldSelf.apiServer.advertiseAddress) && oldSelf.apiServer.advertiseAddress == self.apiServer.advertiseAddress : !has(oldSelf.apiServer) || !has(oldSelf.apiServer.advertiseAddress))",message="apiServer.advertiseAddress is immutable"
// +kubebuilder:validation:XValidation:rule="has(self.restoreFrom) == has(oldSelf.restoreFrom)",message="restoreFrom can be set only when the ControlPlane is created, and never removed"
// +kubebuilder:validation:XValidation:rule="has(self.datastore) && has(self.datastore.className) && size(self.datastore.className) > 0 ? has(oldSelf.datastore) && has(oldSelf.datastore.className) && oldSelf.datastore.className == self.datastore.className : !has(oldSelf.datastore) || !has(oldSelf.datastore.className) || size(oldSelf.datastore.className) == 0",message="datastore.className is immutable in v1alpha1"
// +kubebuilder:validation:XValidation:rule="!has(self.controllerManager) || !has(self.controllerManager.extraArgs) || !('node-cidr-mask-size' in self.controllerManager.extraArgs) || size(self.network.podCIDRs) == 1",message="kube-controller-manager refuses node-cidr-mask-size with two pod ranges: use node-cidr-mask-size-ipv4 and node-cidr-mask-size-ipv6"
// +kubebuilder:validation:XValidation:rule="!has(self.controllerManager) || !has(self.controllerManager.extraArgs) || !('node-cidr-mask-size-ipv4' in self.controllerManager.extraArgs) || size(self.network.podCIDRs) != 1 || !isCIDR(self.network.podCIDRs[0]) || cidr(self.network.podCIDRs[0]).ip().family() == 4",message="kube-controller-manager refuses node-cidr-mask-size-ipv4 with an IPv6 pod range alone"
// +kubebuilder:validation:XValidation:rule="!has(self.controllerManager) || !has(self.controllerManager.extraArgs) || !('node-cidr-mask-size-ipv6' in self.controllerManager.extraArgs) || size(self.network.podCIDRs) != 1 || !isCIDR(self.network.podCIDRs[0]) || cidr(self.network.podCIDRs[0]).ip().family() == 6",message="kube-controller-manager refuses node-cidr-mask-size-ipv6 with an IPv4 pod range alone"
// +kubebuilder:validation:XValidation:rule="self.network.podCIDRs.all(c, !isCIDR(c) || (cidr(c).ip().family() == 4 ? cidr(c).prefixLength() <= (has(self.controllerManager) && has(self.controllerManager.extraArgs) && 'node-cidr-mask-size-ipv4' in self.controllerManager.extraArgs ? int(self.controllerManager.extraArgs['node-cidr-mask-size-ipv4']) : has(self.controllerManager) && has(self.controllerManager.extraArgs) && 'node-cidr-mask-size' in self.controllerManager.extraArgs ? int(self.controllerManager.extraArgs['node-cidr-mask-size']) : 24) : cidr(c).prefixLength() <= (has(self.controllerManager) && has(self.controllerManager.extraArgs) && 'node-cidr-mask-size-ipv6' in self.controllerManager.extraArgs ? int(self.controllerManager.extraArgs['node-cidr-mask-size-ipv6']) : has(self.controllerManager) && has(self.controllerManager.extraArgs) && 'node-cidr-mask-size' in self.controllerManager.extraArgs ? int(self.controllerManager.extraArgs['node-cidr-mask-size']) : 64) && (has(self.controllerManager) && has(self.controllerManager.extraArgs) && 'node-cidr-mask-size-ipv6' in self.controllerManager.extraArgs ? int(self.controllerManager.extraArgs['node-cidr-mask-size-ipv6']) : has(self.controllerManager) && has(self.controllerManager.extraArgs) && 'node-cidr-mask-size' in self.controllerManager.extraArgs ? int(self.controllerManager.extraArgs['node-cidr-mask-size']) : 64) - cidr(c).prefixLength() <= 16))",message="kube-controller-manager refuses a pod range narrower than the node mask of its family, 24 for IPv4 and 64 for IPv6 unless controllerManager.extraArgs sets it, and an IPv6 range more than 16 bits wider than it"
// +kubebuilder:validation:XValidation:rule="!has(self.serviceAccount) || !has(self.serviceAccount.issuer) || !has(self.apiServer) || !has(self.apiServer.authentication) || !has(self.apiServer.authentication.jwt) || !self.apiServer.authentication.jwt.exists(a, a.issuer.url == self.serviceAccount.issuer)",message="kube-apiserver refuses a JWT issuer.url equal to serviceAccount.issuer"
type ControlPlaneSpec struct {
	// Version is the Kubernetes version, vMAJOR.MINOR.PATCH, any patch of a
	// minor Lodia runs. The minor never decreases; Lodia upgrades one minor
	// at a time towards it.
	// +required
	// +kubebuilder:validation:MaxLength=16
	// +kubebuilder:validation:Pattern=`^v1\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$`
	// +kubebuilder:validation:XValidation:rule="int(self.split('.')[1]) >= int(oldSelf.split('.')[1])",message="the Kubernetes minor version cannot decrease"
	Version string `json:"version"`

	// HighAvailability runs every component with more than one replica,
	// spread by FailureTolerance, and etcd with three members.
	// +optional
	HighAvailability *HighAvailability `json:"highAvailability,omitempty"`

	// Datastore is where the cluster's data lives: an etcd of the tenant's
	// own.
	// +optional
	Datastore DatastoreSettings `json:"datastore,omitzero"`

	// Endpoint is how nodes, people and pods reach the API server.
	// +required
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="endpoint is immutable in v1alpha1"
	Endpoint Endpoint `json:"endpoint"`

	// Network holds the tenant cluster's address ranges.
	// +required
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="network is immutable"
	Network Network `json:"network"`

	// APIServer configures kube-apiserver.
	// +optional
	APIServer *APIServer `json:"apiServer,omitempty"`

	// ControllerManager configures kube-controller-manager.
	// +optional
	ControllerManager *Component `json:"controllerManager,omitempty"`

	// Scheduler configures kube-scheduler.
	// +optional
	Scheduler *Scheduler `json:"scheduler,omitempty"`

	// Addons configures what Lodia runs inside the tenant cluster.
	// +optional
	Addons *Addons `json:"addons,omitempty"`

	// Placement says where this control plane's pods run.
	// +optional
	Placement *Placement `json:"placement,omitempty"`

	// Maintenance is when Lodia may roll changes it initiates.
	// +optional
	Maintenance *Maintenance `json:"maintenance,omitempty"`

	// Health configures how Lodia protects the tenant's workloads.
	// +optional
	Health *Health `json:"health,omitempty"`

	// ServiceAccount configures the tokens the API server issues.
	// +optional
	ServiceAccount *ServiceAccount `json:"serviceAccount,omitempty"`

	// RestoreFrom starts the datastore from a Backup, whose source this
	// ControlPlane replaces; set only at creation.
	// +optional
	RestoreFrom *RestoreFrom `json:"restoreFrom,omitempty"`

	// Paused stops reconciliation; everything keeps running. In gateway mode
	// nothing renews the tunnel's client certificate meanwhile: 30 days after
	// its renewal time every pod of the tenant loses its API.
	// +optional
	Paused bool `json:"paused,omitempty"`
}

// HighAvailability is the failure a control plane survives.
type HighAvailability struct {
	// FailureTolerance is Node, replicas spread over nodes by
	// kubernetes.io/hostname, or Zone, over nodes and over zones by
	// topology.kubernetes.io/zone too. It cannot change once set, since
	// etcd's volumes stay where they were spread; highAvailability can be
	// removed.
	// +required
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="failureTolerance is immutable once set"
	FailureTolerance FailureTolerance `json:"failureTolerance"`
}

// FailureTolerance is the failure domain replicas are spread across.
// +kubebuilder:validation:Enum=Node;Zone
type FailureTolerance string

// DatastoreSettings is how the tenant's etcd is built.
type DatastoreSettings struct {
	// ClassName names the DatastoreClass the etcd is built from; empty
	// means the default class.
	// +optional
	// +kubebuilder:validation:MaxLength=253
	ClassName string `json:"className,omitempty"`
}

// Endpoint is exactly one of Gateway, LoadBalancer and NodePort.
// +kubebuilder:validation:XValidation:rule="[has(self.gateway), has(self.loadBalancer), has(self.nodePort)].filter(x, x).size() == 1",message="set exactly one of gateway, loadBalancer and nodePort"
type Endpoint struct {
	// Gateway attaches routes to a shared Gateway that routes by SNI.
	// +optional
	Gateway *GatewayEndpoint `json:"gateway,omitempty"`

	// LoadBalancer creates a Service of type LoadBalancer for this control plane.
	// +optional
	LoadBalancer *LoadBalancerEndpoint `json:"loadBalancer,omitempty"`

	// NodePort creates a Service of type NodePort; for development.
	// +optional
	NodePort *NodePortEndpoint `json:"nodePort,omitempty"`

	// CertSANs are extra names on the API server's serving certificate.
	// +optional
	// +listType=set
	// +kubebuilder:validation:MaxItems=16
	CertSANs []Hostname `json:"certSANs,omitempty"`
}

// GatewayEndpoint is a shared Gateway and the hostnames this control plane takes on it.
type GatewayEndpoint struct {
	// ParentRef is the Gateway with a TLS passthrough listener.
	// +required
	ParentRef ParentReference `json:"parentRef"`

	// Hostnames are the names routed to this control plane.
	// +required
	Hostnames GatewayHostnames `json:"hostnames"`
}

// ParentReference names a Gateway and one of its listeners.
type ParentReference struct {
	// Name of the Gateway.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`

	// Namespace of the Gateway.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	Namespace string `json:"namespace"`

	// SectionName is the listener's name, required since a route without it
	// lands on every listener its hostname fits.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`
	SectionName string `json:"sectionName"`
}

// GatewayHostnames are the SNI names of one control plane.
// +kubebuilder:validation:XValidation:rule="self.apiServer != self.konnectivity && (!has(self.tunnel) || self.apiServer != self.tunnel && self.konnectivity != self.tunnel)",message="apiServer, konnectivity and tunnel must be three different hostnames"
type GatewayHostnames struct {
	// APIServer is the public API hostname.
	// +required
	APIServer Hostname `json:"apiServer"`

	// Konnectivity is where konnectivity agents connect.
	// +required
	Konnectivity Hostname `json:"konnectivity"`

	// Tunnel is the hostname apiserver-proxy on every node dials, inside
	// TLS, for the tenant's pods to reach their API server.
	// +required
	Tunnel Hostname `json:"tunnel"`
}

// LoadBalancerEndpoint configures the control plane's LoadBalancer Service.
type LoadBalancerEndpoint struct {
	// Class is the Service's loadBalancerClass.
	// +optional
	// +kubebuilder:validation:MaxLength=253
	Class string `json:"class,omitempty"`

	// SourceRanges limit who may connect.
	// +optional
	// +listType=set
	// +kubebuilder:validation:MaxItems=32
	SourceRanges []CIDR `json:"sourceRanges,omitempty"`

	// Annotations are added to the Service.
	// +optional
	// +kubebuilder:validation:MaxProperties=32
	Annotations map[string]string `json:"annotations,omitempty"`
}

// NodePortEndpoint has no settings yet.
type NodePortEndpoint struct{}

// Network holds the tenant cluster's address ranges.
type Network struct {
	// ServiceCIDRs are the ranges Services get addresses from; the first
	// address of the first range is the kubernetes Service, the tenth CoreDNS.
	// +required
	// +listType=atomic
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=2
	ServiceCIDRs []CIDR `json:"serviceCIDRs"`

	// PodCIDRs are the ranges node CIDRs are allocated from.
	// +required
	// +listType=atomic
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=2
	PodCIDRs []CIDR `json:"podCIDRs"`

	// ClusterDomain is the cluster's DNS domain.
	// +optional
	// +kubebuilder:default=cluster.local
	ClusterDomain Hostname `json:"clusterDomain,omitempty"`
}

// APIServer configures kube-apiserver.
type APIServer struct {
	// Replicas of the API server; 1 by default, 2 with high availability.
	// +optional
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=5
	Replicas *int32 `json:"replicas,omitempty"`

	// AdvertiseAddress is the address apiserver-proxy owns on every node in
	// gateway mode: 240.0.0.1 by default when the first service range is
	// IPv4, and required when it is IPv6. It cannot change.
	// +optional
	// +kubebuilder:validation:MaxLength=45
	// +kubebuilder:validation:XValidation:rule="isIP(self) && ip.isCanonical(self) && ip(self).isGlobalUnicast()",message="must be a unicast IP address in canonical form, neither loopback nor link-local"
	AdvertiseAddress string `json:"advertiseAddress,omitempty"`

	// Authentication configures structured authentication; the anonymous
	// section is Lodia's.
	// +optional
	Authentication *Authentication `json:"authentication,omitempty"`

	// Encryption configures encryption of Secrets at rest.
	// +optional
	Encryption *Encryption `json:"encryption,omitempty"`

	// ExtraArgs are flags passed to kube-apiserver, each under its name as
	// kube-apiserver spells it. The flags Lodia sets, those whose defaults it
	// relies on, those that conflict with either and those that end the
	// process are refused, but for enable-admission-plugins, whose entries
	// follow Lodia's; featureGates sets feature-gates.
	// +optional
	// +kubebuilder:validation:MaxProperties=64
	// +kubebuilder:validation:XValidation:rule="self.all(k, k.matches('^[a-z0-9]+(-[a-z0-9]+)*$'))",message="a key must be a flag's name as the component spells it: lowercase letters and digits in words joined by single dashes"
	// +kubebuilder:validation:XValidation:rule="!self.exists(k, k in ['advertise-address', 'allow-privileged', 'authentication-config', 'authorization-mode', 'client-ca-file', 'egress-selector-config-file', 'enable-bootstrap-token-auth', 'encryption-provider-config', 'encryption-provider-config-automatic-reload', 'endpoint-reconciler-type', 'etcd-cafile', 'etcd-certfile', 'etcd-keyfile', 'etcd-servers', 'feature-gates', 'goaway-chance', 'kubelet-client-certificate', 'kubelet-client-key', 'kubelet-preferred-address-types', 'proxy-client-cert-file', 'proxy-client-key-file', 'requestheader-allowed-names', 'requestheader-client-ca-file', 'requestheader-extra-headers-prefix', 'requestheader-group-headers', 'requestheader-username-headers', 'secure-port', 'service-account-issuer', 'service-account-jwks-uri', 'service-account-signing-endpoint', 'service-cluster-ip-range', 'shutdown-delay-duration', 'shutdown-watch-termination-grace-period', 'tls-cert-file', 'tls-private-key-file'])",message="Lodia sets this flag"
	// +kubebuilder:validation:XValidation:rule="!self.exists(k, k in ['bind-address', 'etcd-prefix', 'etcd-servers-overrides', 'shutdown-send-retry-after'])",message="Lodia relies on this flag's default"
	// +kubebuilder:validation:XValidation:rule="!self.exists(k, k in ['admission-control', 'anonymous-auth', 'authorization-config', 'authorization-policy-file', 'authorization-webhook-cache-authorized-ttl', 'authorization-webhook-cache-unauthorized-ttl', 'authorization-webhook-config-file', 'authorization-webhook-version', 'cert-dir', 'service-account-key-file', 'service-account-signing-key-file'] || k.startsWith('oidc-'))",message="this flag conflicts with one Lodia sets"
	// +kubebuilder:validation:XValidation:rule="!self.exists(k, k in ['help', 'version'])",message="this flag makes the component exit, or report another version than the one it runs"
	// +kubebuilder:validation:XValidation:rule="!('disable-admission-plugins' in self) || self['disable-admission-plugins'].matches('^[A-Za-z0-9]+(,[A-Za-z0-9]+)*$')",message="disable-admission-plugins is plugin names of letters and digits joined by commas, and nothing else, since kube-apiserver reads it as CSV"
	// +kubebuilder:validation:XValidation:rule="!('disable-admission-plugins' in self) || !self['disable-admission-plugins'].matches('(^|,)NodeRestriction(,|$)')",message="disable-admission-plugins cannot name NodeRestriction, which Lodia enables"
	// +kubebuilder:validation:XValidation:rule="!('service-account-max-token-expiration' in self) || size(self['service-account-max-token-expiration']) > 0 && self['service-account-max-token-expiration'].matches('^([1-9][0-9]*h)?([1-9]m|[1-5][0-9]m)?([1-9]s|[1-5][0-9]s)?$')",message="service-account-max-token-expiration is a duration as Go prints it, without its zero units: hours, then minutes and seconds under 60, as 720h or 1h30m"
	// +kubebuilder:validation:XValidation:rule="!('service-account-max-token-expiration' in self) || duration(self['service-account-max-token-expiration']) >= duration('1h') && duration(self['service-account-max-token-expiration']) <= duration('8760h')",message="kube-apiserver refuses a service-account-max-token-expiration other than 0 under 1h, and one above 8760h, the longest token lodia-signer signs; 0 means 8760h, so leave the flag out instead"
	ExtraArgs map[string]string `json:"extraArgs,omitempty"`

	// FeatureGates are passed to kube-apiserver.
	// +optional
	// +kubebuilder:validation:MaxProperties=64
	// +kubebuilder:validation:XValidation:rule="!('ExternalServiceAccountTokenSigner' in self) || self['ExternalServiceAccountTokenSigner']",message="kube-apiserver refuses service-account-signing-endpoint, which Lodia sets, without ExternalServiceAccountTokenSigner"
	// +kubebuilder:validation:XValidation:rule="!('StructuredAuthenticationConfigurationEgressSelector' in self) || self['StructuredAuthenticationConfigurationEgressSelector']",message="kube-apiserver refuses the egressSelectorType Lodia renders for every JWT authenticator without StructuredAuthenticationConfigurationEgressSelector"
	FeatureGates map[string]bool `json:"featureGates,omitempty"`

	// Resources override the operator's defaults.
	// +optional
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`
}

// Encryption is how Secrets are encrypted in the datastore.
type Encryption struct {
	// Provider is None, secretbox or aescbc.
	// +optional
	// +kubebuilder:default=secretbox
	Provider EncryptionProvider `json:"provider,omitempty"`
}

// EncryptionProvider is an encryption at rest provider.
// +kubebuilder:validation:Enum=None;secretbox;aescbc
type EncryptionProvider string

// Component configures kube-controller-manager.
type Component struct {
	// ExtraArgs are flags passed to the component, each under its name as the
	// component spells it. The flags Lodia sets, those whose defaults it
	// relies on, those that conflict with either and those that end the
	// process are refused, but for controllers, whose entries follow Lodia's;
	// featureGates sets feature-gates.
	// +optional
	// +kubebuilder:validation:MaxProperties=64
	// +kubebuilder:validation:XValidation:rule="self.all(k, k.matches('^[a-z0-9]+(-[a-z0-9]+)*$'))",message="a key must be a flag's name as the component spells it: lowercase letters and digits in words joined by single dashes"
	// +kubebuilder:validation:XValidation:rule="!self.exists(k, k in ['allocate-node-cidrs', 'authentication-kubeconfig', 'authorization-kubeconfig', 'bind-address', 'client-ca-file', 'cluster-cidr', 'cluster-name', 'cluster-signing-duration', 'cluster-signing-kubelet-client-cert-file', 'cluster-signing-kubelet-client-key-file', 'feature-gates', 'kubeconfig', 'leader-elect', 'leader-elect-lease-duration', 'leader-elect-renew-deadline', 'leader-elect-resource-lock', 'leader-elect-resource-name', 'leader-elect-resource-namespace', 'leader-elect-retry-period', 'node-monitor-grace-period', 'node-monitor-period', 'requestheader-client-ca-file', 'root-ca-file', 'service-cluster-ip-range', 'use-service-account-credentials'])",message="Lodia sets this flag"
	// +kubebuilder:validation:XValidation:rule="!self.exists(k, k in ['authorization-always-allow-paths', 'secure-port'])",message="Lodia relies on this flag's default"
	// +kubebuilder:validation:XValidation:rule="!self.exists(k, k in ['cluster-signing-cert-file', 'cluster-signing-key-file', 'master', 'service-account-private-key-file'])",message="this flag conflicts with one Lodia sets"
	// +kubebuilder:validation:XValidation:rule="['node-cidr-mask-size', 'node-cidr-mask-size-ipv4', 'node-cidr-mask-size-ipv6'].all(k, !(k in self) || self[k].matches('^[1-9][0-9]*$'))",message="a node mask size is a positive decimal number without a sign or a leading zero, since kube-controller-manager reads 024 as octal and 0 as unset"
	// +kubebuilder:validation:XValidation:rule="!self.exists(k, k in ['help', 'version'])",message="this flag makes the component exit, or report another version than the one it runs"
	ExtraArgs map[string]string `json:"extraArgs,omitempty"`

	// FeatureGates are passed to the component.
	// +optional
	// +kubebuilder:validation:MaxProperties=64
	FeatureGates map[string]bool `json:"featureGates,omitempty"`

	// Resources override the operator's defaults.
	// +optional
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`
}

// Scheduler configures kube-scheduler.
type Scheduler struct {
	// ExtraArgs are flags passed to kube-scheduler, each under its name as
	// kube-scheduler spells it. The flags Lodia sets, those whose defaults it
	// relies on, those that conflict with either and those that end the
	// process are refused; featureGates sets feature-gates.
	// +optional
	// +kubebuilder:validation:MaxProperties=64
	// +kubebuilder:validation:XValidation:rule="self.all(k, k.matches('^[a-z0-9]+(-[a-z0-9]+)*$'))",message="a key must be a flag's name as the component spells it: lowercase letters and digits in words joined by single dashes"
	// +kubebuilder:validation:XValidation:rule="!self.exists(k, k in ['authentication-kubeconfig', 'authorization-kubeconfig', 'bind-address', 'feature-gates', 'kubeconfig', 'leader-elect', 'leader-elect-resource-lock', 'leader-elect-resource-name', 'leader-elect-resource-namespace'])",message="Lodia sets this flag"
	// +kubebuilder:validation:XValidation:rule="!self.exists(k, k in ['authorization-always-allow-paths', 'secure-port'])",message="Lodia relies on this flag's default"
	// +kubebuilder:validation:XValidation:rule="!self.exists(k, k in ['config', 'master'])",message="this flag conflicts with one Lodia sets"
	// +kubebuilder:validation:XValidation:rule="!self.exists(k, k in ['help', 'version', 'write-config-to'])",message="this flag makes the component exit, or report another version than the one it runs"
	ExtraArgs map[string]string `json:"extraArgs,omitempty"`

	// FeatureGates are passed to kube-scheduler.
	// +optional
	// +kubebuilder:validation:MaxProperties=64
	FeatureGates map[string]bool `json:"featureGates,omitempty"`

	// Resources override the operator's defaults.
	// +optional
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`
}

// Addons are the components Lodia runs inside the tenant cluster.
type Addons struct {
	// Konnectivity configures the tunnel between the control plane and the nodes.
	// +optional
	Konnectivity *Konnectivity `json:"konnectivity,omitempty"`

	// KubeProxy configures kube-proxy.
	// +optional
	KubeProxy *KubeProxy `json:"kubeProxy,omitempty"`

	// CoreDNS configures cluster DNS.
	// +optional
	CoreDNS *CoreDNS `json:"coreDNS,omitempty"`
}

// Konnectivity configures konnectivity-server and its agents.
type Konnectivity struct {
	// Version pins the konnectivity version across a Kubernetes upgrade;
	// empty follows the version table.
	// +optional
	// +kubebuilder:validation:Pattern=`^v0\.[0-9]+\.[0-9]+$`
	// +kubebuilder:validation:MaxLength=16
	Version string `json:"version,omitempty"`

	// Agent configures the agents in the tenant.
	// +optional
	Agent *KonnectivityAgent `json:"agent,omitempty"`
}

// KonnectivityAgent configures how agents run.
type KonnectivityAgent struct {
	// Mode is DaemonSet, an agent per node, or Deployment.
	// +optional
	// +kubebuilder:default=DaemonSet
	Mode AgentMode `json:"mode,omitempty"`

	// Replicas of the Deployment mode.
	// +optional
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=10
	Replicas *int32 `json:"replicas,omitempty"`
}

// AgentMode is how konnectivity agents run.
// +kubebuilder:validation:Enum=DaemonSet;Deployment
type AgentMode string

// KubeProxy configures kube-proxy.
type KubeProxy struct {
	// Enabled is false for CNIs that replace kube-proxy.
	// +optional
	// +kubebuilder:default=true
	Enabled *bool `json:"enabled,omitempty"`

	// Mode is iptables, nftables or ipvs.
	// +optional
	// +kubebuilder:default=iptables
	Mode ProxyMode `json:"mode,omitempty"`

	// HealthzBindAddress is the IP address kube-proxy serves its health port,
	// 10256, on: beyond loopback for cloud load balancers; loopback by default.
	// +optional
	// +kubebuilder:validation:MaxLength=45
	// +kubebuilder:validation:XValidation:rule="isIP(self)",message="must be an IP address"
	HealthzBindAddress string `json:"healthzBindAddress,omitempty"`

	// Conntrack configures kube-proxy's connection tracking.
	// +optional
	Conntrack *KubeProxyConntrack `json:"conntrack,omitempty"`
}

// KubeProxyConntrack is kube-proxy's conntrack configuration.
type KubeProxyConntrack struct {
	// MaxPerCore is kube-proxy's conntrack.maxPerCore, the connections tracked
	// per CPU core; 0 leaves the node's nf_conntrack_max as it is. Unset
	// means kube-proxy's default.
	// +optional
	// +kubebuilder:validation:Minimum=0
	MaxPerCore *int32 `json:"maxPerCore,omitempty"`
}

// ProxyMode is kube-proxy's mode.
// +kubebuilder:validation:Enum=iptables;nftables;ipvs
type ProxyMode string

// CoreDNS configures cluster DNS.
type CoreDNS struct {
	// Enabled is false when the tenant brings its own DNS.
	// +optional
	// +kubebuilder:default=true
	Enabled *bool `json:"enabled,omitempty"`

	// Replicas of CoreDNS.
	// +optional
	// +kubebuilder:default=2
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=10
	Replicas *int32 `json:"replicas,omitempty"`
}

// Placement says where a control plane's pods run.
type Placement struct {
	// NodeSelector for every control plane pod.
	// +optional
	// +kubebuilder:validation:MaxProperties=16
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// Tolerations for every control plane pod.
	// +optional
	// +listType=atomic
	// +kubebuilder:validation:MaxItems=16
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`

	// PriorityClassName for every control plane pod.
	// +optional
	// +kubebuilder:validation:MaxLength=253
	PriorityClassName string `json:"priorityClassName,omitempty"`
}

// Maintenance is when Lodia may roll the changes it initiates.
type Maintenance struct {
	// Window is the daily maintenance window; without one, any time.
	// +optional
	Window *MaintenanceWindow `json:"window,omitempty"`

	// AutoUpdate opts into updates Lodia applies in the window.
	// +optional
	AutoUpdate *AutoUpdate `json:"autoUpdate,omitempty"`
}

// MaintenanceWindow is daily, from Begin up to End in its time zone; it
// crosses midnight when End is before Begin.
// +kubebuilder:validation:XValidation:rule="self.begin != self.end",message="begin and end must differ"
type MaintenanceWindow struct {
	// Begin is HH:MM.
	// +required
	// +kubebuilder:validation:Pattern=`^([01][0-9]|2[0-3]):[0-5][0-9]$`
	Begin string `json:"begin"`

	// End is HH:MM.
	// +required
	// +kubebuilder:validation:Pattern=`^([01][0-9]|2[0-3]):[0-5][0-9]$`
	End string `json:"end"`

	// TimeZone is an IANA time zone; UTC by default. One the operator's
	// embedded zone database lacks is TimeZoneUnsupported.
	// +optional
	// +kubebuilder:validation:MaxLength=64
	TimeZone string `json:"timeZone,omitempty"`
}

// AutoUpdate lists the updates Lodia applies on its own.
type AutoUpdate struct {
	// KubernetesPatch moves to the newest patch of the running minor that
	// the operator's version table names, in the window.
	// +optional
	KubernetesPatch bool `json:"kubernetesPatch,omitempty"`
}

// Health configures the protection of the tenant's workloads.
type Health struct {
	// HoldControllerManager holds the controller manager's leader lease while
	// most nodes cannot reach the API, so nothing evicts their pods.
	// +optional
	// +kubebuilder:default=true
	HoldControllerManager *bool `json:"holdControllerManager,omitempty"`
}

// ServiceAccount configures ServiceAccount tokens.
type ServiceAccount struct {
	// Issuer is the tokens' issuer and the OIDC discovery URL; the public API
	// URL by default.
	// +optional
	// +kubebuilder:validation:MaxLength=512
	// +kubebuilder:validation:XValidation:rule="self.startsWith('https://')",message="must be an https URL"
	Issuer string `json:"issuer,omitempty"`
}

// RestoreFrom names the Backup a new control plane starts from. The
// ControlPlane serves the Backup's data from the start, but runs no
// controller manager until it is released and no ControlPlane with the
// Backup's source UID exists in the namespace.
type RestoreFrom struct {
	// BackupRef names a Backup in the ControlPlane's namespace, never another
	// tenant's.
	// +required
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="restoreFrom.backupRef is immutable"
	BackupRef LocalReference `json:"backupRef"`

	// Release is Pending until the owner has made sure nothing outside the
	// cluster acts on the restored objects, then Released, for good.
	// +optional
	// +kubebuilder:default=Pending
	// +kubebuilder:validation:XValidation:rule="self == oldSelf || oldSelf == 'Pending'",message="restoreFrom.release moves only from Pending to Released"
	Release RestoreRelease `json:"release,omitempty"`
}

// RestoreRelease says whether a restored control plane may run its
// controller manager.
// +kubebuilder:validation:Enum=Pending;Released
type RestoreRelease string

const (
	// RestorePending keeps the controller manager stopped.
	RestorePending RestoreRelease = "Pending"
	// RestoreReleased lets it run once the Backup's source is gone.
	RestoreReleased RestoreRelease = "Released"
)

// ControlPlaneStatus is what Lodia observed and did.
type ControlPlaneStatus struct {
	// ObservedGeneration is the newest generation every readiness condition
	// has been reported for.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions, each written by the one controller that owns its type, and
	// Progressing and Ready by Lifecycle.
	// +optional
	// +listType=map
	// +listMapKey=type
	// +kubebuilder:validation:MaxItems=16
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// Target is what Lifecycle rolls now.
	// +optional
	Target *Target `json:"target,omitempty"`

	// Kubernetes is the version every component runs and the upgrade in progress.
	// +optional
	Kubernetes *KubernetesStatus `json:"kubernetes,omitempty"`

	// Rehearsal is the minor step the condition Rehearsed is about.
	// +optional
	Rehearsal *RehearsalStatus `json:"rehearsal,omitempty"`

	// Components are the rollouts of the API server, the controller manager
	// and the scheduler.
	// +optional
	Components *ComponentsStatus `json:"components,omitempty"`

	// Addons is what Tenant last applied in full inside the tenant.
	// +optional
	Addons *Rendered `json:"addons,omitempty"`

	// Endpoint is where the control plane is reachable.
	// +optional
	Endpoint *EndpointStatus `json:"endpoint,omitempty"`

	// Credentials are the Secrets clients need.
	// +optional
	Credentials *CredentialsStatus `json:"credentials,omitempty"`

	// Datastore is the store in use.
	// +optional
	Datastore *DatastoreStatus `json:"datastore,omitempty"`

	// Restore is the restore this ControlPlane's data came from. The Datastore
	// controller writes it and keeps it for the ControlPlane's life, with or
	// without a store.
	// +optional
	Restore *RestoreStatus `json:"restore,omitempty"`

	// Nodes summarise the tenant's nodes.
	// +optional
	Nodes *NodesStatus `json:"nodes,omitempty"`

	// Health is present while Lodia holds the controller manager.
	// +optional
	Health *HealthStatus `json:"health,omitempty"`

	// Maintenance is the maintenance Lodia was asked for and did.
	// +optional
	Maintenance *MaintenanceStatus `json:"maintenance,omitempty"`
}

// Target is the state Lifecycle rolls towards now.
type Target struct {
	// Version is the Kubernetes version the API server rolls to now: one
	// minor at a time towards spec.version, and with
	// maintenance.autoUpdate.kubernetesPatch the newest patch of that minor,
	// moved in the window.
	// +optional
	// +kubebuilder:validation:MaxLength=16
	Version string `json:"version,omitempty"`

	// OperatorRevision is the operator revision this tenant may be rendered
	// with: the operator's build version and a digest of its rendering
	// configuration, compared for equality only.
	// +optional
	// +kubebuilder:validation:MaxLength=128
	OperatorRevision string `json:"operatorRevision,omitempty"`

	// DatastoreClass is the UID and generation of the DatastoreClass that
	// status.datastore.class names which the Etcd may run: an edit of the
	// class, or a class created again under its name, reaches the Etcd once
	// Lifecycle grants it, in the window.
	// +optional
	DatastoreClass *DatastoreClassGeneration `json:"datastoreClass,omitempty"`

	// ScheduledFor is when a change Lodia initiates may roll: while it waits
	// for the window, the window's next start; while it waits for a slot of
	// the fleet, when it became eligible, in its window and Ready, which
	// orders the queue.
	// +optional
	ScheduledFor *metav1.Time `json:"scheduledFor,omitempty"`
}

// KubernetesStatus is the running version and the upgrade in progress.
type KubernetesStatus struct {
	// Version is the oldest of the versions the components and the addons
	// report, and absent until all of them do.
	// +optional
	// +kubebuilder:validation:MaxLength=16
	Version string `json:"version,omitempty"`

	// Upgrade is present from the moment a version change starts, a patch
	// moving the target or a minor step passing its preflight, until every
	// component and the addons run target.version.
	// +optional
	Upgrade *UpgradeStatus `json:"upgrade,omitempty"`
}

// RehearsalStatus names the minor step a rehearsal ran for.
type RehearsalStatus struct {
	// Version is the Kubernetes version the step goes to.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=16
	Version string `json:"version"`
}

// UpgradeStatus is the version change in progress.
type UpgradeStatus struct {
	// Step is Rehearsal while a minor step is rehearsed on a copy of the
	// API server, then Backup while its pre-upgrade Backup is taken, both
	// before the target moves, then the first of the others not at the
	// target yet.
	// +required
	Step UpgradeStep `json:"step"`

	// StartedAt is when the version change started.
	// +required
	StartedAt metav1.Time `json:"startedAt"`
}

// UpgradeStep is a step of a version change, in their order: the rehearsal
// and the pre-upgrade Backup of a minor step, then the API server, the
// controller manager and the scheduler, and the objects inside the tenant.
// +kubebuilder:validation:Enum=Rehearsal;Backup;APIServer;ControllerManager;Scheduler;Addons
type UpgradeStep string

const (
	UpgradeStepRehearsal         UpgradeStep = "Rehearsal"
	UpgradeStepBackup            UpgradeStep = "Backup"
	UpgradeStepAPIServer         UpgradeStep = "APIServer"
	UpgradeStepControllerManager UpgradeStep = "ControllerManager"
	UpgradeStepScheduler         UpgradeStep = "Scheduler"
	UpgradeStepAddons            UpgradeStep = "Addons"
)

// ComponentsStatus is the rollout of each control plane component.
type ComponentsStatus struct {
	// +optional
	APIServer *ComponentStatus `json:"apiServer,omitempty"`
	// +optional
	ControllerManager *ComponentStatus `json:"controllerManager,omitempty"`
	// +optional
	Scheduler *ComponentStatus `json:"scheduler,omitempty"`
}

// ComponentStatus counts one component's replicas, those its Deployment asks
// for, and says what all of them run.
type ComponentStatus struct {
	RolloutCount `json:",inline"`

	Rendered `json:",inline"`
}

// Rendered is what a renderer rolled out in full: for a component, its
// template once every replica is updated and ready, the previous one until
// then; for the addons, the render last applied whole.
type Rendered struct {
	// Version is the Kubernetes version of that render.
	// +optional
	// +kubebuilder:validation:MaxLength=16
	Version string `json:"version,omitempty"`

	// OperatorRevision is the operator revision of that render, which the
	// annotation lodia.io/operator-revision on its objects records.
	// +optional
	// +kubebuilder:validation:MaxLength=128
	OperatorRevision string `json:"operatorRevision,omitempty"`
}

// EndpointStatus is where the control plane is reachable from outside the
// management cluster and from the tenant's pods. Workloads writes it whole
// once the public address exists; until then it is absent.
type EndpointStatus struct {
	// APIServer is the API server's public endpoint.
	// +required
	APIServer HostPort `json:"apiServer"`

	// Konnectivity is where konnectivity agents reach the konnectivity
	// server.
	// +required
	Konnectivity HostPort `json:"konnectivity"`

	// InCluster is where the tenant's pods reach the API server: the
	// endpoint of its kubernetes Service, which Tenant writes.
	// +required
	// +kubebuilder:validation:XValidation:rule="isIP(self.host)",message="must be an IP address"
	InCluster HostPort `json:"inCluster"`

	// Tunnel is where apiserver-proxy reaches the API server for the
	// tenant's pods; in gateway mode only.
	// +optional
	Tunnel *HostPort `json:"tunnel,omitempty"`
}

// HostPort is a host and a port.
type HostPort struct {
	// Host is an IP address, an IPv6 one without brackets, or a lowercase DNS
	// name: labels of at most 63 characters, and not digits and dots alone.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:XValidation:rule="isIP(self) || self.matches('^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?([.][a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?)*$') && !self.matches('^[0-9.]+$')",message="must be an IP address or a lowercase DNS name"
	Host string `json:"host"`

	// Port is a TCP port.
	// +required
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	Port int32 `json:"port"`
}

// CredentialsStatus names the Secrets clients need.
type CredentialsStatus struct {
	// AdminKubeconfigSecretRef names the admin kubeconfig Secret.
	// +optional
	AdminKubeconfigSecretRef *LocalReference `json:"adminKubeconfigSecretRef,omitempty"`

	// CABundleSecretRef names the CAs a client must trust now.
	// +optional
	CABundleSecretRef *LocalReference `json:"caBundleSecretRef,omitempty"`

	// NotAfter is the earliest expiry among the certificates PKI's Secrets
	// hold for this ControlPlane, its CAs among them. It counts each Secret's
	// own certificate and the newest complete etcd generation, and leaves out
	// the replaced certificates in the CA bundles, the staged kubelet signer
	// and a previous etcd generation.
	// +optional
	NotAfter *metav1.Time `json:"notAfter,omitempty"`
}

// DatastoreStatus is the store the API server uses. The Datastore
// controller writes it whole once the store exists.
type DatastoreStatus struct {
	// Endpoints are where the API server reaches etcd.
	// +required
	// +listType=atomic
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=9
	Endpoints []HostPort `json:"endpoints"`

	// Class is the DatastoreClass a dedicated etcd was built from.
	// +optional
	Class *DatastoreClassStatus `json:"class,omitempty"`

	// Members counts the etcd members.
	// +required
	Members RolloutCount `json:"members"`

	// Snapshots is where the etcd's continuous snapshots go.
	// +optional
	Snapshots *SnapshotLocation `json:"snapshots,omitempty"`

	// OperatorRevision is the operator revision the Etcd was rendered with.
	// +optional
	// +kubebuilder:validation:MaxLength=128
	OperatorRevision string `json:"operatorRevision,omitempty"`
}

// SnapshotLocation is where snapshots are: a store, a bucket in it and a
// prefix in the bucket.
type SnapshotLocation struct {
	// +required
	Provider StoreProvider `json:"provider"`

	// Endpoint of an S3-compatible store; empty for the provider's own.
	// +optional
	// +kubebuilder:validation:MaxLength=512
	Endpoint string `json:"endpoint,omitempty"`

	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	Bucket string `json:"bucket"`

	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=512
	Prefix string `json:"prefix"`
}

// RestoreStatus is a completed restore from a Backup: the Datastore
// controller writes it once the restored data is cleaned up and a full
// snapshot of it is in the store.
type RestoreStatus struct {
	// Backup is the Backup restored.
	// +required
	Backup UIDReference `json:"backup"`

	// Source is the ControlPlane the Backup was taken of, which this one
	// replaces: while one with its UID exists in the namespace, the
	// controller manager does not run.
	// +required
	Source UIDReference `json:"source"`

	// CompletedAt is when the restored data was cleaned up and snapshotted.
	// +required
	CompletedAt metav1.Time `json:"completedAt"`
}

// RolloutCount counts a component's replicas or an etcd's members: how many
// the spec asks for, how many of them are ready, and how many run the current
// spec.
type RolloutCount struct {
	Count `json:",inline"`

	// Updated run the current spec and are available; absent until the
	// controllers that roll them out have observed that spec.
	// +optional
	// +kubebuilder:validation:Minimum=0
	Updated *int32 `json:"updated,omitempty"`
}

// DatastoreClassStatus is a DatastoreClass at the generation an etcd was
// built from.
type DatastoreClassStatus struct {
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`

	DatastoreClassGeneration `json:",inline"`
}

// DatastoreClassGeneration is a generation of one DatastoreClass: a class
// deleted and created again under its name has another UID, and starts again
// at generation 1.
type DatastoreClassGeneration struct {
	// UID of the class.
	// +required
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=36
	UID types.UID `json:"uid"`

	// +required
	// +kubebuilder:validation:Minimum=1
	Generation int64 `json:"generation"`
}

// Count is how many there are and how many of them are ready.
type Count struct {
	// +required
	// +kubebuilder:validation:Minimum=0
	Total int32 `json:"total"`

	// Ready never exceeds Total.
	// +required
	// +kubebuilder:validation:Minimum=0
	Ready int32 `json:"ready"`
}

// NodesStatus summarises the tenant's nodes.
type NodesStatus struct {
	Count `json:",inline"`

	// OutdatedKubelets run an older minor than the control plane.
	// +optional
	OutdatedKubelets int32 `json:"outdatedKubelets,omitempty"`

	// OldestKubelet is the oldest version a node's kubelet reports.
	// +optional
	// +kubebuilder:validation:MaxLength=64
	OldestKubelet string `json:"oldestKubelet,omitempty"`

	// Truncated is true when Health stopped listing the Nodes at a bound: total
	// and ready count what it read, and outdatedKubelets and oldestKubelet are
	// absent.
	// +optional
	Truncated bool `json:"truncated,omitempty"`
}

// HealthStatus reports the controller manager hold.
type HealthStatus struct {
	// ControllerManagerHold is present while the hold lasts.
	// +optional
	ControllerManagerHold *ControllerManagerHold `json:"controllerManagerHold,omitempty"`
}

// MaintenanceStatus is the maintenance Lodia was asked for and did.
type MaintenanceStatus struct {
	// LastHandled is the value of the annotation
	// lodia.io/maintain-requested-at that Lifecycle acted on last; a value
	// longer than this field holds is not handled.
	// +optional
	// +kubebuilder:validation:MaxLength=256
	LastHandled string `json:"lastHandled,omitempty"`

	// SettlingSince is when Lifecycle first found the change it started in
	// the tenant's slot of the fleet no longer rolling; the slot is kept for a
	// settling time after it.
	// +optional
	SettlingSince *metav1.Time `json:"settlingSince,omitempty"`

	// SlotSince is when the change Lifecycle started took a slot of the
	// fleet: the tenant holds the slot while this is present, until the change
	// settles or the ControlPlane is deleted.
	// +optional
	SlotSince *metav1.Time `json:"slotSince,omitempty"`
}

// ControllerManagerHold is one hold of the controller manager's leader lease.
type ControllerManagerHold struct {
	// +optional
	Since *metav1.Time `json:"since,omitempty"`
	// +optional
	SilentNodes int32 `json:"silentNodes,omitempty"`
	// +optional
	CountedNodes int32 `json:"countedNodes,omitempty"`
}
