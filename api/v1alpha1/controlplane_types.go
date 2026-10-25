// Copyright 2026 The Brygga Authors.
// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ControlPlane is one tenant cluster's control plane: an API server with its
// konnectivity server, a controller manager, a scheduler and a datastore,
// running as pods in this namespace.
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=cp
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
// +kubebuilder:validation:XValidation:rule="self.layout != 'Compact' || !has(self.highAvailability)",message="the Compact layout runs one replica of everything: remove highAvailability"
// +kubebuilder:validation:XValidation:rule="self.layout != 'Compact' || !has(self.apiServer) || !has(self.apiServer.replicas) || self.apiServer.replicas == 1",message="the Compact layout runs one API server"
// +kubebuilder:validation:XValidation:rule="!has(self.restoreFrom) || (has(oldSelf.restoreFrom) && oldSelf.restoreFrom == self.restoreFrom)",message="restoreFrom can only be set when the ControlPlane is created"
type ControlPlaneSpec struct {
	// Version is the Kubernetes version, vMAJOR.MINOR.PATCH. The minor never
	// decreases; Brygga upgrades one minor at a time towards it.
	// +required
	// +kubebuilder:validation:MaxLength=16
	// +kubebuilder:validation:Pattern=`^v1\.[0-9]+\.[0-9]+$`
	// +kubebuilder:validation:XValidation:rule="int(self.split('.')[1]) >= int(oldSelf.split('.')[1])",message="the Kubernetes minor version cannot decrease"
	Version string `json:"version"`

	// Layout is Split, a Deployment per component, or Compact, every
	// component in one pod.
	// +optional
	// +kubebuilder:default=Split
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="layout is immutable"
	Layout Layout `json:"layout,omitempty"`

	// HighAvailability runs every component with more than one replica,
	// spread by FailureTolerance, and etcd with three members.
	// +optional
	HighAvailability *HighAvailability `json:"highAvailability,omitempty"`

	// Datastore is where the cluster's data lives.
	// +required
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="datastore is immutable in v1alpha1"
	Datastore DatastoreChoice `json:"datastore"`

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

	// Addons configures what Brygga runs inside the tenant cluster.
	// +optional
	Addons *Addons `json:"addons,omitempty"`

	// Placement says where this control plane's pods run.
	// +optional
	Placement *Placement `json:"placement,omitempty"`

	// Maintenance is when Brygga may roll changes it initiates.
	// +optional
	Maintenance *Maintenance `json:"maintenance,omitempty"`

	// Hibernation puts the control plane to sleep, keeping its data.
	// +optional
	Hibernation *Hibernation `json:"hibernation,omitempty"`

	// Health configures how Brygga protects the tenant's workloads.
	// +optional
	Health *Health `json:"health,omitempty"`

	// ServiceAccount configures the tokens the API server issues.
	// +optional
	ServiceAccount *ServiceAccount `json:"serviceAccount,omitempty"`

	// RestoreFrom starts the datastore from a Backup; set only at creation.
	// +optional
	RestoreFrom *RestoreFrom `json:"restoreFrom,omitempty"`

	// Paused stops reconciliation; everything keeps running.
	// +optional
	Paused bool `json:"paused,omitempty"`
}

// Layout is how the control plane's components are laid out in pods.
// +kubebuilder:validation:Enum=Split;Compact
type Layout string

const (
	// LayoutSplit runs each component in its own Deployment.
	LayoutSplit Layout = "Split"
	// LayoutCompact runs every component in one pod.
	LayoutCompact Layout = "Compact"
)

// HighAvailability is the failure a control plane survives.
type HighAvailability struct {
	// FailureTolerance is Node (replicas on different nodes) or Zone
	// (replicas in different zones).
	// +required
	FailureTolerance FailureTolerance `json:"failureTolerance"`
}

// FailureTolerance is the failure domain replicas are spread across.
// +kubebuilder:validation:Enum=Node;Zone
type FailureTolerance string

// DatastoreChoice is exactly one of Dedicated and Shared.
// +kubebuilder:validation:XValidation:rule="has(self.dedicated) != has(self.shared)",message="set exactly one of dedicated and shared"
type DatastoreChoice struct {
	// Dedicated is an etcd of the tenant's own.
	// +optional
	Dedicated *DedicatedDatastore `json:"dedicated,omitempty"`

	// Shared is a prefix in an etcd several tenants share.
	// +optional
	Shared *SharedDatastore `json:"shared,omitempty"`
}

// DedicatedDatastore is built from a DatastoreClass.
type DedicatedDatastore struct {
	// ClassName names a DatastoreClass; empty means the default class.
	// +optional
	// +kubebuilder:validation:MaxLength=253
	ClassName string `json:"className,omitempty"`
}

// SharedDatastore names a Datastore.
type SharedDatastore struct {
	// Name of the Datastore.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`
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

// ParentReference names a Gateway and, optionally, one of its listeners.
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

	// SectionName is the listener's name.
	// +optional
	// +kubebuilder:validation:MaxLength=253
	SectionName string `json:"sectionName,omitempty"`
}

// GatewayHostnames are the SNI names of one control plane.
type GatewayHostnames struct {
	// APIServer is the public API hostname.
	// +required
	APIServer Hostname `json:"apiServer"`

	// Konnectivity is where konnectivity agents connect.
	// +required
	Konnectivity Hostname `json:"konnectivity"`

	// Tunnel is the hostname apiserver-proxy dials for in-cluster access;
	// empty means no in-cluster proxy.
	// +optional
	Tunnel Hostname `json:"tunnel,omitempty"`
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
	// gateway mode; 172.20.0.1 by default.
	// +optional
	// +kubebuilder:validation:MaxLength=45
	// +kubebuilder:validation:XValidation:rule="isIP(self)",message="must be an IP address"
	AdvertiseAddress string `json:"advertiseAddress,omitempty"`

	// Authentication configures structured authentication; the anonymous
	// section is Brygga's.
	// +optional
	Authentication *Authentication `json:"authentication,omitempty"`

	// Audit configures audit logging.
	// +optional
	Audit *Audit `json:"audit,omitempty"`

	// Encryption configures encryption of Secrets at rest.
	// +optional
	Encryption *Encryption `json:"encryption,omitempty"`

	// Admission names a ConfigMap with an AdmissionConfiguration.
	// +optional
	Admission *Admission `json:"admission,omitempty"`

	// ExtraArgs are flags passed to kube-apiserver; the flags Brygga sets are refused.
	// +optional
	// +kubebuilder:validation:MaxProperties=64
	// +kubebuilder:validation:XValidation:rule="!self.exists(k, k in ['advertise-address', 'client-ca-file', 'egress-selector-config-file', 'etcd-cafile', 'etcd-certfile', 'etcd-keyfile', 'etcd-servers', 'secure-port', 'service-account-issuer', 'service-account-key-file', 'service-account-signing-endpoint', 'service-account-signing-key-file', 'service-cluster-ip-range', 'tls-cert-file', 'tls-private-key-file'])",message="Brygga sets this flag"
	ExtraArgs map[string]string `json:"extraArgs,omitempty"`

	// FeatureGates are passed to kube-apiserver.
	// +optional
	// +kubebuilder:validation:MaxProperties=64
	FeatureGates map[string]bool `json:"featureGates,omitempty"`

	// Resources override the operator's defaults.
	// +optional
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`
}

// Authentication is the part of AuthenticationConfiguration a tenant sets.
type Authentication struct {
	// JWT authenticators, such as OIDC providers.
	// +optional
	// +listType=atomic
	// +kubebuilder:validation:MaxItems=8
	JWT []JWTAuthenticator `json:"jwt,omitempty"`
}

// JWTAuthenticator accepts tokens from one issuer.
type JWTAuthenticator struct {
	// Issuer is the token issuer.
	// +required
	Issuer Issuer `json:"issuer"`

	// ClaimMappings say which claims become the user name and groups.
	// +optional
	ClaimMappings *ClaimMappings `json:"claimMappings,omitempty"`
}

// Issuer is a token issuer and the audiences it issues for.
type Issuer struct {
	// URL is the issuer's https URL.
	// +required
	// +kubebuilder:validation:MaxLength=512
	// +kubebuilder:validation:XValidation:rule="self.startsWith('https://')",message="must be an https URL"
	URL string `json:"url"`

	// Audiences accepted in the tokens.
	// +required
	// +listType=set
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=8
	// +kubebuilder:validation:items:MaxLength=253
	Audiences []string `json:"audiences"`
}

// ClaimMappings map claims to the user name and groups.
type ClaimMappings struct {
	// Username is the claim for the user name.
	// +optional
	Username *PrefixedClaim `json:"username,omitempty"`

	// Groups is the claim for the groups.
	// +optional
	Groups *PrefixedClaim `json:"groups,omitempty"`
}

// PrefixedClaim is a claim and a prefix added to its value.
type PrefixedClaim struct {
	// Claim is the JWT claim.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Claim string `json:"claim"`

	// Prefix is added to the claim's value.
	// +optional
	// +kubebuilder:validation:MaxLength=64
	Prefix string `json:"prefix,omitempty"`
}

// Audit names an audit policy.
type Audit struct {
	// PolicyRef names a ConfigMap with key policy.yaml.
	// +required
	PolicyRef LocalReference `json:"policyRef"`
}

// Encryption is how Secrets are encrypted in the datastore.
type Encryption struct {
	// Provider is None, secretbox, aescbc or kms.
	// +optional
	// +kubebuilder:default=secretbox
	Provider EncryptionProvider `json:"provider,omitempty"`
}

// EncryptionProvider is an encryption at rest provider.
// +kubebuilder:validation:Enum=None;secretbox;aescbc;kms
type EncryptionProvider string

// Admission names an admission configuration.
type Admission struct {
	// ConfigRef names a ConfigMap with an AdmissionConfiguration.
	// +required
	ConfigRef LocalReference `json:"configRef"`
}

// Component configures kube-controller-manager.
type Component struct {
	// ExtraArgs are flags passed to the component; the flags Brygga sets are refused.
	// +optional
	// +kubebuilder:validation:MaxProperties=64
	// +kubebuilder:validation:XValidation:rule="!self.exists(k, k in ['authentication-kubeconfig', 'authorization-kubeconfig', 'cluster-cidr', 'cluster-signing-cert-file', 'cluster-signing-key-file', 'kubeconfig', 'root-ca-file', 'service-account-private-key-file', 'service-cluster-ip-range'])",message="Brygga sets this flag"
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
	// ExtraArgs are flags passed to kube-scheduler; the flags Brygga sets are refused.
	// +optional
	// +kubebuilder:validation:MaxProperties=64
	// +kubebuilder:validation:XValidation:rule="!self.exists(k, k in ['authentication-kubeconfig', 'authorization-kubeconfig', 'config', 'kubeconfig'])",message="Brygga sets this flag"
	ExtraArgs map[string]string `json:"extraArgs,omitempty"`

	// FeatureGates are passed to kube-scheduler.
	// +optional
	// +kubebuilder:validation:MaxProperties=64
	FeatureGates map[string]bool `json:"featureGates,omitempty"`

	// Resources override the operator's defaults.
	// +optional
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`

	// ConfigRef names a ConfigMap with a KubeSchedulerConfiguration; Brygga
	// overrides its client connection.
	// +optional
	ConfigRef *LocalReference `json:"configRef,omitempty"`
}

// Addons are the components Brygga runs inside the tenant cluster.
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

// Maintenance is when Brygga may roll the changes it initiates.
type Maintenance struct {
	// Window is the daily maintenance window; without one, any time.
	// +optional
	Window *MaintenanceWindow `json:"window,omitempty"`

	// AutoUpdate opts into updates Brygga applies in the window.
	// +optional
	AutoUpdate *AutoUpdate `json:"autoUpdate,omitempty"`
}

// MaintenanceWindow is a daily window; End may be before Begin (it crosses midnight).
type MaintenanceWindow struct {
	// Begin is HH:MM.
	// +required
	// +kubebuilder:validation:Pattern=`^([01][0-9]|2[0-3]):[0-5][0-9]$`
	Begin string `json:"begin"`

	// End is HH:MM.
	// +required
	// +kubebuilder:validation:Pattern=`^([01][0-9]|2[0-3]):[0-5][0-9]$`
	End string `json:"end"`

	// TimeZone is an IANA time zone; UTC by default.
	// +optional
	// +kubebuilder:validation:MaxLength=64
	TimeZone string `json:"timeZone,omitempty"`
}

// AutoUpdate lists the updates Brygga applies on its own.
type AutoUpdate struct {
	// KubernetesPatch moves to the newest patch of the running minor.
	// +optional
	KubernetesPatch bool `json:"kubernetesPatch,omitempty"`
}

// Hibernation puts the control plane to sleep.
type Hibernation struct {
	// Enabled overrides the schedules; unset, the schedules decide.
	// +optional
	Enabled *bool `json:"enabled,omitempty"`

	// Schedules put the control plane to sleep at Start and wake it at End.
	// +optional
	// +listType=atomic
	// +kubebuilder:validation:MaxItems=8
	Schedules []HibernationSchedule `json:"schedules,omitempty"`
}

// HibernationSchedule is a pair of cron expressions.
// +kubebuilder:validation:XValidation:rule="has(self.start) || has(self.end)",message="set start, end or both"
type HibernationSchedule struct {
	// Start is a cron expression for falling asleep.
	// +optional
	// +kubebuilder:validation:MaxLength=64
	Start string `json:"start,omitempty"`

	// End is a cron expression for waking up.
	// +optional
	// +kubebuilder:validation:MaxLength=64
	End string `json:"end,omitempty"`

	// TimeZone is an IANA time zone; UTC by default.
	// +optional
	// +kubebuilder:validation:MaxLength=64
	TimeZone string `json:"timeZone,omitempty"`
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

	// PublicDiscovery lets unauthenticated clients read the discovery
	// documents, for federation.
	// +optional
	PublicDiscovery bool `json:"publicDiscovery,omitempty"`
}

// RestoreFrom names the Backup a new control plane starts from.
type RestoreFrom struct {
	// BackupRef names a Backup in the ControlPlane's namespace, never another
	// tenant's.
	// +required
	BackupRef LocalReference `json:"backupRef"`
}

// ControlPlaneStatus is what Brygga observed and did.
type ControlPlaneStatus struct {
	// ObservedGeneration is the newest generation every readiness condition
	// has been reported for.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions, one per controller, plus Progressing, Hibernated and Ready.
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

	// Endpoint is where the control plane is reachable.
	// +optional
	Endpoint *EndpointStatus `json:"endpoint,omitempty"`

	// Credentials are the Secrets clients need and the rotations.
	// +optional
	Credentials *CredentialsStatus `json:"credentials,omitempty"`

	// Datastore is the store in use and its last backup.
	// +optional
	Datastore *DatastoreStatus `json:"datastore,omitempty"`

	// Nodes summarise the tenant's nodes.
	// +optional
	Nodes *NodesStatus `json:"nodes,omitempty"`

	// Health is present while Brygga holds the controller manager.
	// +optional
	Health *HealthStatus `json:"health,omitempty"`
}

// Target is the state Lifecycle rolls towards now.
type Target struct {
	// Version is the Kubernetes version being rolled, one minor at a time.
	// +optional
	// +kubebuilder:validation:MaxLength=16
	Version string `json:"version,omitempty"`

	// OperatorVersion is the Brygga version this tenant's pods are rendered with.
	// +optional
	// +kubebuilder:validation:MaxLength=64
	OperatorVersion string `json:"operatorVersion,omitempty"`

	// Hibernated is the desired sleep state.
	// +optional
	Hibernated bool `json:"hibernated,omitempty"`

	// Reason says why the target lags the spec.
	// +optional
	// +kubebuilder:validation:MaxLength=64
	Reason string `json:"reason,omitempty"`

	// ScheduledFor is when a deferred change rolls.
	// +optional
	ScheduledFor *metav1.Time `json:"scheduledFor,omitempty"`
}

// KubernetesStatus is the running version and the upgrade in progress.
type KubernetesStatus struct {
	// Version every component runs.
	// +optional
	// +kubebuilder:validation:MaxLength=16
	Version string `json:"version,omitempty"`

	// Upgrade is present while an upgrade runs or is stuck.
	// +optional
	Upgrade *UpgradeStatus `json:"upgrade,omitempty"`
}

// UpgradeStatus is one upgrade's progress.
type UpgradeStatus struct {
	// +optional
	// +kubebuilder:validation:MaxLength=16
	From string `json:"from,omitempty"`
	// +optional
	// +kubebuilder:validation:MaxLength=16
	To string `json:"to,omitempty"`
	// Step is the step running or failed.
	// +optional
	// +kubebuilder:validation:MaxLength=64
	Step string `json:"step,omitempty"`
	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`
	// +optional
	// +kubebuilder:validation:MaxLength=1024
	Message string `json:"message,omitempty"`
}

// EndpointStatus is where the control plane is reachable from outside the
// management cluster. Workloads writes it whole once the public address
// exists; until then it is absent.
type EndpointStatus struct {
	// APIServer is the API server's public endpoint.
	// +required
	APIServer HostPort `json:"apiServer"`

	// Konnectivity is where konnectivity agents reach the konnectivity
	// server.
	// +required
	Konnectivity HostPort `json:"konnectivity"`
}

// HostPort is a host and a port.
type HostPort struct {
	// Host is a lowercase DNS name or an IP address, an IPv6 address without
	// brackets.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:XValidation:rule="isIP(self) || !format.dns1123Subdomain().validate(self).hasValue()",message="must be an IP address or a lowercase DNS name"
	Host string `json:"host"`

	// Port is a TCP port.
	// +required
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	Port int32 `json:"port"`
}

// CredentialsStatus names the Secrets clients need and tracks rotations.
type CredentialsStatus struct {
	// AdminKubeconfigSecretRef names the admin kubeconfig Secret.
	// +optional
	AdminKubeconfigSecretRef *LocalReference `json:"adminKubeconfigSecretRef,omitempty"`

	// CABundleSecretRef names the CAs a client must trust now.
	// +optional
	CABundleSecretRef *LocalReference `json:"caBundleSecretRef,omitempty"`

	// Rotation is the state of each credential's rotation.
	// +optional
	Rotation *Rotations `json:"rotation,omitempty"`
}

// Rotations is the rotation state per credential.
type Rotations struct {
	// +optional
	ClientCA *RotationStatus `json:"clientCA,omitempty"`
	// +optional
	CA *RotationStatus `json:"ca,omitempty"`
	// +optional
	ServiceAccountKey *RotationStatus `json:"serviceAccountKey,omitempty"`
	// +optional
	EtcdEncryptionKey *RotationStatus `json:"etcdEncryptionKey,omitempty"`
	// +optional
	EtcdCA *RotationStatus `json:"etcdCA,omitempty"`
	// +optional
	FrontProxyCA *RotationStatus `json:"frontProxyCA,omitempty"`
}

// RotationStatus is one credential's rotation.
type RotationStatus struct {
	// Phase of the rotation.
	// +optional
	Phase RotationPhase `json:"phase,omitempty"`
	// +optional
	LastInitiated *metav1.Time `json:"lastInitiated,omitempty"`
	// +optional
	LastCompleted *metav1.Time `json:"lastCompleted,omitempty"`
}

// RotationPhase is a stage of a credential rotation.
// +kubebuilder:validation:Enum=Idle;Preparing;Prepared;Completing
type RotationPhase string

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
	Members Count `json:"members"`
}

// DatastoreClassStatus is a DatastoreClass at the generation an etcd was
// built from.
type DatastoreClassStatus struct {
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`

	// +required
	// +kubebuilder:validation:Minimum=1
	Generation int64 `json:"generation"`
}

// Count is how many there are and how many of them are ready.
type Count struct {
	// +required
	// +kubebuilder:validation:Minimum=0
	Total int32 `json:"total"`

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
	// +optional
	SampledAt *metav1.Time `json:"sampledAt,omitempty"`
}

// HealthStatus reports the controller manager hold.
type HealthStatus struct {
	// ControllerManagerHold is present while the hold lasts.
	// +optional
	ControllerManagerHold *ControllerManagerHold `json:"controllerManagerHold,omitempty"`
}

// ControllerManagerHold is one hold of the controller manager's leader lease.
type ControllerManagerHold struct {
	// +optional
	Since *metav1.Time `json:"since,omitempty"`
	// +optional
	SilentNodes int32 `json:"silentNodes,omitempty"`
	// +optional
	CountedNodes int32 `json:"countedNodes,omitempty"`
	// BlockedBy names another holder of the lease Brygga left alone.
	// +optional
	// +kubebuilder:validation:MaxLength=253
	BlockedBy string `json:"blockedBy,omitempty"`
}
