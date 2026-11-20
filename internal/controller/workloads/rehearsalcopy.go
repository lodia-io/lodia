// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package workloads

import (
	"encoding/csv"
	"maps"
	"net"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/pflag"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/contract"
	"go.lodia.io/lodia/internal/versions"
)

const (
	// copyDeadline is how long after its creation a copy ends with a verdict,
	// and when its containers end if the operator is gone.
	copyDeadline = 15 * time.Minute
	copyGrace    = 5 * time.Second
	copyDir      = "/var/rehearsal"
	copyEtcd     = "http://127.0.0.1:2379"
	copyPeer     = "http://127.0.0.1:2380"

	versionAnnotation    = "lodia.io/rehearsal-version"
	generationAnnotation = "lodia.io/rehearsal-generation"
)

// webhookPlugins are the admission plugins the copy runs without: it reaches
// no webhook, and each call would wait the webhook's whole timeout.
var webhookPlugins = []string{"MutatingAdmissionWebhook", "ValidatingAdmissionWebhook"}

func copyName(cp string) string { return cp + "-rehearsal" }

// rehearsalCopy is the copy of the API server that rehearses cp's minor step
// at r.images, for generation: the API server's render with only the changes
// docs/decisions/rehearse-a-minor-step.md lists, on an etcd of its own that an
// init container restores from a fresh snapshot of the tenant's, with quota
// and resources the DatastoreClass's.
func (r rendering) rehearsalCopy(version string, generation int64, quota resource.Quantity, etcdResources *corev1.ResourceRequirements) *corev1.Pod {
	cp := r.cp
	pod := r.apiServer().Spec.Template.Spec
	api := pod.Containers[0]
	api.Command = copyCommand(api.Command)
	api.VolumeMounts = slices.DeleteFunc(api.VolumeMounts, func(m corev1.VolumeMount) bool { return m.Name == "etcd-ca" || m.Name == "etcd-client" })
	api.ReadinessProbe = readinessProbe(contract.APIServerReadyzPath, intstr.FromString("https"))
	api.LivenessProbe, api.StartupProbe, api.Lifecycle = nil, nil, nil
	api.TerminationMessagePolicy = corev1.TerminationMessageFallbackToLogsOnError
	pod.Containers = []corev1.Container{api}

	signer := pod.InitContainers[slices.IndexFunc(pod.InitContainers, func(c corev1.Container) bool { return c.Name == "lodia-signer" })]
	pod.InitContainers = []corev1.Container{copySnapshot(r.etcd[0]), copyRestore(), copyEtcdServer(quota, etcdResources), signer}
	dropped := []string{"konnectivity-server", "konnectivity-kubeconfig", "tunnel-serving", "tunnel-ca"}
	pod.Volumes = slices.DeleteFunc(pod.Volumes, func(v corev1.Volume) bool { return slices.Contains(dropped, v.Name) })
	size := quota.DeepCopy()
	size.Add(quota)
	pod.Volumes = append(pod.Volumes, corev1.Volume{Name: "rehearsal", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: &size}}})
	pod.TopologySpreadConstraints = nil
	pod.RestartPolicy = corev1.RestartPolicyOnFailure
	pod.ActiveDeadlineSeconds = new(int64(copyDeadline / time.Second))
	pod.TerminationGracePeriodSeconds = new(int64(copyGrace / time.Second))

	meta := objectMeta(cp, "")
	meta.GenerateName = copyName(cp.Name) + "-"
	meta.Labels = podLabels(cp.Name, lodiav1alpha1.RehearsalComponent)
	meta.Annotations = map[string]string{versionAnnotation: version, generationAnnotation: strconv.FormatInt(generation, 10)}
	return &corev1.Pod{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"}, ObjectMeta: meta, Spec: pod}
}

// copyCommand is the API server's command on the copy's own etcd, with the
// webhook plugins disabled and no compaction, so that a sweep's continue
// tokens hold across its passes.
func copyCommand(command []string) []string {
	flags := map[string]string{}
	for _, arg := range command[1:] {
		name, value, _ := strings.Cut(strings.TrimPrefix(arg, "--"), "=")
		flags[name] = value
	}
	for _, name := range []string{"etcd-cafile", "etcd-certfile", "etcd-keyfile"} {
		delete(flags, name)
	}
	flags["etcd-servers"] = copyEtcd
	flags["etcd-compaction-interval"] = "0"
	if enabled, err := plugins(flags["enable-admission-plugins"]); err == nil {
		flags["enable-admission-plugins"] = pluginList(slices.DeleteFunc(enabled, func(p string) bool { return slices.Contains(webhookPlugins, p) }))
	}
	if disabled, err := plugins(flags["disable-admission-plugins"]); err == nil {
		for _, p := range webhookPlugins {
			if !slices.Contains(disabled, p) {
				disabled = append(disabled, p)
			}
		}
		flags["disable-admission-plugins"] = pluginList(disabled)
	}
	out := []string{command[0]}
	for _, name := range slices.Sorted(maps.Keys(flags)) {
		out = append(out, "--"+name+"="+flags[name])
	}
	return out
}

// plugins reads a list of admission plugins as kube-apiserver reads its flags,
// with pflag's StringSlice: a CSV record, whose quotes are not part of a name
// (https://github.com/kubernetes/apiserver/blob/v0.37.1/pkg/server/options/admission.go#L108-L116). A list
// that does not parse fails the tenant's API server as it fails the copy's.
func plugins(value string) ([]string, error) {
	var list []string
	flags := pflag.NewFlagSet("admission", pflag.ContinueOnError)
	flags.StringSliceVar(&list, "plugins", nil, "")
	err := flags.Set("plugins", value)
	return list, err
}

func pluginList(plugins []string) string {
	var b strings.Builder
	w := csv.NewWriter(&b)
	_ = w.Write(plugins)
	w.Flush()
	return strings.TrimSuffix(b.String(), "\n")
}

func copySnapshot(etcd lodiav1alpha1.HostPort) corev1.Container {
	return corev1.Container{
		Name:  "snapshot",
		Image: versions.EtcdImage,
		Command: []string{
			"etcdctl",
			"--endpoints=https://" + net.JoinHostPort(etcd.Host, strconv.Itoa(int(etcd.Port))),
			"--cacert=/etc/lodia/etcd-ca/" + lodiav1alpha1.CACertKey,
			"--cert=/etc/lodia/etcd-client/" + lodiav1alpha1.TLSCertKey,
			"--key=/etc/lodia/etcd-client/" + lodiav1alpha1.TLSPrivateKeyKey,
			"--dial-timeout=10s",
			"--command-timeout=5m",
			"snapshot", "save", copyDir + "/snapshot.db",
		},
		TerminationMessagePolicy: corev1.TerminationMessageFallbackToLogsOnError,
		VolumeMounts:             append(mounts("etcd-ca", "etcd-client"), corev1.VolumeMount{Name: "rehearsal", MountPath: copyDir}),
	}
}

func copyRestore() corev1.Container {
	return corev1.Container{
		Name:  "restore",
		Image: versions.EtcdImage,
		Command: []string{
			"etcdutl", "snapshot", "restore", copyDir + "/snapshot.db",
			"--data-dir=" + copyDir + "/data",
			"--name=rehearsal",
			"--initial-cluster=rehearsal=" + copyPeer,
			"--initial-advertise-peer-urls=" + copyPeer,
		},
		TerminationMessagePolicy: corev1.TerminationMessageFallbackToLogsOnError,
		VolumeMounts:             []corev1.VolumeMount{{Name: "rehearsal", MountPath: copyDir}},
	}
}

func copyEtcdServer(quota resource.Quantity, resources *corev1.ResourceRequirements) corev1.Container {
	health := corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"etcdctl", "--endpoints=" + copyEtcd, "endpoint", "health"}}}
	c := corev1.Container{
		Name:          "etcd",
		Image:         versions.EtcdImage,
		RestartPolicy: new(corev1.ContainerRestartPolicyAlways),
		Command: []string{
			"etcd",
			"--data-dir=" + copyDir + "/data",
			"--listen-client-urls=" + copyEtcd,
			"--advertise-client-urls=" + copyEtcd,
			"--listen-peer-urls=" + copyPeer,
			"--listen-metrics-urls=http://127.0.0.1:2381",
			"--name=rehearsal",
			"--initial-cluster=rehearsal=" + copyPeer,
			"--initial-advertise-peer-urls=" + copyPeer,
			"--quota-backend-bytes=" + strconv.FormatInt(quota.Value(), 10),
		},
		StartupProbe:             &corev1.Probe{ProbeHandler: health, PeriodSeconds: 1, FailureThreshold: 300},
		ReadinessProbe:           &corev1.Probe{ProbeHandler: *health.DeepCopy(), PeriodSeconds: 5},
		TerminationMessagePolicy: corev1.TerminationMessageFallbackToLogsOnError,
		VolumeMounts:             []corev1.VolumeMount{{Name: "rehearsal", MountPath: copyDir}},
	}
	if resources != nil {
		c.Resources = *resources
	}
	return c
}

func copyPolicy(cp *lodiav1alpha1.ControlPlane, operatorNamespace string) *networkingv1.NetworkPolicy {
	return &networkingv1.NetworkPolicy{
		TypeMeta:   metav1.TypeMeta{APIVersion: "networking.k8s.io/v1", Kind: "NetworkPolicy"},
		ObjectMeta: objectMeta(cp, copyName(cp.Name)),
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{MatchLabels: podLabels(cp.Name, lodiav1alpha1.RehearsalComponent)},
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress},
			Ingress: []networkingv1.NetworkPolicyIngressRule{{
				From:  []networkingv1.NetworkPolicyPeer{{NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{corev1.LabelMetadataName: operatorNamespace}}}},
				Ports: []networkingv1.NetworkPolicyPort{{Protocol: new(corev1.ProtocolTCP), Port: new(intstr.FromInt32(kubernetesPort))}},
			}},
		},
	}
}
