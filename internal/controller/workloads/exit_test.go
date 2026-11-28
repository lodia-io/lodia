// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package workloads_test

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/controller/workloads"
)

// TestANewReplicasExitIsNamedInAPass names, in one pass, the newest pod of a stuck rollout's current template that is neither ready nor Failed, with the last line of its termination message.
func TestANewReplicasExitIsNamedInAPass(t *testing.T) {
	cp := running(t)
	component := "kube-apiserver"
	d := deployment(t, cp, component)
	foreign := d.DeepCopy()
	foreign.UID = "another-owner"
	replicaSet(t, foreign, "a-foreign", d.Spec.Template)
	current, older := stuck(t, cp, component)
	usage := strings.Repeat("      --some-flag string   what it does\n", 40)
	componentPod(t, cp, component, "a-old", current, true, nil)
	componentPod(t, cp, component, "b-earlier", current, false, exitedOn(usage+"Error: unknown flag: --earlier\n"))
	componentPod(t, cp, component, "c-new", current, false, exitedOn(usage+"Error: unknown flag: --concurrent-service-syncs\n\n"))
	componentPod(t, cp, component, "d-older", older, false, exitedOn("Error: unknown flag: --older\n"))
	evicted(t, componentPod(t, cp, component, "e-evicted", current, false, exitedOn("Error: unknown flag: --evicted\n")))
	componentPod(t, cp, component, "f-foreign", "a-foreign", false, exitedOn("Error: unknown flag: --foreign\n"))
	result := reconcileThrough(t, cp, k8s)
	want := "kube-apiserver's pod " + cp.Name + "-kube-apiserver-c-new last exited 1 (Error): Error: unknown flag: --concurrent-service-syncs"
	if c := condition(t, cp); c == nil || !strings.Contains(c.Message, want) || strings.Contains(c.Message, "--earlier") || strings.Contains(c.Message, "--some-flag") ||
		strings.Contains(c.Message, "--older") || strings.Contains(c.Message, "--evicted") || strings.Contains(c.Message, "--foreign") {
		t.Fatalf("APIServerReady is %+v, want its message to hold %q", c, want)
	}
	if result.RequeueAfter <= 0 || result.RequeueAfter > 15*time.Second {
		t.Errorf("a pass over a stuck rollout comes back after %s, want within 15s", result.RequeueAfter)
	}
}

// TestTheReadOfAComponentsPodsIsBoundedAfterItsSelection reads at most 16 pods of a component's current template, counted once the Failed ones and those of older templates are left out.
func TestTheReadOfAComponentsPodsIsBoundedAfterItsSelection(t *testing.T) {
	cp := running(t)
	component := "kube-apiserver"
	current, older := stuck(t, cp, component)
	for i := range 20 {
		evicted(t, componentPod(t, cp, component, fmt.Sprintf("a-failed-%02d", i), current, false, exitedOn("Error: a failed one\n")))
		componentPod(t, cp, component, fmt.Sprintf("b-older-%02d", i), older, false, exitedOn("Error: an older one\n"))
	}
	for i := range 17 {
		componentPod(t, cp, component, fmt.Sprintf("c-%02d", i), current, false, exitedOn(fmt.Sprintf("Error: the current one %02d\n", i)))
	}
	reconcileThrough(t, cp, k8s)
	want := "kube-apiserver's pod " + cp.Name + "-kube-apiserver-c-15 last exited 1 (Error): Error: the current one 15"
	if c := condition(t, cp); c == nil || !strings.Contains(c.Message, want) {
		t.Fatalf("APIServerReady is %+v, want its message to hold %q", c, want)
	}
}

// TestAFailedReadOfPodsWritesTheConditionWithoutTheExit writes the count a pass makes when its read of a new replica's pods fails, and comes back within 15s.
func TestAFailedReadOfPodsWritesTheConditionWithoutTheExit(t *testing.T) {
	cp := running(t)
	component := "kube-apiserver"
	current, _ := stuck(t, cp, component)
	componentPod(t, cp, component, "a-new", current, false, exitedOn("Error: unknown flag: --misspelt\n"))
	reconcileThrough(t, cp, k8s)
	if c := condition(t, cp); c == nil || !strings.Contains(c.Message, "--misspelt") {
		t.Fatalf("APIServerReady is %+v, want the exit named", c)
	}
	reader := interceptor.NewClient(k8s, interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			switch list.(type) {
			case *appsv1.ReplicaSetList, *corev1.PodList:
				return errors.New("the management API server is unavailable")
			}
			return c.List(ctx, list, opts...)
		},
	})
	reconciler := &workloads.Reconciler{Client: k8s, Reader: reader, SignerImage: signerImage, TunnelImage: tunnelImage, GatewayAPI: true, Revision: revision}
	result, err := reconciler.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)})
	if err != nil {
		t.Fatalf("a failed read of pods fails the pass: %v", err)
	}
	if c := condition(t, cp); c == nil || c.Reason != lodiav1alpha1.ReasonReady || strings.Contains(c.Message, "last exited") {
		t.Fatalf("APIServerReady is %+v, want the count without the exit", c)
	}
	if result.RequeueAfter <= 0 || result.RequeueAfter > 15*time.Second {
		t.Errorf("the pass comes back after %s, want within 15s", result.RequeueAfter)
	}
}

// TestAHeldOrWaitingPassNamesANewReplicasExitAndComesBackWithin15s names the exit in the count a held pass and a wait make, and brings the next pass within 15s.
func TestAHeldOrWaitingPassNamesANewReplicasExitAndComesBackWithin15s(t *testing.T) {
	for _, c := range []struct {
		name string
		pass func(*testing.T, *lodiav1alpha1.ControlPlane) ctrl.Result
	}{
		{"held", func(t *testing.T, cp *lodiav1alpha1.ControlPlane) ctrl.Result {
			result, err := operatorAt(k8s, next).Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)})
			if err != nil {
				t.Fatal(err)
			}
			return result
		}},
		{"waiting for certificates", func(t *testing.T, cp *lodiav1alpha1.ControlPlane) ctrl.Result {
			setFalse(t, cp, lodiav1alpha1.ConditionCertificatesReady, "Renewing")
			return reconcileThrough(t, cp, k8s)
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			cp := running(t)
			component := "kube-controller-manager"
			current, _ := stuck(t, cp, component)
			componentPod(t, cp, component, "a-new", current, false, exitedOn("Error: unknown flag: --concurrent-service-syncs\n"))
			result := c.pass(t, cp)
			if got := condition(t, cp); got == nil || !strings.Contains(got.Message, "kube-controller-manager's pod "+cp.Name+"-kube-controller-manager-a-new last exited 1 (Error)") {
				t.Fatalf("APIServerReady is %+v, want the exit named", got)
			}
			if result.RequeueAfter <= 0 || result.RequeueAfter > 15*time.Second {
				t.Errorf("the pass comes back after %s, want within 15s", result.RequeueAfter)
			}
		})
	}
}

// TestTheExitIsReadFromTheOldestReplicaSetOfTheCurrentTemplate names the exit of a pod of the older of two ReplicaSets of the current template, which the Deployment controller keeps, though the younger sorts first by name.
func TestTheExitIsReadFromTheOldestReplicaSetOfTheCurrentTemplate(t *testing.T) {
	cp := running(t)
	component := "kube-apiserver"
	d := deployment(t, cp, component)
	older := replicaSet(t, d, "z-older", d.Spec.Template)
	time.Sleep(time.Until(older.CreationTimestamp.Add(time.Second)))
	replicaSet(t, d, "a-younger", d.Spec.Template)
	setDeploymentStatus(t, cp, component, func(s *appsv1.DeploymentStatus) {
		s.Replicas, s.UpdatedReplicas, s.ReadyReplicas, s.AvailableReplicas = 2, 1, 1, 1
	})
	componentPod(t, cp, component, "a-of-the-older", "z-older", false, exitedOn("Error: the older ReplicaSet\n"))
	componentPod(t, cp, component, "b-of-the-younger", "a-younger", false, exitedOn("Error: the younger ReplicaSet\n"))
	reconcileThrough(t, cp, k8s)
	want := "kube-apiserver's pod " + cp.Name + "-kube-apiserver-a-of-the-older last exited 1 (Error): Error: the older ReplicaSet"
	if c := condition(t, cp); c == nil || !strings.Contains(c.Message, want) {
		t.Fatalf("APIServerReady is %+v, want its message to hold %q", c, want)
	}
}

// TestAStuckRollInNodePortModeComesBackSoonerThanTheModesMinute brings a nodePort tenant's stuck rollout back 15s after it progressed, where the mode's own pass would come after a minute.
func TestAStuckRollInNodePortModeComesBackSoonerThanTheModesMinute(t *testing.T) {
	cp := runningOnNodePort(t)
	progressed(t, cp, "kube-scheduler", 0)
	if result := reconcileThrough(t, cp, k8s); result.RequeueAfter < 15*time.Second || result.RequeueAfter > 17*time.Second {
		t.Errorf("the pass comes back after %s, want 15s after the rollout last progressed", result.RequeueAfter)
	}
}

// TestAStuckRollComesBackLaterTheLongerItHasNotProgressed brings a stuck rollout back 15s after it progressed, and after 2 minutes once it last progressed 105s ago.
func TestAStuckRollComesBackLaterTheLongerItHasNotProgressed(t *testing.T) {
	cp := running(t)
	for ago, want := range map[time.Duration]time.Duration{0: 15 * time.Second, 105 * time.Second: 2 * time.Minute} {
		progressed(t, cp, "kube-scheduler", ago)
		if result := reconcileThrough(t, cp, k8s); result.RequeueAfter < want || result.RequeueAfter > want+2*time.Second {
			t.Errorf("a pass %s after the rollout last progressed comes back after %s, want %s", ago, result.RequeueAfter, want)
		}
	}
}

// TestAPassAtRestReadsNoReplicaSetOrPod reads no ReplicaSet and no pod while every replica of every component runs the current template and is available.
func TestAPassAtRestReadsNoReplicaSetOrPod(t *testing.T) {
	cp := running(t)
	reader := interceptor.NewClient(k8s, interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			switch list.(type) {
			case *appsv1.ReplicaSetList, *corev1.PodList:
				t.Errorf("a pass at rest listed %T", list)
			}
			return c.List(ctx, list, opts...)
		},
	})
	reconciler := &workloads.Reconciler{Client: k8s, Reader: reader, SignerImage: signerImage, TunnelImage: tunnelImage, GatewayAPI: true, Revision: revision}
	result, err := reconciler.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)})
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter != 0 {
		t.Errorf("a pass at rest comes back after %s", result.RequeueAfter)
	}
}

// stuck leaves a new replica of the component unavailable beside the ReplicaSets of its template and an older one, and returns their hashes.
func stuck(t *testing.T, cp *lodiav1alpha1.ControlPlane, component string) (string, string) {
	t.Helper()
	d := deployment(t, cp, component)
	older := d.Spec.Template.DeepCopy()
	older.Annotations = map[string]string{"older": "true"}
	replicaSet(t, d, "b-older", *older)
	replicaSet(t, d, "current", d.Spec.Template)
	setDeploymentStatus(t, cp, component, func(s *appsv1.DeploymentStatus) {
		s.Replicas, s.UpdatedReplicas, s.ReadyReplicas, s.AvailableReplicas = 2, 1, 1, 1
	})
	return "current", "b-older"
}

func replicaSet(t *testing.T, d *appsv1.Deployment, hash string, template corev1.PodTemplateSpec) *appsv1.ReplicaSet {
	t.Helper()
	labels := maps.Clone(template.Labels)
	labels[appsv1.DefaultDeploymentUniqueLabelKey] = hash
	template.Labels = labels
	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: d.Name + "-" + hash, Namespace: d.Namespace, Labels: labels,
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(d, appsv1.SchemeGroupVersion.WithKind("Deployment"))},
		},
		Spec: appsv1.ReplicaSetSpec{Replicas: new(int32(1)), Selector: &metav1.LabelSelector{MatchLabels: labels}, Template: template},
	}
	if err := k8s.Create(t.Context(), rs); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = k8s.Delete(context.Background(), rs) })
	return rs
}

// progressed leaves a new replica of the component unavailable, its rollout having last progressed ago.
func progressed(t *testing.T, cp *lodiav1alpha1.ControlPlane, component string, ago time.Duration) {
	t.Helper()
	setDeploymentStatus(t, cp, component, func(s *appsv1.DeploymentStatus) {
		s.Replicas, s.UpdatedReplicas, s.ReadyReplicas, s.AvailableReplicas = 2, 1, 1, 1
		s.Conditions = []appsv1.DeploymentCondition{{Type: appsv1.DeploymentProgressing, Status: corev1.ConditionTrue, Reason: "ReplicaSetUpdated", LastUpdateTime: metav1.NewTime(time.Now().Add(-ago))}}
	})
}

func exitedOn(message string) *corev1.ContainerStateTerminated {
	return &corev1.ContainerStateTerminated{ExitCode: 1, Reason: "Error", Message: message}
}

func evicted(t *testing.T, pod *corev1.Pod) {
	t.Helper()
	pod.Status.Phase, pod.Status.Reason = corev1.PodFailed, "Evicted"
	if err := k8s.Status().Update(t.Context(), pod); err != nil {
		t.Fatal(err)
	}
}

func componentPod(t *testing.T, cp *lodiav1alpha1.ControlPlane, component, suffix, hash string, ready bool, ended *corev1.ContainerStateTerminated) *corev1.Pod {
	t.Helper()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: cp.Name + "-" + component + "-" + suffix, Namespace: cp.Namespace,
			Labels: map[string]string{lodiav1alpha1.ControlPlaneLabel: cp.Name, lodiav1alpha1.ComponentLabel: component, appsv1.DefaultDeploymentUniqueLabelKey: hash},
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: component, Image: "registry.k8s.io/" + component + ":" + version}}},
	}
	if err := k8s.Create(t.Context(), pod); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = k8s.Delete(context.Background(), pod) })
	readiness := corev1.ConditionFalse
	status := corev1.ContainerStatus{Name: component, Image: pod.Spec.Containers[0].Image, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}, Ready: ready}
	if ready {
		readiness = corev1.ConditionTrue
	}
	if ended != nil {
		status.State = corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}}
		status.LastTerminationState = corev1.ContainerState{Terminated: ended}
	}
	pod.Status = corev1.PodStatus{
		Phase:             corev1.PodRunning,
		Conditions:        []corev1.PodCondition{{Type: corev1.PodReady, Status: readiness}},
		ContainerStatuses: []corev1.ContainerStatus{status},
	}
	if err := k8s.Status().Update(t.Context(), pod); err != nil {
		t.Fatal(err)
	}
	return pod
}
