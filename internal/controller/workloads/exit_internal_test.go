// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package workloads

import (
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

const (
	fatal      = `F1128 12:06:53.914442   32563 hooks.go:204] PostStartHook "scheduling/bootstrap-system-priority-classes" failed: unable to add default system priority classes: priorityclasses.scheduling.k8s.io "system-node-critical" is forbidden: not yet ready to handle request`
	fatalToo   = `F1128 12:06:53.915120   32563 hooks.go:204] PostStartHook "start-service-ip-repair-controllers" failed: unable to perform initial IP and Port allocation check (timeout)`
	afterFatal = `E1128 12:06:53.965438   32563 storage_rbac.go:232] "Unhandled Error" err="unable to reconcile clusterrole.rbac.authorization.k8s.io/cluster-admin: clusterroles.rbac.authorization.k8s.io \"cluster-admin\" is forbidden: not yet ready to handle request" logger="UnhandledError"`
)

// TestTheExitNamedIsTheNewestOfAPodNotReadyWithTheLineItExitedOnBounded picks the pod and the line a component's exit is named by.
func TestTheExitNamedIsTheNewestOfAPodNotReadyWithTheLineItExitedOnBounded(t *testing.T) {
	at := time.Date(2026, 11, 28, 10, 0, 0, 0, time.UTC)
	pod := func(name string, created time.Time, ready, deleting bool, message string) corev1.Pod {
		p := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, CreationTimestamp: metav1.NewTime(created)}}
		if deleting {
			p.DeletionTimestamp = new(metav1.NewTime(created))
		}
		readiness := corev1.ConditionFalse
		if ready {
			readiness = corev1.ConditionTrue
		}
		p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: readiness}}
		if message != "-" {
			p.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: apiServer, LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1, Reason: "Error", Message: message}}}}
		}
		return p
	}
	long := strings.Repeat("x", 2000)
	for _, c := range []struct {
		name string
		pods []corev1.Pod
		want string
	}{
		{"the newest of two", []corev1.Pod{pod("b", at.Add(time.Second), false, false, "Error: b"), pod("a", at, false, false, "Error: a")}, "pod b last exited 1 (Error): Error: b"},
		{"by name within a second", []corev1.Pod{pod("a", at, false, false, "Error: a"), pod("b", at, false, false, "Error: b")}, "pod b last exited 1 (Error): Error: b"},
		{"a ready one left out", []corev1.Pod{pod("b", at.Add(time.Second), true, false, "Error: b"), pod("a", at, false, false, "Error: a")}, "pod a last exited 1 (Error): Error: a"},
		{"one being deleted left out", []corev1.Pod{pod("b", at.Add(time.Second), false, true, "Error: b"), pod("a", at, false, false, "Error: a")}, "pod a last exited 1 (Error): Error: a"},
		{"one that never exited left out", []corev1.Pod{pod("b", at.Add(time.Second), false, false, "-"), pod("a", at, false, false, "Error: a")}, "pod a last exited 1 (Error): Error: a"},
		{"the last line after the usage", []corev1.Pod{pod("a", at, false, false, "Usage:\n  kube-apiserver [flags]\n\nError: invalid argument \"yes\" for \"--profiling\" flag: strconv.ParseBool: parsing \"yes\": invalid syntax\n\n")}, `pod a last exited 1 (Error): Error: invalid argument "yes" for "--profiling" flag: strconv.ParseBool: parsing "yes": invalid syntax`},
		{"the fatal line before another goroutine's error", []corev1.Pod{pod("a", at, false, false, fatal+"\n"+afterFatal+"\n")}, "pod a last exited 1 (Error): " + fatal},
		{"the first of two fatal lines", []corev1.Pod{pod("a", at, false, false, fatal+"\n"+fatalToo+"\n")}, "pod a last exited 1 (Error): " + fatal},
		{"the last line after one that only starts as a fatal line does", []corev1.Pod{pod("a", at, false, false, "F1128 12:06:53 is no klog header\nError: a\n")}, "pod a last exited 1 (Error): Error: a"},
		{"no message", []corev1.Pod{pod("a", at, false, false, "")}, "pod a last exited 1 (Error)"},
		{"a long line cut", []corev1.Pod{pod("a", at, false, false, long)}, "pod a last exited 1 (Error): " + long[:maxQuoted] + "…"},
		{"a long line cut before the rune that crosses the bound", []corev1.Pod{pod("a", at, false, false, long[:maxQuoted-1]+"é"+long)}, "pod a last exited 1 (Error): " + long[:maxQuoted-1] + "…"},
		{"none", nil, ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := newestExit(c.pods, apiServer); got != c.want {
				t.Errorf("newestExit is %q, want %q", got, c.want)
			}
		})
	}
}

// TestAHeldControllerManagerWithNoReadyReplicaStillNamesItsNewReplicasExit names the exit of a new controller manager replica while Health holds the leader Lease.
func TestAHeldControllerManagerWithNoReadyReplicaStillNamesItsNewReplicasExit(t *testing.T) {
	var deployments []*appsv1.Deployment
	for _, component := range components {
		d := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{lodiav1alpha1.ComponentLabel: component}}, Spec: appsv1.DeploymentSpec{Replicas: new(int32(1))}}
		d.Status = appsv1.DeploymentStatus{Replicas: 1, UpdatedReplicas: 1, ReadyReplicas: 1, AvailableReplicas: 1}
		if component == controllerManager {
			d.Status = appsv1.DeploymentStatus{Replicas: 2, UpdatedReplicas: 1}
		}
		deployments = append(deployments, d)
	}
	exit := "pod b last exited 1 (Error): Error: unknown flag: --concurrent-service-syncs"
	c := availability(deployments, "", true, map[string]string{controllerManager: exit})
	if c.Status != metav1.ConditionTrue || !strings.Contains(c.Message, "while Health holds its leader Lease; "+controllerManager+"'s "+exit) {
		t.Errorf("APIServerReady is %+v, want the held controller manager's exit named", c)
	}
}

// TestTheRehearsalQuotesTheLineAnExitEndedOn quotes the first fatal line of a container that leaves its logs as its termination message, else the line it printed last.
func TestTheRehearsalQuotesTheLineAnExitEndedOn(t *testing.T) {
	gate := `E1128 07:25:08.365324       1 run.go:72] "command failed" err="unrecognized feature gate: APIServerTracing"`
	startUp := strings.Repeat("W1128 07:25:08.100000       1 options.go:249] a warning the start-up prints\n", 15)
	for _, c := range []struct{ name, message, want string }{
		{"a line alone", gate, "1 (Error): " + gate},
		{"the error after the start-up", startUp + gate + "\n", "1 (Error): " + gate},
		{"the fatal line before another goroutine's error", startUp + fatal + "\n" + afterFatal + "\n", "1 (Error): " + fatal},
		{"the first of two fatal lines", startUp + fatal + "\n" + fatalToo + "\n", "1 (Error): " + fatal},
		{"no message", "", "1 (Error)"},
	} {
		t.Run(c.name, func(t *testing.T) {
			status := &corev1.ContainerStatus{LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1, Reason: "Error", Message: c.message}}}
			if got := exited(status); got != c.want {
				t.Errorf("exited is %q, want %q", got, c.want)
			}
		})
	}
}

// TestAStuckRollComesBackAfter15sDoublingToFiveMinutes waits 15s after a rollout last progressed, then twice as long each time up to five minutes, and five minutes past its progress deadline.
func TestAStuckRollComesBackAfter15sDoublingToFiveMinutes(t *testing.T) {
	now := time.Date(2026, 11, 28, 12, 0, 0, 0, time.UTC)
	stuck := func(conditions ...appsv1.DeploymentCondition) *appsv1.Deployment {
		return &appsv1.Deployment{Spec: appsv1.DeploymentSpec{Replicas: new(int32(1))}, Status: appsv1.DeploymentStatus{Replicas: 2, UpdatedReplicas: 1, ReadyReplicas: 1, AvailableReplicas: 1, Conditions: conditions}}
	}
	progressed := func(ago time.Duration) appsv1.DeploymentCondition {
		return appsv1.DeploymentCondition{Type: appsv1.DeploymentProgressing, Status: corev1.ConditionTrue, Reason: "ReplicaSetUpdated", LastUpdateTime: metav1.NewTime(now.Add(-ago))}
	}
	available := &appsv1.Deployment{Spec: appsv1.DeploymentSpec{Replicas: new(int32(1))}, Status: appsv1.DeploymentStatus{Replicas: 1, UpdatedReplicas: 1, ReadyReplicas: 1, AvailableReplicas: 1, Conditions: []appsv1.DeploymentCondition{progressed(time.Hour)}}}
	timedOut := appsv1.DeploymentCondition{Type: appsv1.DeploymentProgressing, Status: corev1.ConditionFalse, Reason: timedOutReason, LastUpdateTime: metav1.NewTime(now)}
	for _, c := range []struct {
		name        string
		deployments []*appsv1.Deployment
		want        time.Duration
	}{
		{"at rest", []*appsv1.Deployment{available}, 0},
		{"without a Progressing condition", []*appsv1.Deployment{stuck()}, 15 * time.Second},
		{"as it progresses", []*appsv1.Deployment{stuck(progressed(0))}, 15 * time.Second},
		{"after the first wait", []*appsv1.Deployment{stuck(progressed(15 * time.Second))}, 30 * time.Second},
		{"after the second", []*appsv1.Deployment{stuck(progressed(45 * time.Second))}, time.Minute},
		{"after the third", []*appsv1.Deployment{stuck(progressed(105 * time.Second))}, 2 * time.Minute},
		{"after the fourth", []*appsv1.Deployment{stuck(progressed(225 * time.Second))}, 4 * time.Minute},
		{"after the fifth", []*appsv1.Deployment{stuck(progressed(465 * time.Second))}, 5 * time.Minute},
		{"an hour on", []*appsv1.Deployment{stuck(progressed(time.Hour))}, 5 * time.Minute},
		{"progressed after now", []*appsv1.Deployment{stuck(progressed(-time.Minute))}, 15 * time.Second},
		{"past its progress deadline", []*appsv1.Deployment{stuck(timedOut)}, 5 * time.Minute},
		{"the sooner of two", []*appsv1.Deployment{stuck(progressed(225 * time.Second)), stuck(progressed(15 * time.Second))}, 30 * time.Second},
		{"beside one at rest", []*appsv1.Deployment{available, stuck(progressed(45 * time.Second))}, time.Minute},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := exitsAfter(c.deployments, now); got != c.want {
				t.Errorf("exitsAfter is %s, want %s", got, c.want)
			}
		})
	}
}

// TestTheComponentsLeaveTheirLogsAsTheirTerminationMessage renders FallbackToLogsOnError on every component's own container.
func TestTheComponentsLeaveTheirLogsAsTheirTerminationMessage(t *testing.T) {
	cp := &lodiav1alpha1.ControlPlane{
		ObjectMeta: metav1.ObjectMeta{Name: "dev", Namespace: "team"},
		Spec:       lodiav1alpha1.ControlPlaneSpec{Network: lodiav1alpha1.Network{ServiceCIDRs: []lodiav1alpha1.CIDR{"10.96.0.0/16"}, PodCIDRs: []lodiav1alpha1.CIDR{"10.244.0.0/16"}}},
	}
	r := rendering{cp: cp, endpoint: &lodiav1alpha1.EndpointStatus{}, etcd: []lodiav1alpha1.HostPort{{Host: "dev-etcd-client.team.svc", Port: 2379}}}
	for _, d := range r.deployments() {
		if c := d.Spec.Template.Spec.Containers[0]; c.TerminationMessagePolicy != corev1.TerminationMessageFallbackToLogsOnError {
			t.Errorf("%s's termination message policy is %q", c.Name, c.TerminationMessagePolicy)
		}
	}
}
