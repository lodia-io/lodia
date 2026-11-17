// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package fleet

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"go.lodia.io/lodia/internal/operator"
)

type memory struct {
	RSS, HeapInuse, HeapLive, Stacks uint64
	Goroutines                       int
}

func (m *memory) max(o memory) {
	m.RSS, m.HeapInuse, m.HeapLive, m.Stacks = max(m.RSS, o.RSS), max(m.HeapInuse, o.HeapInuse), max(m.HeapLive, o.HeapLive), max(m.Stacks, o.Stacks)
	m.Goroutines = max(m.Goroutines, o.Goroutines)
}

func (m memory) String() string {
	return fmt.Sprintf("RSS %s, heap in use %s, live heap %s, stacks %s, %d goroutines", mib(m.RSS), mib(m.HeapInuse), mib(m.HeapLive), mib(m.Stacks), m.Goroutines)
}

func mib(b uint64) string { return fmt.Sprintf("%.1f MiB", float64(b)/(1<<20)) }

type sampler struct {
	op     *operatorProcess
	stop   chan struct{}
	done   chan struct{}
	mu     sync.Mutex
	peak   memory
	last   memory
	writes int64
}

const statusWrites = "patch controlplanes/status"

func sample(op *operatorProcess) *sampler {
	s := &sampler{op: op, stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(s.done)
		var previous int64 = -1
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		for {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			stats, err := op.read(ctx)
			rss, rssErr := op.rss(ctx)
			cancel()
			if err == nil && rssErr == nil {
				m := memory{RSS: rss, HeapInuse: stats.HeapInuse, HeapLive: stats.HeapLive, Stacks: stats.Stacks, Goroutines: stats.Goroutines}
				s.mu.Lock()
				s.peak.max(m)
				s.last = m
				if previous >= 0 {
					s.writes = max(s.writes, stats.Requests[statusWrites]-previous)
				}
				previous = stats.Requests[statusWrites]
				s.mu.Unlock()
			}
			select {
			case <-s.stop:
				return
			case <-tick.C:
			}
		}
	}()
	return s
}

func (s *sampler) end() (peak, last memory, busiestWrites int64) {
	close(s.stop)
	<-s.done
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.peak, s.last, s.writes
}

type delta struct {
	requests, tracked map[string]int64
	reconciles        map[string]float64
	errors            map[string]float64
}

func diff(before, after operatorStats) delta {
	d := delta{requests: map[string]int64{}, tracked: map[string]int64{}, reconciles: map[string]float64{}, errors: map[string]float64{}}
	for k, v := range after.Requests {
		if n := v - before.Requests[k]; n != 0 {
			d.requests[k] = n
		}
	}
	for k, v := range after.Tracked {
		if n := v - before.Tracked[k]; n != 0 {
			d.tracked[k] = n
		}
	}
	for k, v := range after.Reconciles {
		if n := v - before.Reconciles[k]; n != 0 {
			d.reconciles[k] = n
		}
	}
	for k, v := range after.Errors {
		if n := v - before.Errors[k]; n != 0 {
			d.errors[k] = n
		}
	}
	return d
}

func total(m map[string]int64) int64 {
	var n int64
	for _, v := range m {
		n += v
	}
	return n
}

func counts[V int64 | float64](m map[string]V, window time.Duration) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if m[keys[i]] != m[keys[j]] {
			return m[keys[i]] > m[keys[j]]
		}
		return keys[i] < keys[j]
	})
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		if window > 0 {
			parts = append(parts, fmt.Sprintf("%s %.1f/s", k, float64(m[k])/window.Seconds()))
		} else {
			parts = append(parts, fmt.Sprintf("%s %v", k, m[k]))
		}
	}
	if len(parts) == 0 {
		return "none"
	}
	return strings.Join(parts, ", ")
}

func quantiles(ds []time.Duration) string {
	if len(ds) == 0 {
		return "none"
	}
	sorted := slices.Clone(ds)
	slices.Sort(sorted)
	at := func(q float64) time.Duration { return sorted[min(len(sorted)-1, int(q*float64(len(sorted))))] }
	return fmt.Sprintf("p50 %s, p99 %s, max %s (%d)", at(0.5).Round(10*time.Millisecond), at(0.99).Round(10*time.Millisecond), sorted[len(sorted)-1].Round(10*time.Millisecond), len(sorted))
}

// timed runs f n times and returns the median time and the bytes the process
// allocated per run, which other goroutines can only add to.
func timed(n int, f func() error) (time.Duration, uint64, error) {
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	times := make([]time.Duration, 0, n)
	for range n {
		start := time.Now()
		if err := f(); err != nil {
			return 0, 0, err
		}
		times = append(times, time.Since(start))
	}
	runtime.ReadMemStats(&after)
	slices.Sort(times)
	return times[len(times)/2], (after.TotalAlloc - before.TotalAlloc) / uint64(n), nil
}

type wireCounter struct{ n atomic.Int64 }

func (c *wireCounter) wrap(next http.RoundTripper) http.RoundTripper {
	return roundTripper(func(req *http.Request) (*http.Response, error) {
		resp, err := next.RoundTrip(req)
		if err == nil {
			resp.Body = countedBody{resp.Body, &c.n}
		}
		return resp, err
	})
}

type countedBody struct {
	io.ReadCloser
	n *atomic.Int64
}

func (b countedBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.n.Add(int64(n))
	return n, err
}

func uncached(cfg *rest.Config) (client.Client, *wireCounter, error) {
	counter := &wireCounter{}
	own := rest.CopyConfig(cfg)
	own.Wrap(counter.wrap)
	c, err := client.New(own, client.Options{Scheme: operator.Scheme()})
	return c, counter, err
}

func kubeletNode(name string, at time.Time) *corev1.Node {
	images := make([]corev1.ContainerImage, 24)
	for i := range images {
		images[i] = corev1.ContainerImage{Names: []string{fmt.Sprintf("registry.example.com/team/image-%02d@sha256:%064d", i, i), fmt.Sprintf("registry.example.com/team/image-%02d:v1.%d.0", i, i)}, SizeBytes: 123456789}
	}
	condition := func(t corev1.NodeConditionType, status corev1.ConditionStatus, reason, message string) corev1.NodeCondition {
		return corev1.NodeCondition{Type: t, Status: status, Reason: reason, Message: message, LastHeartbeatTime: metav1.NewTime(at), LastTransitionTime: metav1.NewTime(at)}
	}
	resources := corev1.ResourceList{
		corev1.ResourceCPU: resource.MustParse("8"), corev1.ResourceMemory: resource.MustParse("32780676Ki"),
		corev1.ResourceEphemeralStorage: resource.MustParse("253196720Ki"), corev1.ResourcePods: resource.MustParse("110"),
	}
	return &corev1.Node{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Node"},
		ObjectMeta: metav1.ObjectMeta{
			Name: name, UID: types.UID("node-" + name), ResourceVersion: "1",
			Labels: map[string]string{
				"beta.kubernetes.io/arch": "amd64", "beta.kubernetes.io/os": "linux", "kubernetes.io/arch": "amd64",
				"kubernetes.io/hostname": name, "kubernetes.io/os": "linux", "topology.kubernetes.io/zone": "zone-a",
			},
			Annotations: map[string]string{"node.alpha.kubernetes.io/ttl": "0", "volumes.kubernetes.io/controller-managed-attach-detach": "true"},
		},
		Spec: corev1.NodeSpec{PodCIDR: "10.244.1.0/24", PodCIDRs: []string{"10.244.1.0/24"}, ProviderID: "provider://" + name},
		Status: corev1.NodeStatus{
			Capacity: resources, Allocatable: resources,
			Conditions: []corev1.NodeCondition{
				condition(corev1.NodeMemoryPressure, corev1.ConditionFalse, "KubeletHasSufficientMemory", "kubelet has sufficient memory available"),
				condition(corev1.NodeDiskPressure, corev1.ConditionFalse, "KubeletHasNoDiskPressure", "kubelet has no disk pressure"),
				condition(corev1.NodePIDPressure, corev1.ConditionFalse, "KubeletHasSufficientPID", "kubelet has sufficient PID available"),
				condition(corev1.NodeReady, corev1.ConditionTrue, "KubeletReady", "kubelet is posting ready status"),
			},
			Addresses:       []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "10.0.0.1"}, {Type: corev1.NodeHostName, Address: name}},
			DaemonEndpoints: corev1.NodeDaemonEndpoints{KubeletEndpoint: corev1.DaemonEndpoint{Port: 10250}},
			NodeInfo: corev1.NodeSystemInfo{
				MachineID: "0123456789abcdef0123456789abcdef", SystemUUID: "01234567-89ab-cdef-0123-456789abcdef", BootID: "01234567-89ab-cdef-0123-456789abcdef",
				KernelVersion: "6.8.0-1000-generic", OSImage: "Ubuntu 24.04.1 LTS", ContainerRuntimeVersion: "containerd://2.0.0",
				KubeletVersion: kubernetesVersion, OperatingSystem: "linux", Architecture: "amd64",
			},
			Images: images,
		},
	}
}
