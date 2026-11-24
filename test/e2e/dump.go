// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package e2e

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/yaml"
)

// withheld are the resources a dump leaves out, by name and group: a Secret
// holds credentials, and the CustomResourceDefinitions are the chart's.
var withheld = []string{"secrets", "customresourcedefinitions.apiextensions.k8s.io"}

// Dump writes into dir what a failed run is read from: every object of the
// cluster cfg reaches, events included, every container's log, and the
// consoles and journals of the containers the suite started for the kind
// cluster named cluster. It goes on past a failure and returns all of them.
func Dump(ctx context.Context, cfg *rest.Config, cluster, dir string) error {
	cfg = rest.CopyConfig(cfg)
	cfg.QPS, cfg.Burst = 50, 100
	k8s, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return err
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return err
	}
	return errors.Join(dumpObjects(ctx, k8s.Discovery(), dyn, dir), dumpLogs(ctx, k8s, dir), dumpContainers(ctx, cluster, dir))
}

func dumpObjects(ctx context.Context, disco discovery.DiscoveryInterface, dyn dynamic.Interface, dir string) error {
	_, lists, err := discovery.ToServerResourcesInterfaceWithContext(disco).ServerGroupsAndResourcesWithContext(ctx)
	errs := []error{err}
	seen := map[string]bool{}
	for _, list := range lists {
		gv, err := schema.ParseGroupVersion(list.GroupVersion)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		for _, r := range list.APIResources {
			name := strings.TrimSuffix(r.Name+"."+gv.Group, ".")
			if !slices.Contains(r.Verbs, "list") || seen[name] || slices.Contains(withheld, name) {
				continue
			}
			seen[name] = true
			items, err := dyn.Resource(gv.WithResource(r.Name)).List(ctx, metav1.ListOptions{})
			if err != nil {
				errs = append(errs, fmt.Errorf("list %s: %w", name, err))
				continue
			}
			if len(items.Items) == 0 {
				continue
			}
			for i := range items.Items {
				unstructured.RemoveNestedField(items.Items[i].Object, "metadata", "managedFields")
			}
			out, err := yaml.Marshal(items.UnstructuredContent())
			if err != nil {
				errs = append(errs, fmt.Errorf("marshal %s: %w", name, err))
				continue
			}
			errs = append(errs, write(filepath.Join(dir, "objects", name+".yaml"), out))
		}
	}
	return errors.Join(errs...)
}

func dumpLogs(ctx context.Context, k8s kubernetes.Interface, dir string) error {
	pods, err := k8s.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	var errs []error
	for _, pod := range pods.Items {
		for _, status := range slices.Concat(pod.Status.InitContainerStatuses, pod.Status.ContainerStatuses) {
			base := filepath.Join(dir, "logs", pod.Namespace, pod.Name, status.Name)
			errs = append(errs, writeLog(ctx, k8s, pod, status.Name, false, base+".log"))
			if status.RestartCount > 0 {
				errs = append(errs, writeLog(ctx, k8s, pod, status.Name, true, base+".previous.log"))
			}
		}
	}
	return errors.Join(errs...)
}

func writeLog(ctx context.Context, k8s kubernetes.Interface, pod corev1.Pod, container string, previous bool, path string) error {
	stream, err := k8s.CoreV1().Pods(pod.Namespace).GetLogs(pod.Name, &corev1.PodLogOptions{Container: container, Previous: previous, Timestamps: true}).Stream(ctx)
	if err != nil {
		return write(path, []byte(err.Error()+"\n"))
	}
	defer func() { _ = stream.Close() }()
	log, err := io.ReadAll(stream)
	if err != nil {
		return errors.Join(err, write(path, log))
	}
	return write(path, log)
}

func dumpContainers(ctx context.Context, cluster, dir string) error {
	names, err := suiteContainers(ctx, cluster, "{{.Names}}", containerName)
	if err != nil {
		return err
	}
	var errs []error
	for _, name := range names {
		base := filepath.Join(dir, "containers", name)
		if console, err := docker(ctx, "logs", name); err != nil {
			errs = append(errs, err)
		} else {
			errs = append(errs, write(filepath.Join(base, "console.log"), []byte(console)))
		}
		// The DNS container has no journal.
		if journal, err := docker(ctx, "exec", name, "journalctl", "--no-pager"); err == nil {
			errs = append(errs, write(filepath.Join(base, "journal.log"), []byte(journal)))
		}
	}
	return errors.Join(errs...)
}

func write(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o600)
}
