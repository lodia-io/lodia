// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package testenv

import (
	"context"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	toolscache "k8s.io/client-go/tools/cache"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

// Lagging is a cache's NewInformer that builds the informer of ControlPlanes
// on a watch that hands each event on lag after it came, so that the cache is
// always that far behind the API server.
func Lagging(lag time.Duration) func(toolscache.ListerWatcher, runtime.Object, time.Duration, toolscache.Indexers) toolscache.SharedIndexInformer {
	return func(lw toolscache.ListerWatcher, obj runtime.Object, resync time.Duration, indexers toolscache.Indexers) toolscache.SharedIndexInformer {
		if listWatch, ok := lw.(*toolscache.ListWatch); ok {
			if _, controlPlane := obj.(*lodiav1alpha1.ControlPlane); controlPlane {
				watchWith := listWatch.WatchFuncWithContext
				lw = &toolscache.ListWatch{
					ListWithContextFunc: listWatch.ListWithContextFunc,
					WatchFuncWithContext: func(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
						w, err := watchWith(ctx, options)
						if err != nil {
							return nil, err
						}
						return delayed(w, lag), nil
					},
				}
			}
		}
		return toolscache.NewSharedIndexInformer(lw, obj, resync, indexers)
	}
}

type delayedWatch struct {
	watch.Interface
	out  chan watch.Event
	stop chan struct{}
	once sync.Once
}

func delayed(w watch.Interface, lag time.Duration) watch.Interface {
	type stamped struct {
		event watch.Event
		due   time.Time
	}
	d := &delayedWatch{Interface: w, out: make(chan watch.Event), stop: make(chan struct{})}
	queue := make(chan stamped, 100_000)
	go func() {
		defer close(queue)
		for event := range w.ResultChan() {
			queue <- stamped{event, time.Now().Add(lag)}
		}
	}()
	go func() {
		defer close(d.out)
		for s := range queue {
			select {
			case <-time.After(time.Until(s.due)):
			case <-d.stop:
				return
			}
			select {
			case d.out <- s.event:
			case <-d.stop:
				return
			}
		}
	}()
	return d
}

func (d *delayedWatch) ResultChan() <-chan watch.Event { return d.out }

func (d *delayedWatch) Stop() {
	d.once.Do(func() { close(d.stop) })
	d.Interface.Stop()
}
