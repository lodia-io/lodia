// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package datastore

import (
	"context"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

var Unrecordable = unrecordable

func Told(l *Latches) <-chan event.GenericEvent { return l.told }

func Tell(l *Latches, key types.NamespacedName, uid types.UID, told lodiav1alpha1.MemberRestoreStatus) {
	l.tell(key, uid, told)
}

func Latched(l *Latches, key types.NamespacedName) *lodiav1alpha1.MemberRestoreStatus {
	l.mu.Lock()
	defer l.mu.Unlock()
	if held := l.held[key]; held != nil {
		return new(held.told)
	}
	return nil
}

func Send(ctx context.Context, l *Latches, cp client.Object) { l.send(ctx, cp) }
