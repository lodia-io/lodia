// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package datastore

import "time"

// etcdRequestTimeout bounds every call to a tenant's etcd.
const etcdRequestTimeout = 10 * time.Second
