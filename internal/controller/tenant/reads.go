// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package tenant

import (
	"encoding/json"
	"errors"
	"slices"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"go.lodia.io/lodia/internal/jsonscan"
	"go.lodia.io/lodia/internal/tenantclient"
)

const (
	// maxUIDRaw is the raw bound of a UID, which is 36 bytes.
	maxUIDRaw = 64

	// maxManagers is how many field managers of another owner's object the
	// message names.
	maxManagers = 10

	// maxEntries is the most objects an inventory lists: a render has fewer
	// than 50.
	maxEntries = 256

	// maxInventoryRaw is the raw bound of the inventory's text: maxEntries
	// entries of five names of 253 bytes are 324,000 bytes, which the
	// ConfigMap's string escapes to at most twice that.
	maxInventoryRaw = 1 << 20
)

var (
	errNotInTable = errors.New("the kind is not one Tenant writes")
	errTooMany    = errors.New("the inventory lists more objects than Tenant applies")
)

type object struct {
	uid, resourceVersion string
	applied              bool
	managers             managers
}

func objectOf(data []byte) (*object, error) {
	o := new(object)
	err := jsonscan.Walk("the object", data, func(key, value []byte) error {
		if string(key) != "metadata" {
			return nil
		}
		return jsonscan.Walk("metadata", value, func(key, value []byte) (err error) {
			switch string(key) {
			case "uid":
				o.uid, err = jsonscan.Text("metadata.uid", value, maxUIDRaw)
			case "resourceVersion":
				o.resourceVersion, err = jsonscan.Text("metadata.resourceVersion", value, tenantclient.MaxVersionRaw)
			case "managedFields":
				err = o.scan(value)
			}
			return err
		})
	})
	return o, err
}

// scan reads every managedFields entry, since Lodia's apply may be the last of
// them, and keeps no more than maxManagers names.
func (o *object) scan(value []byte) error {
	switch {
	case jsonscan.Null(value):
		return nil
	case value[0] != '[':
		return jsonscan.NotA("metadata.managedFields", "an array")
	}
	for value := range jsonscan.Elements(value) {
		var manager, operation string
		err := jsonscan.Walk("a managedFields entry", value, func(key, value []byte) (err error) {
			switch string(key) {
			case "manager":
				manager, err = jsonscan.Text("metadata.managedFields.manager", value, tenantclient.MaxNameRaw)
			case "operation":
				operation, err = jsonscan.Word("metadata.managedFields.operation", value, tenantclient.MaxWordRaw)
			}
			return err
		})
		if err != nil {
			return err
		}
		if manager == "" {
			continue
		}
		if manager == FieldManager && operation == string(metav1.ManagedFieldsOperationApply) {
			o.applied = true
		}
		o.managers.add(manager)
	}
	return nil
}

// managers keeps the first maxManagers names of the managedFields entries that
// sort first, once each, and counts the entries of the others.
type managers struct {
	names  []string
	counts []int
	total  int
}

func (m *managers) add(name string) {
	m.total++
	name = tenantclient.Cut(name, tenantclient.MaxName)
	at, found := slices.BinarySearch(m.names, name)
	switch {
	case found:
		m.counts[at]++
		return
	case at == maxManagers:
		return
	case len(m.names) == maxManagers:
		m.names, m.counts = m.names[:maxManagers-1], m.counts[:maxManagers-1]
	}
	m.names, m.counts = slices.Insert(m.names, at, name), slices.Insert(m.counts, at, 1)
}

func (m managers) more() int {
	kept := 0
	for _, n := range m.counts {
		kept += n
	}
	return m.total - kept
}

type workload struct {
	generation, observedGeneration             int32
	replicas                                   *int32
	updated, available, desiredNumberScheduled int32
}

func workloadOf(kind string) func([]byte) (*workload, error) {
	updated, available := "updatedReplicas", "availableReplicas"
	if kind == "DaemonSet" {
		updated, available = "updatedNumberScheduled", "numberAvailable"
	}
	return func(data []byte) (*workload, error) {
		w := new(workload)
		number := func(field string, value []byte, into *int32) error {
			n, err := jsonscan.Integer(field, value, tenantclient.MaxNumberRaw)
			if n != nil {
				*into = *n
			}
			return err
		}
		err := jsonscan.Walk("the workload", data, func(key, value []byte) error {
			switch string(key) {
			case "metadata":
				return jsonscan.Walk("metadata", value, func(key, value []byte) error {
					if string(key) == "generation" {
						return number("metadata.generation", value, &w.generation)
					}
					return nil
				})
			case "spec":
				return jsonscan.Walk("spec", value, func(key, value []byte) (err error) {
					if string(key) == "replicas" {
						w.replicas, err = jsonscan.Integer("spec.replicas", value, tenantclient.MaxNumberRaw)
					}
					return err
				})
			case "status":
				return jsonscan.Walk("status", value, func(key, value []byte) error {
					switch string(key) {
					case "observedGeneration":
						return number("status.observedGeneration", value, &w.observedGeneration)
					case updated:
						return number("status."+updated, value, &w.updated)
					case available:
						return number("status."+available, value, &w.available)
					case "desiredNumberScheduled":
						return number("status.desiredNumberScheduled", value, &w.desiredNumberScheduled)
					}
					return nil
				})
			}
			return nil
		})
		return w, err
	}
}

type inventory struct{ entries []entry }

// inventoryFrom reads the entries of the inventory ConfigMap's data.objects. An
// entry's names are within the bound of a name, and an inventory of more than
// maxEntries entries is as unreadable as one that is no JSON.
func inventoryFrom(data []byte) (*inventory, error) {
	var text string
	err := jsonscan.Walk("the inventory", data, func(key, value []byte) error {
		if string(key) != "data" {
			return nil
		}
		return jsonscan.Walk("data", value, func(key, value []byte) (err error) {
			if string(key) == inventoryKey {
				text, err = jsonscan.Text("data."+inventoryKey, value, maxInventoryRaw)
			}
			return err
		})
	})
	if err != nil {
		return nil, err
	}
	if !json.Valid([]byte(text)) {
		return nil, tenantclient.ErrNotJSON
	}
	entries, err := tenantclient.ArrayOf("the inventory", []byte(text), maxEntries, entryFrom)
	if err != nil {
		return nil, err
	}
	if entries.Over {
		return nil, errTooMany
	}
	return &inventory{entries.Items}, nil
}

func entryFrom(data []byte) (e entry, err error) {
	err = jsonscan.Walk("an entry", data, func(key, value []byte) (err error) {
		var into *string
		switch string(key) {
		case "group":
			into = &e.Group
		case "version":
			into = &e.Version
		case "kind":
			into = &e.Kind
		case "namespace":
			into = &e.Namespace
		case "name":
			into = &e.Name
		default:
			return nil
		}
		*into, err = jsonscan.Text("an entry's "+string(key), value, tenantclient.MaxNameRaw)
		if err == nil && len(*into) > tenantclient.MaxName {
			err = &jsonscan.TooLong{Field: "an entry's " + string(key), Most: tenantclient.MaxName}
		}
		return err
	})
	return e, err
}
