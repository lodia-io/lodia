// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package v1alpha1_test

import (
	"bytes"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/gardener/etcd-druid/api/core/v1alpha1/crds"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"sigs.k8s.io/yaml"
)

var etcdFields = []struct{ class, etcd string }{
	{"spec.etcd.quotaBackendBytes", "spec.etcd.quota"},
	{"spec.etcd.autoCompaction.mode", "spec.sharedConfig.autoCompactionMode"},
	{"spec.etcd.autoCompaction.retention", "spec.sharedConfig.autoCompactionRetention"},
	{"spec.etcd.defragmentation.schedule", "spec.etcd.defragmentationSchedule"},
	{"spec.etcd.resources", "spec.etcd.resources"},
	{"spec.storage.className", "spec.storageClass"},
	{"spec.storage.size", "spec.storageCapacity"},
	{"spec.backup.store.provider", "spec.backup.store.provider"},
	{"spec.backup.store.bucket", "spec.backup.store.container"},
	{"spec.backup.store.endpoint", "spec.backup.store.endpointOverride"},
	{"spec.backup.fullSnapshotSchedule", "spec.backup.fullSnapshotSchedule"},
	{"spec.backup.deltaSnapshotPeriod", "spec.backup.deltaSnapshotPeriod"},
	{"spec.backup.retention.days", "spec.backup.maxBackupsLimitBasedGC"},
	{"spec.placement.tolerations", "spec.schedulingConstraints.tolerations"},
	{"spec.placement.priorityClassName", "spec.priorityClassName"},
}

var acknowledgedEtcdRules = []struct {
	path, why string
	rules     []string
}{
	{"", "they read additionalAdvertisedURLs or bootstrapWithExistingCluster, which Lodia never sets, or a change of replicas, which it keeps at 1", []string{
		`!has(self.spec.etcd.additionalAdvertisedURLs) || !has(self.spec.etcd.additionalAdvertisedURLs.peerURLs) || self.spec.etcd.additionalAdvertisedURLs.peerURLs.members.all(m, has(self.spec.memberNamePrefix) ? m.name.startsWith(self.spec.memberNamePrefix + '-' + self.metadata.name + '-') : m.name.startsWith(self.metadata.name + '-'))`,
		`!has(self.spec.etcd.additionalAdvertisedURLs) || !has(self.spec.etcd.additionalAdvertisedURLs.peerURLs) || self.spec.etcd.additionalAdvertisedURLs.peerURLs.members.all(m, int(m.name.substring(m.name.lastIndexOf('-')+1)) < self.spec.replicas)`,
		`!has(self.spec.etcd.additionalAdvertisedURLs) || !has(self.spec.etcd.additionalAdvertisedURLs.clientURLs) || self.spec.etcd.additionalAdvertisedURLs.clientURLs.members.all(m, has(self.spec.memberNamePrefix) ? m.name.startsWith(self.spec.memberNamePrefix + '-' + self.metadata.name + '-') : m.name.startsWith(self.metadata.name + '-'))`,
		`!has(self.spec.etcd.additionalAdvertisedURLs) || !has(self.spec.etcd.additionalAdvertisedURLs.clientURLs) || self.spec.etcd.additionalAdvertisedURLs.clientURLs.members.all(m, int(m.name.substring(m.name.lastIndexOf('-')+1)) < self.spec.replicas)`,
		`!has(self.spec.etcd.bootstrapWithExistingCluster) || !has(oldSelf.spec.etcd.bootstrapWithExistingCluster) || !has(self.status) || !has(self.status.conditions) || !self.status.conditions.exists(c, c.type == 'BootstrappedWithExistingCluster' && c.status == 'False') || self.spec.etcd.bootstrapWithExistingCluster.members == oldSelf.spec.etcd.bootstrapWithExistingCluster.members`,
		`!has(self.spec.etcd.bootstrapWithExistingCluster) || !has(oldSelf.spec.etcd.bootstrapWithExistingCluster) || !has(self.status) || !has(self.status.conditions) || !self.status.conditions.exists(c, c.type == 'BootstrappedWithExistingCluster' && c.status == 'False') || self.spec.etcd.bootstrapWithExistingCluster.clientEndpoints == oldSelf.spec.etcd.bootstrapWithExistingCluster.clientEndpoints`,
		`!has(self.spec.etcd.bootstrapWithExistingCluster) || self.spec.etcd.bootstrapWithExistingCluster.members.all(m1, self.spec.etcd.bootstrapWithExistingCluster.members.filter(m2, m1.name == m2.name).size() == 1)`,
		`!has(self.spec.etcd.bootstrapWithExistingCluster) || self.spec.etcd.bootstrapWithExistingCluster.members.all(m, has(self.spec.memberNamePrefix) ? !m.name.startsWith(self.spec.memberNamePrefix + '-' + self.metadata.name + '-') : !m.name.startsWith(self.metadata.name + '-'))`,
		`(self.spec.replicas > oldSelf.spec.replicas && self.spec.replicas > 0 && oldSelf.spec.replicas > 0) ? (!has(self.status) || !has(self.status.conditions) || !self.status.conditions.exists(c, c.type == 'ScaleOperationComplete' && c.status == 'False' && (c.reason == 'ScalingIn' || c.reason == 'BootstrapMembersRemoval'))) : true`,
		`(self.spec.replicas < oldSelf.spec.replicas && self.spec.replicas > 0 && oldSelf.spec.replicas > 0) ? (!has(self.status) || !has(self.status.conditions) || !self.status.conditions.exists(c, c.type == 'ScaleOperationComplete' && c.status == 'False' && (c.reason == 'ScalingOut' || c.reason == 'BootstrapMembersRemoval'))) : true`,
		`(self.spec.replicas == 0 && oldSelf.spec.replicas > 0) ? (!has(self.status) || !has(self.status.conditions) || !self.status.conditions.exists(c, c.type == 'ScaleOperationComplete' && c.status == 'False' && (c.reason == 'ScalingIn' || c.reason == 'BootstrapMembersRemoval'))) : true`,
		`has(oldSelf.spec.etcd.bootstrapWithExistingCluster) && has(self.spec.etcd.bootstrapWithExistingCluster) && self.spec.etcd.bootstrapWithExistingCluster.members != oldSelf.spec.etcd.bootstrapWithExistingCluster.members ? (!has(self.status) || !has(self.status.conditions) || !self.status.conditions.exists(c, c.type == 'ScaleOperationComplete' && c.status == 'False' && (c.reason == 'ScalingIn' || c.reason == 'ScalingOut'))) : true`,
		`has(oldSelf.spec.etcd.bootstrapWithExistingCluster) && !has(self.spec.etcd.bootstrapWithExistingCluster) ? (!has(self.status) || !has(self.status.conditions) || !self.status.conditions.exists(c, c.type == 'ScaleOperationComplete' && c.status == 'False' && (c.reason == 'ScalingIn' || c.reason == 'ScalingOut'))) : true`,
	}},
	{"spec", "they read memberNamePrefix, volumeClaimTemplate, externallyManagedMemberAddresses and volume mounts, which Lodia never sets, and whether storageClass is set, which it keeps as the Etcd was created", []string{
		`has(oldSelf.memberNamePrefix) == has(self.memberNamePrefix)`,
		`has(oldSelf.storageClass) ==  has(self.storageClass)`,
		`has(oldSelf.volumeClaimTemplate) == has(self.volumeClaimTemplate)`,
		`has(self.externallyManagedMemberAddresses) ? self.replicas == self.externallyManagedMemberAddresses.size() : true`,
		`has(oldSelf.externallyManagedMemberAddresses) == has(self.externallyManagedMemberAddresses)`,
		`!has(self.etcd.volumeMounts) || self.etcd.volumeMounts.all(vm, has(self.volumes) && self.volumes.exists(v, v.name == vm.name))`,
		`!has(self.backup.volumeMounts) || self.backup.volumeMounts.all(vm, has(self.volumes) && self.volumes.exists(v, v.name == vm.name))`,
	}},
	{"spec.backup", "Lodia never sets garbageCollectionPeriod", []string{
		`!(has(self.deltaSnapshotPeriod) && has(self.garbageCollectionPeriod)) || duration(self.deltaSnapshotPeriod).getSeconds() < duration(self.garbageCollectionPeriod).getSeconds()`,
	}},
	{"spec.etcd", "Lodia never sets additionalAdvertisedURLs or bootstrapWithExistingCluster", []string{
		`!has(self.additionalAdvertisedURLs) || !has(self.additionalAdvertisedURLs.peerURLs) || !has(self.peerUrlTls) || self.additionalAdvertisedURLs.peerURLs.members.all(m, m.urls.all(u, u.startsWith('https://')))`,
		`!has(self.additionalAdvertisedURLs) || !has(self.additionalAdvertisedURLs.peerURLs) || has(self.peerUrlTls) || self.additionalAdvertisedURLs.peerURLs.members.all(m, m.urls.all(u, u.startsWith('http://')))`,
		`!has(self.additionalAdvertisedURLs) || !has(self.additionalAdvertisedURLs.clientURLs) || !has(self.clientUrlTls) || self.additionalAdvertisedURLs.clientURLs.members.all(m, m.urls.all(u, u.startsWith('https://')))`,
		`!has(self.additionalAdvertisedURLs) || !has(self.additionalAdvertisedURLs.clientURLs) || has(self.clientUrlTls) || self.additionalAdvertisedURLs.clientURLs.members.all(m, m.urls.all(u, u.startsWith('http://')))`,
		`!has(self.bootstrapWithExistingCluster) || !has(self.peerUrlTls) || self.bootstrapWithExistingCluster.members.all(m, m.peerUrls.all(u, u.startsWith('https://')))`,
		`!has(self.bootstrapWithExistingCluster) || has(self.peerUrlTls) || self.bootstrapWithExistingCluster.members.all(m, m.peerUrls.all(u, u.startsWith('http://')))`,
		`!has(self.bootstrapWithExistingCluster) || !has(self.clientUrlTls) || self.bootstrapWithExistingCluster.clientEndpoints.all(u, u.startsWith('https://'))`,
		`!has(self.bootstrapWithExistingCluster) || has(self.clientUrlTls) || self.bootstrapWithExistingCluster.clientEndpoints.all(u, u.startsWith('http://'))`,
		`!has(self.bootstrapWithExistingCluster) || has(oldSelf.bootstrapWithExistingCluster)`,
	}},
	{"spec.storageClass", "Lodia keeps the storage class the Etcd was created with", []string{
		`self == oldSelf`,
	}},
}

func TestDatastoreClassMirrorsEtcdDruid(t *testing.T) {
	manifests, err := crds.GetAll("1.35")
	if err != nil {
		t.Fatal(err)
	}
	etcd := crdSchema(t, []byte(manifests[crds.ResourceNameEtcd]))
	raw, err := os.ReadFile(filepath.Join("..", "..", "config", "crd", "bases", "lodia.io_datastoreclasses.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	class := crdSchema(t, raw)

	acknowledged := map[string][]string{}
	for _, a := range acknowledgedEtcdRules {
		acknowledged[a.path] = a.rules
		if got := rules(at(t, etcd, a.path)); !slices.Equal(got, a.rules) {
			t.Errorf("etcd-druid's rules on %q changed; acknowledged because %s, now:\n%s", a.path, a.why, strings.Join(got, "\n"))
		}
	}
	for _, f := range etcdFields {
		for parent := f.etcd; parent != ""; {
			parent, _ = cut(parent)
			if _, ok := acknowledged[parent]; !ok && len(rules(at(t, etcd, parent))) > 0 {
				t.Errorf("etcd-druid's rules on %q, a parent of %s, are not acknowledged:\n%s", parent, f.etcd, strings.Join(rules(at(t, etcd, parent)), "\n"))
			}
		}
		mirror(t, f.class, at(t, class, f.class), f.etcd, at(t, etcd, f.etcd), acknowledged)
	}
}

func mirror(t *testing.T, path string, class apiextensionsv1.JSONSchemaProps, etcdPath string, etcd apiextensionsv1.JSONSchemaProps, acknowledged map[string][]string) {
	t.Helper()
	if etcd.Type != class.Type || etcd.Format != "" && etcd.Format != class.Format {
		t.Errorf("%s is %s %s, etcd-druid's %s is %s %s", path, class.Type, class.Format, etcdPath, etcd.Type, etcd.Format)
	}
	if etcd.Pattern != "" && etcd.Pattern != class.Pattern {
		t.Errorf("%s has the pattern %q, etcd-druid's %s %q", path, class.Pattern, etcdPath, etcd.Pattern)
	}
	if len(etcd.Enum) > 0 && (len(class.Enum) == 0 || slices.ContainsFunc(class.Enum, func(v apiextensionsv1.JSON) bool {
		return !slices.ContainsFunc(etcd.Enum, func(e apiextensionsv1.JSON) bool { return bytes.Equal(e.Raw, v.Raw) })
	})) {
		t.Errorf("%s allows values etcd-druid's %s refuses", path, etcdPath)
	}
	for _, b := range []struct {
		name        string
		class, etcd *int64
		upper       bool
	}{
		{"maxLength", class.MaxLength, etcd.MaxLength, true},
		{"minLength", class.MinLength, etcd.MinLength, false},
		{"maxItems", class.MaxItems, etcd.MaxItems, true},
		{"minItems", class.MinItems, etcd.MinItems, false},
		{"maxProperties", class.MaxProperties, etcd.MaxProperties, true},
		{"minProperties", class.MinProperties, etcd.MinProperties, false},
	} {
		if b.etcd != nil && (b.class == nil || b.upper && *b.class > *b.etcd || !b.upper && *b.class < *b.etcd) {
			t.Errorf("%s is looser than etcd-druid's %s in %s", path, etcdPath, b.name)
		}
	}
	for _, b := range []struct {
		name        string
		class, etcd *float64
		upper       bool
	}{
		{"maximum", class.Maximum, etcd.Maximum, true},
		{"minimum", class.Minimum, etcd.Minimum, false},
	} {
		if b.etcd != nil && (b.class == nil || b.upper && *b.class > *b.etcd || !b.upper && *b.class < *b.etcd) {
			t.Errorf("%s is looser than etcd-druid's %s in %s", path, etcdPath, b.name)
		}
	}
	for _, r := range etcd.XValidations {
		if !slices.Contains(rules(class), r.Rule) && !slices.Contains(acknowledged[etcdPath], r.Rule) {
			t.Errorf("%s lacks etcd-druid's rule on %s: %s", path, etcdPath, r.Rule)
		}
	}
	for name, e := range etcd.Properties {
		if c, ok := class.Properties[name]; ok {
			mirror(t, path+"."+name, c, etcdPath+"."+name, e, acknowledged)
		}
	}
	if etcd.Items != nil && etcd.Items.Schema != nil && class.Items != nil && class.Items.Schema != nil {
		mirror(t, path+"[]", *class.Items.Schema, etcdPath+"[]", *etcd.Items.Schema, acknowledged)
	}
	if etcd.AdditionalProperties != nil && etcd.AdditionalProperties.Schema != nil && class.AdditionalProperties != nil && class.AdditionalProperties.Schema != nil {
		mirror(t, path+"{}", *class.AdditionalProperties.Schema, etcdPath+"{}", *etcd.AdditionalProperties.Schema, acknowledged)
	}
}

func crdSchema(t *testing.T, manifest []byte) apiextensionsv1.JSONSchemaProps {
	t.Helper()
	crd := &apiextensionsv1.CustomResourceDefinition{}
	if err := yaml.Unmarshal(manifest, crd); err != nil {
		t.Fatal(err)
	}
	return *crd.Spec.Versions[0].Schema.OpenAPIV3Schema
}

func at(t *testing.T, schema apiextensionsv1.JSONSchemaProps, path string) apiextensionsv1.JSONSchemaProps {
	t.Helper()
	for name := range strings.SplitSeq(path, ".") {
		if name == "" {
			continue
		}
		child, ok := schema.Properties[name]
		if !ok {
			t.Fatalf("the schema has no %s", path)
		}
		schema = child
	}
	return schema
}

func cut(path string) (string, string) {
	i := strings.LastIndexByte(path, '.')
	if i < 0 {
		return "", path
	}
	return path[:i], path[i+1:]
}

func rules(schema apiextensionsv1.JSONSchemaProps) []string {
	out := make([]string, 0, len(schema.XValidations))
	for _, r := range schema.XValidations {
		out = append(out, r.Rule)
	}
	return out
}
