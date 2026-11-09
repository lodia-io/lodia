// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package operator_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"sigs.k8s.io/yaml"

	"go.lodia.io/lodia/internal/operator"
)

func revisionInputs() operator.Inputs {
	return operator.Inputs{
		Version:            "v1.2.3",
		SignerImage:        "registry.example/lodia-signer:v1.2.3",
		TunnelImage:        "registry.example/lodia-tunnel:v1.2.3",
		EtcdDruidVersion:   "v0.39.0@sha256:80d2ee4803acf96c46f8f1169772127ac2fb9aee7903a3a27c3af8cd1ad78775",
		EtcdDruidNamespace: "lodia-system",
		OperatorNamespace:  "lodia-system",
	}
}

func revisionOf(t *testing.T, in operator.Inputs) string {
	t.Helper()
	revision, err := in.Revision()
	if err != nil {
		t.Fatal(err)
	}
	return revision
}

func TestTheRevisionIsTheVersionAndADigestOfTheRunTimeInputs(t *testing.T) {
	revision := revisionOf(t, revisionInputs())
	if !regexp.MustCompile(`^v1\.2\.3\+[0-9a-f]{64}$`).MatchString(revision) {
		t.Errorf("the revision is %q, want v1.2.3+ and a SHA-256 in hex", revision)
	}
	for range 3 {
		if again := revisionOf(t, revisionInputs()); again != revision {
			t.Errorf("the same inputs give %q after %q", again, revision)
		}
	}
	other := revisionInputs()
	other.Config.Health.Period.Duration++
	other.GatewayAPI = !other.GatewayAPI
	if got := revisionOf(t, other); got != revision {
		t.Errorf("an input outside the digest changed the revision from %q to %q", revision, got)
	}
}

func TestTheRevisionChangesWithEachInputAndWithTheVersion(t *testing.T) {
	base := revisionOf(t, revisionInputs())
	digest := func(revision string) string {
		_, digest, _ := strings.Cut(revision, "+")
		return digest
	}
	for _, c := range []struct {
		name   string
		change func(*operator.Inputs)
	}{
		{"the signer image", func(in *operator.Inputs) { in.SignerImage += "x" }},
		{"the tunnel image", func(in *operator.Inputs) { in.TunnelImage += "x" }},
		{"etcd-druid's version", func(in *operator.Inputs) { in.EtcdDruidVersion += "x" }},
		{"etcd-druid's namespace", func(in *operator.Inputs) { in.EtcdDruidNamespace += "x" }},
		{"the operator's namespace", func(in *operator.Inputs) { in.OperatorNamespace += "x" }},
	} {
		t.Run(c.name, func(t *testing.T) {
			in := revisionInputs()
			c.change(&in)
			got := revisionOf(t, in)
			if got == base || digest(got) == digest(base) {
				t.Errorf("a change of %s leaves the revision %q, was %q", c.name, got, base)
			}
		})
	}
	in := revisionInputs()
	in.Version = "v1.2.4"
	if got := revisionOf(t, in); got == base || digest(got) != digest(base) {
		t.Errorf("a change of the version gives %q, want the digest of %q under another version", got, base)
	}
}

func TestTheDigestTellsWhereOneInputEndsAndTheNextBegins(t *testing.T) {
	inputs := func(in *operator.Inputs) []*string {
		return []*string{&in.SignerImage, &in.TunnelImage, &in.EtcdDruidVersion, &in.EtcdDruidNamespace, &in.OperatorNamespace}
	}
	for i := range len(inputs(&operator.Inputs{})) - 1 {
		in := revisionInputs()
		neighbours := inputs(&in)
		*neighbours[i], *neighbours[i+1] = "ab", "c"
		first := revisionOf(t, in)
		*neighbours[i], *neighbours[i+1] = "a", "bc"
		if second := revisionOf(t, in); second == first {
			t.Errorf(`"ab" and "c" in inputs %d and %d, then "a" and "bc", give one revision, %q`, i, i+1, first)
		}
		*neighbours[i], *neighbours[i+1] = "a b", "c"
		first = revisionOf(t, in)
		*neighbours[i], *neighbours[i+1] = "a", "b c"
		if second := revisionOf(t, in); second == first {
			t.Errorf(`"a b" and "c" in inputs %d and %d, then "a" and "b c", give one revision, %q`, i, i+1, first)
		}
		*neighbours[i], *neighbours[i+1] = "x", "y"
		first = revisionOf(t, in)
		*neighbours[i], *neighbours[i+1] = "y", "x"
		if second := revisionOf(t, in); second == first {
			t.Errorf("inputs %d and %d swapped give one revision, %q", i, i+1, first)
		}
	}
}

func TestTheRevisionFitsEveryFieldItIsStoredIn(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "api", "crds", "lodia.io_controlplanes.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	crd := &apiextensionsv1.CustomResourceDefinition{}
	if err := yaml.Unmarshal(raw, crd); err != nil {
		t.Fatal(err)
	}
	var limits []int64
	var walk func(schema apiextensionsv1.JSONSchemaProps, path string)
	walk = func(schema apiextensionsv1.JSONSchemaProps, path string) {
		for name, property := range schema.Properties {
			if name == "operatorRevision" {
				if property.MaxLength == nil {
					t.Errorf("%s.%s has no maxLength", path, name)
					continue
				}
				limits = append(limits, *property.MaxLength)
			}
			walk(property, path+"."+name)
		}
	}
	walk(*crd.Spec.Versions[0].Schema.OpenAPIV3Schema, "")
	if len(limits) != 6 {
		t.Fatalf("the schema holds %d operatorRevision fields, want 6: the target, three components, the addons and the datastore", len(limits))
	}
	for _, limit := range limits[1:] {
		if limit != limits[0] {
			t.Fatalf("the operatorRevision fields hold %v characters, want one limit", limits)
		}
	}

	const digest = len("+") + 64
	in := revisionInputs()
	in.Version = strings.Repeat("v", int(limits[0])-digest)
	if got := revisionOf(t, in); int64(len(got)) != limits[0] {
		t.Errorf("a version of %d characters gives a revision of %d, want %d", len(in.Version), len(got), limits[0])
	}
	in.Version += "v"
	if revision, err := in.Revision(); err == nil {
		t.Errorf("a revision of %d characters is accepted, the fields hold %d: %q", len(revision), limits[0], revision)
	}
}

func TestTheOperatorRefusesToStartWithoutAVersion(t *testing.T) {
	in := revisionInputs()
	in.Version = ""
	if revision, err := in.Revision(); err == nil {
		t.Errorf("an empty version gives the revision %q", revision)
	}
	if err := operator.Setup(nil, in); err == nil || !strings.Contains(err.Error(), "without a version") {
		t.Errorf("Setup with an empty version returns %v, want the refusal before it touches the manager", err)
	}
}

func TestTheOperatorRefusesAVersionThatIsNotUTF8(t *testing.T) {
	in := revisionInputs()
	in.Version = "v1.2.3\xff"
	if revision, err := in.Revision(); err == nil {
		t.Errorf("a version that is not UTF-8 gives the revision %q", revision)
	}
}

func TestTheOperatorRefusesToStartWithARevisionThatDoesNotFit(t *testing.T) {
	in := revisionInputs()
	in.Version = strings.Repeat("v", 128)
	if err := operator.Setup(nil, in); err == nil || !strings.Contains(err.Error(), "more than the 128") {
		t.Errorf("Setup with a version of 128 characters returns %v, want the refusal before it touches the manager", err)
	}
}
