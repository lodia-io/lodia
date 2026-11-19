// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package authnconfig_test

import (
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/api/equality"
	apiserverv1 "k8s.io/apiserver/pkg/apis/apiserver/v1"
	sigsjson "sigs.k8s.io/json"
	"sigs.k8s.io/randfill"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/authnconfig"
)

func paths(t reflect.Type, prefix string, out map[string]string) {
	pointer := ""
	for t.Kind() == reflect.Pointer {
		t, pointer = t.Elem(), "*"
	}
	switch t.Kind() {
	case reflect.Struct:
		for f := range t.Fields() {
			name, _, _ := strings.Cut(f.Tag.Get("json"), ",")
			if name == "" || name == "-" {
				continue
			}
			paths(f.Type, prefix+pointer+"."+name, out)
		}
	case reflect.Slice:
		paths(t.Elem(), prefix+pointer+"[]", out)
	default:
		out[prefix] = pointer + t.Kind().String()
	}
}

func TestLodiasJWTAuthenticatorHasUpstreamsShape(t *testing.T) {
	ours, theirs := map[string]string{}, map[string]string{}
	paths(reflect.TypeFor[lodiav1alpha1.JWTAuthenticator](), "", ours)
	paths(reflect.TypeFor[apiserverv1.JWTAuthenticator](), "", theirs)
	var diff []string
	for p, kind := range theirs {
		if ours[p] != kind {
			diff = append(diff, "upstream has "+p+" of "+kind+", Lodia's "+ours[p])
		}
	}
	for p, kind := range ours {
		if _, ok := theirs[p]; !ok {
			diff = append(diff, "Lodia's alone has "+p+" of "+kind)
		}
	}
	slices.Sort(diff)
	for _, d := range diff {
		t.Error(d)
	}
	if len(theirs) != 22 {
		t.Errorf("upstream's JWTAuthenticator has %d paths, the design counted 22: %v", len(theirs), theirs)
	}
}

func strictly(t *testing.T, from, into any) {
	t.Helper()
	data, err := json.Marshal(from)
	if err != nil {
		t.Fatal(err)
	}
	strict, err := sigsjson.UnmarshalStrict(data, into)
	if err := errors.Join(append(strict, err)...); err != nil {
		t.Fatalf("%s does not decode strictly into %T: %v", data, into, err)
	}
}

func TestAFilledAuthenticatorSurvivesEitherTypeAndBack(t *testing.T) {
	for seed := range int64(200) {
		filler := randfill.NewWithSeed(seed).NilChance(0).NumElements(1, 3)

		var upstream apiserverv1.JWTAuthenticator
		filler.Fill(&upstream)
		var ours lodiav1alpha1.JWTAuthenticator
		strictly(t, upstream, &ours)
		var back apiserverv1.JWTAuthenticator
		strictly(t, ours, &back)
		if !equality.Semantic.DeepEqual(upstream, back) {
			t.Fatalf("seed %d: upstream's %+v comes back through Lodia's as %+v", seed, upstream, back)
		}

		var filled lodiav1alpha1.JWTAuthenticator
		filler.Fill(&filled)
		converted, err := authnconfig.Upstream([]lodiav1alpha1.JWTAuthenticator{filled})
		if err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		var again lodiav1alpha1.JWTAuthenticator
		strictly(t, converted[0], &again)
		if !equality.Semantic.DeepEqual(filled, again) {
			t.Fatalf("seed %d: Lodia's %+v comes back through upstream's as %+v", seed, filled, again)
		}
	}
}
