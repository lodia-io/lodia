// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package authnconfig

import (
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/apis/apiserver"
	"k8s.io/apiserver/pkg/apis/apiserver/install"
	"k8s.io/apiserver/pkg/apis/apiserver/validation"
	"k8s.io/apiserver/pkg/features"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
)

const (
	// shown is how many of a refusal's errors its message names.
	shown = 3
	// cut is the most of an error its message holds, in bytes.
	cut = 256
)

var codecs = func() serializer.CodecFactory {
	scheme := runtime.NewScheme()
	install.Install(scheme)
	return serializer.NewCodecFactory(scheme, serializer.EnableStrict)
}()

// Invalid is a file kube-apiserver of a minor refuses, the minor empty when it
// does not decode.
type Invalid struct {
	Minor  string
	Errors []string
}

func (i *Invalid) Error() string {
	var parts []string
	for _, e := range i.Errors[:min(len(i.Errors), shown)] {
		if len(e) > cut {
			e = strings.ToValidUTF8(e[:cut], "") + "…"
		}
		parts = append(parts, e)
	}
	if more := len(i.Errors) - shown; more > 0 {
		parts = append(parts, fmt.Sprintf("and %d more", more))
	}
	if i.Minor == "" {
		return "it does not decode: " + strings.Join(parts, "; ")
	}
	return "kube-apiserver " + i.Minor + " refuses it: " + strings.Join(parts, "; ")
}

// Validate decodes data as strictly as kube-apiserver decodes its
// --authentication-config, and runs upstream's validation of it for each of
// minors with the CEL libraries and the parser of that minor; no JWT
// authenticator may name one of issuers, the ServiceAccount issuers, and gates
// are the API server's --feature-gates. On the expressions of
// testdata/corpus.txt it agrees with each minor's own validation, but refuses
// duration() of an int, which 1.35 and 1.36 accept.
func Validate(data []byte, minors, issuers []string, gates map[string]bool) error {
	decoded, err := runtime.Decode(codecs.UniversalDecoder(), data)
	if err != nil {
		return &Invalid{Errors: []string{err.Error()}}
	}
	config, ok := decoded.(*apiserver.AuthenticationConfiguration)
	if !ok {
		return &Invalid{Errors: []string{fmt.Sprintf("it decodes as %T", decoded)}}
	}
	for _, minor := range minors {
		c, err := compilerOf(minor)
		if err != nil {
			return err
		}
		if errs := append(validation.ValidateAuthenticationConfiguration(c, config, issuers), egressGated(config, gates)...); len(errs) > 0 {
			invalid := &Invalid{Minor: minor}
			for _, e := range errs {
				invalid.Errors = append(invalid.Errors, e.Error())
			}
			return invalid
		}
	}
	return nil
}

// egressGated refuses an egress selector, as upstream's validation does, when
// gates turn StructuredAuthenticationConfigurationEgressSelector off: that
// validation reads the gate of the process it runs in, the operator's.
func egressGated(config *apiserver.AuthenticationConfiguration, gates map[string]bool) field.ErrorList {
	gate := features.StructuredAuthenticationConfigurationEgressSelector
	if on, set := gates[string(gate)]; !set || on || !utilfeature.DefaultFeatureGate.Enabled(gate) {
		return nil
	}
	var errs field.ErrorList
	for i, a := range config.JWT {
		if a.Issuer.EgressSelectorType != "" {
			errs = append(errs, field.Invalid(field.NewPath("jwt").Index(i).Child("issuer", "egressSelectorType"), a.Issuer.EgressSelectorType, "egress selector is not supported when "+string(gate)+" feature gate is disabled"))
		}
	}
	return errs
}
