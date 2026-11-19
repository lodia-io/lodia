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
	authenticationcel "k8s.io/apiserver/pkg/authentication/cel"
	"k8s.io/apiserver/pkg/server/egressselector"
)

const (
	// minor is the Kubernetes minor of the k8s.io/apiserver the operator links.
	minor = "1.37"
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

// Validate loads data as kube-apiserver of minor loads its
// --authentication-config beside the --egress-selector-config-file egress:
// decoded as strictly, then validated by upstream's validation with the
// default compiler and the operator's feature gates, no JWT authenticator
// naming one of issuers, the ServiceAccount issuers, and then each
// authenticator's egress looked up as kube-apiserver builds it. An egress that
// does not decode is an error, not a refusal of data.
func Validate(data []byte, issuers []string, egress []byte) error {
	lookup, err := egressLookup(egress)
	if err != nil {
		return err
	}
	config, err := Decode(data)
	if err != nil {
		return err
	}
	errs := validation.ValidateAuthenticationConfiguration(authenticationcel.NewDefaultCompiler(), config, issuers)
	for i, a := range config.JWT {
		if refused := undialed(lookup, a.Issuer.EgressSelectorType); refused != "" {
			errs = append(errs, field.Invalid(field.NewPath("jwt").Index(i).Child("issuer", "egressSelectorType"), a.Issuer.EgressSelectorType, refused))
		}
	}
	if len(errs) == 0 {
		return nil
	}
	invalid := &Invalid{Minor: minor}
	for _, e := range errs {
		invalid.Errors = append(invalid.Errors, e.Error())
	}
	return invalid
}

// Decode is data as kube-apiserver decodes its --authentication-config, or an
// *Invalid without a minor.
func Decode(data []byte) (*apiserver.AuthenticationConfiguration, error) {
	decoded, err := runtime.Decode(codecs.UniversalDecoder(), data)
	if err != nil {
		return nil, &Invalid{Errors: []string{err.Error()}}
	}
	config, ok := decoded.(*apiserver.AuthenticationConfiguration)
	if !ok {
		return nil, &Invalid{Errors: []string{fmt.Sprintf("it decodes as %T", decoded)}}
	}
	return config, nil
}

// egressLookup is the lookup kube-apiserver gives its JWT authenticators for
// the egress selector configuration egress, nil when it names no selection
// (kubernetes@v1.37.1 pkg/kubeapiserver/options/authentication.go#L737-L744).
func egressLookup(egress []byte) (egressselector.Lookup, error) {
	decoded, err := runtime.Decode(codecs.UniversalDecoder(), egress)
	if err != nil {
		return nil, fmt.Errorf("decode the egress selector configuration: %w", err)
	}
	config, ok := decoded.(*apiserver.EgressSelectorConfiguration)
	if !ok {
		return nil, fmt.Errorf("the egress selector configuration decodes as %T", decoded)
	}
	selector, err := egressselector.NewEgressSelector(config)
	if selector == nil || err != nil {
		return nil, err
	}
	return selector.Lookup, nil
}

// undialed is why kube-apiserver refuses to build a JWT authenticator whose
// egress selector type lookup has no dialer for, empty when it has one or the
// type names none (k8s.io/apiserver@v0.37.1
// plugin/pkg/authenticator/token/oidc/oidc.go#L316-L330, #L473-L487).
func undialed(lookup egressselector.Lookup, selectorType apiserver.EgressSelectorType) string {
	var egress egressselector.EgressType
	switch selectorType {
	case apiserver.EgressSelectorControlPlane:
		egress = egressselector.ControlPlane
	case apiserver.EgressSelectorCluster:
		egress = egressselector.Cluster
	default:
		return ""
	}
	if lookup == nil {
		return fmt.Sprintf("egress lookup required with egress selector type %q", egress)
	}
	dial, err := lookup(egress.AsNetworkContext())
	switch {
	case err != nil:
		return fmt.Sprintf("egress lookup for %q failed: %v", egress, err)
	case dial == nil:
		return fmt.Sprintf("egress lookup for %q is not configured", egress)
	}
	return ""
}
