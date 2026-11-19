// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package authnconfig

import (
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apiserver/pkg/apis/apiserver"
	"k8s.io/apiserver/pkg/apis/apiserver/install"
	"k8s.io/apiserver/pkg/apis/apiserver/validation"
	authenticationcel "k8s.io/apiserver/pkg/authentication/cel"
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
// --authentication-config: decoded as strictly, then validated by upstream's
// validation with the default compiler and the operator's feature gates, no
// JWT authenticator naming one of issuers, the ServiceAccount issuers.
func Validate(data []byte, issuers []string) error {
	decoded, err := runtime.Decode(codecs.UniversalDecoder(), data)
	if err != nil {
		return &Invalid{Errors: []string{err.Error()}}
	}
	config, ok := decoded.(*apiserver.AuthenticationConfiguration)
	if !ok {
		return &Invalid{Errors: []string{fmt.Sprintf("it decodes as %T", decoded)}}
	}
	errs := validation.ValidateAuthenticationConfiguration(authenticationcel.NewDefaultCompiler(), config, issuers)
	if len(errs) == 0 {
		return nil
	}
	invalid := &Invalid{Minor: minor}
	for _, e := range errs {
		invalid.Errors = append(invalid.Errors, e.Error())
	}
	return invalid
}
