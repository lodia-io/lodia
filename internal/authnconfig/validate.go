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
// --authentication-config, and validates it as the API server of each of
// minors does at its start and at a reload, with the CEL libraries of that
// minor; no JWT authenticator may name one of issuers, the ServiceAccount
// issuers.
func Validate(data []byte, minors, issuers []string) error {
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
		if errs := validation.ValidateAuthenticationConfiguration(c, config, issuers); len(errs) > 0 {
			invalid := &Invalid{Minor: minor}
			for _, e := range errs {
				invalid.Errors = append(invalid.Errors, e.Error())
			}
			return invalid
		}
	}
	return nil
}
