// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

// Command kubeauthn checks authentication configurations as kube-apiserver of
// the k8s.io/apiserver it is built with checks its --authentication-config: a
// strict decode, then upstream's validation with upstream's default compiler
// (pkg/kubeapiserver/options/authentication.go). It reads a JSON array of
// files on standard input and writes, as JSON, the versions it links and the
// error of each file, empty when the file passes. Each minor's dependencies
// are <minor>.mod and <minor>.sum, built with -modfile; go.mod only marks the
// module's root.
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"runtime/debug"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apiserver/pkg/apis/apiserver"
	"k8s.io/apiserver/pkg/apis/apiserver/install"
	"k8s.io/apiserver/pkg/apis/apiserver/validation"
	authenticationcel "k8s.io/apiserver/pkg/authentication/cel"
)

type report struct {
	Versions map[string]string `json:"versions"`
	Errors   []string          `json:"errors"`
}

func main() {
	var files []string
	if err := json.NewDecoder(os.Stdin).Decode(&files); err != nil {
		log.Fatal(err)
	}
	scheme := runtime.NewScheme()
	install.Install(scheme)
	decoder := serializer.NewCodecFactory(scheme, serializer.EnableStrict).UniversalDecoder()
	compiler := authenticationcel.NewDefaultCompiler()
	out := report{Versions: map[string]string{}}
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, m := range info.Deps {
			out.Versions[m.Path] = m.Version
		}
	}
	for _, file := range files {
		out.Errors = append(out.Errors, check(decoder, compiler, []byte(file)))
	}
	if err := json.NewEncoder(os.Stdout).Encode(out); err != nil {
		log.Fatal(err)
	}
}

func check(decoder runtime.Decoder, compiler authenticationcel.Compiler, data []byte) string {
	decoded, err := runtime.Decode(decoder, data)
	if err != nil {
		return err.Error()
	}
	config, ok := decoded.(*apiserver.AuthenticationConfiguration)
	if !ok {
		return fmt.Sprintf("expected AuthenticationConfiguration, got %T", decoded)
	}
	if err := validation.ValidateAuthenticationConfiguration(compiler, config, nil).ToAggregate(); err != nil {
		return err.Error()
	}
	return ""
}
