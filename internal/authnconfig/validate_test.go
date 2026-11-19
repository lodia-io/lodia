// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package authnconfig

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	authenticationcel "k8s.io/apiserver/pkg/authentication/cel"

	"go.lodia.io/lodia/internal/versions"
)

func TestIncludesCompilesFrom137(t *testing.T) {
	for minor, want := range map[string]bool{"1.35": false, "1.36": false, "1.37": true} {
		c, err := compilerOf(minor)
		if err != nil {
			t.Fatal(err)
		}
		_, err = c.CompileUserExpression(&authenticationcel.UserValidationCondition{Expression: "!user.groups.includes('oidc:other')"})
		if (err == nil) != want {
			t.Errorf("includes at %s: %v, want compiled %t", minor, err, want)
		}
		_, err = c.CompileClaimsExpression(&authenticationcel.ClaimMappingExpression{Expression: "dyn(claims.roles).map(r, 'oidc:' + r)"})
		if err != nil {
			t.Errorf("map at %s: %v", minor, err)
		}
	}
}

const file = `apiVersion: apiserver.config.k8s.io/v1
kind: AuthenticationConfiguration
anonymous:
  enabled: true
  conditions:
  - path: /livez
jwt:
- issuer:
    url: https://login.example.com
    audiences: [lodia]
    egressSelectorType: cluster%s
  claimMappings:
    username: {expression: "'oidc:' + claims.sub"}
    groups: {expression: "dyn(claims.roles).map(r, 'oidc:' + r)"}
    uid: {expression: claims.sub}
    extra:
    - {key: example.com/tenant, valueExpression: claims.tenant}
  claimValidationRules:
  - {expression: "claims.tenant == 'acme'", message: not acme's}
  userValidationRules:
  - {expression: "%s", message: refused}
`

func TestValidateIsKubeAPIServersAtEveryMinor(t *testing.T) {
	const ok = "!('oidc:suspended' in user.groups)"
	all := versions.Minors()
	for _, c := range []struct {
		name, issuer, rule string
		minors             []string
		want               string
	}{
		{"the acceptance case", "", ok, all, ""},
		{"includes for every minor", "", "!user.groups.includes('x')", all, "kube-apiserver 1.35 refuses it: jwt[0].userValidationRules[0].expression: Invalid value: \"!user.groups.includes('x')\": compilation failed: ERROR: <input>:1:22: undeclared reference to 'includes'"},
		{"includes for a 1.36 tenant moving to 1.37", "", "!user.groups.includes('x')", []string{"1.36", "1.37"}, "kube-apiserver 1.36 refuses it"},
		{"includes for 1.37", "", "!user.groups.includes('x')", []string{"1.37"}, ""},
		{"a rule that is not a condition", "", "user.username", all, "must evaluate to bool"},
		{"the ServiceAccount issuer", "", ok, []string{"1.35"}, "jwt[0].issuer.url: Invalid value: \"https://login.example.com\": URL must not overlap with disallowed issuers: [https://login.example.com]"},
		{"an unknown field", "\n    notAField: 1", ok, all, `it does not decode: strict decoding error: unknown field "jwt[0].issuer.notAField"`},
		{"a CA that does not parse", "\n    certificateAuthority: not a certificate", ok, all, "jwt[0].issuer.certificateAuthority: Invalid value: \"<omitted>\": data does not contain any valid RSA or ECDSA certificates"},
	} {
		t.Run(c.name, func(t *testing.T) {
			var issuers []string
			if c.name == "the ServiceAccount issuer" {
				issuers = []string{"https://login.example.com"}
			}
			err := Validate(fmt.Appendf(nil, file, c.issuer, c.rule), c.minors, issuers)
			switch {
			case c.want == "" && err != nil:
				t.Fatalf("refused: %v", err)
			case c.want != "" && (err == nil || !strings.Contains(err.Error(), c.want)):
				t.Fatalf("got %v, want %q", err, c.want)
			}
			var invalid *Invalid
			if err != nil && !errors.As(err, &invalid) {
				t.Errorf("%v is not an *Invalid", err)
			}
		})
	}
}

func TestARefusalNamesItsFirstErrorsWithinBounds(t *testing.T) {
	long := strings.Repeat("é", 300)
	err := &Invalid{Minor: "1.35", Errors: []string{long, "b", "c", "d", "e"}}
	got := err.Error()
	want := "kube-apiserver 1.35 refuses it: " + strings.Repeat("é", 128) + "…; b; c; and 2 more"
	if got != want {
		t.Errorf("the message is %q, want %q", got, want)
	}
}
