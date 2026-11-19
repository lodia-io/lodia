// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package authnconfig

import (
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/util/version"

	"go.lodia.io/lodia/internal/versions"
)

func TestValidateIsKubeAPIServerOfTheNewestMinorLodiaRuns(t *testing.T) {
	out, err := exec.Command("go", "list", "-m", "-f", "{{.Version}}", "k8s.io/apiserver").Output()
	if err != nil {
		t.Fatal(err)
	}
	linked, err := version.ParseSemantic(strings.TrimSpace(string(out)))
	if err != nil {
		t.Fatal(err)
	}
	all := versions.Minors()
	if newest := all[len(all)-1]; fmt.Sprintf("1.%d", linked.Minor()) != newest || minor != newest {
		t.Errorf("the operator links k8s.io/apiserver %s and its refusals name kube-apiserver %s; the newest minor Lodia runs is %s", linked, minor, newest)
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

func TestValidateIsKubeAPIServersLoad(t *testing.T) {
	const ok = "!('oidc:suspended' in user.groups)"
	for _, c := range []struct {
		name, issuer, rule string
		want               string
	}{
		{"the acceptance case", "", ok, ""},
		{"includes", "", "!user.groups.includes('x')", ""},
		{"a rule that is not a condition", "", "user.username", "kube-apiserver 1.37 refuses it: jwt[0].userValidationRules[0].expression: Invalid value: \"user.username\": must evaluate to bool"},
		{"the ServiceAccount issuer", "", ok, "jwt[0].issuer.url: Invalid value: \"https://login.example.com\": URL must not overlap with disallowed issuers: [https://login.example.com]"},
		{"an unknown field", "\n    notAField: 1", ok, `it does not decode: strict decoding error: unknown field "jwt[0].issuer.notAField"`},
		{"a CA that does not parse", "\n    certificateAuthority: not a certificate", ok, "jwt[0].issuer.certificateAuthority: Invalid value: \"<omitted>\": data does not contain any valid RSA or ECDSA certificates"},
	} {
		t.Run(c.name, func(t *testing.T) {
			var issuers []string
			if c.name == "the ServiceAccount issuer" {
				issuers = []string{"https://login.example.com"}
			}
			err := Validate(fmt.Appendf(nil, file, c.issuer, c.rule), issuers)
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
