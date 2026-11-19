// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"encoding/json"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	apiserverv1 "k8s.io/apiserver/pkg/apis/apiserver/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/authnconfig"
	"go.lodia.io/lodia/internal/versions"
)

func authenticators(t *testing.T, o *unstructured.Unstructured) []any {
	t.Helper()
	jwt, found, err := unstructured.NestedFieldNoCopy(o.Object, "spec", "apiServer", "authentication", "jwt")
	if err != nil || !found {
		t.Fatalf("the example has no jwt: %v", err)
	}
	return jwt.([]any)
}

func first(t *testing.T, o *unstructured.Unstructured) map[string]any {
	t.Helper()
	return authenticators(t, o)[0].(map[string]any)
}

func under(m map[string]any, path ...string) map[string]any {
	for _, p := range path {
		m = m[p].(map[string]any)
	}
	return m
}

func TestJWTAuthenticators(t *testing.T) {
	needsEnvtest(t)
	ensureNamespaces(t, "team-a")
	const (
		https       = "must be an https URL without a user, a query or a fragment"
		exactlyOne  = "set exactly one of claim and expression"
		atMostOne   = "set at most one of claim and expression"
		prefix      = "prefix is required with claim; it may be empty"
		noPrefix    = "prefix is set only with claim"
		domainPath  = "must be a lowercase domain-prefixed path, such as example.org/foo"
		reserved    = "k8s.io, kubernetes.io and their subdomains are reserved"
		gate        = "kube-apiserver refuses the egressSelectorType Lodia renders for every JWT authenticator without StructuredAuthenticationConfigurationEgressSelector"
		matchAny    = "audienceMatchPolicy must be MatchAny for multiple audiences"
		differ      = "discoveryURL must differ from url"
		issuerOnce  = "each issuer.url appears once"
		discoveryOn = "each issuer.discoveryURL appears once"
	)
	issuer := func(o map[string]any) map[string]any { return under(o, "issuer") }
	mappings := func(o map[string]any) map[string]any { return under(o, "claimMappings") }
	second := func(t *testing.T, o *unstructured.Unstructured, url string) map[string]any {
		copied := runtime.DeepCopyJSONValue(first(t, o)).(map[string]any)
		issuer(copied)["url"] = url
		set(t, o, append(authenticators(t, o), copied), "spec", "apiServer", "authentication", "jwt")
		return copied
	}
	for _, c := range []struct {
		name   string
		mutate func(*testing.T, *unstructured.Unstructured)
		want   string
	}{
		{"the acceptance case", func(*testing.T, *unstructured.Unstructured) {}, ""},
		{"an issuer reached through the control plane's network", func(t *testing.T, o *unstructured.Unstructured) {
			issuer(first(t, o))["egressSelectorType"] = "controlplane"
		}, `Unsupported value: "controlplane"`},
		{"an http issuer", func(t *testing.T, o *unstructured.Unstructured) {
			issuer(first(t, o))["url"] = "http://login.example.com"
		}, https},
		{"an issuer with a query", func(t *testing.T, o *unstructured.Unstructured) {
			issuer(first(t, o))["url"] = "https://login.example.com/?realm=a"
		}, https},
		{"an issuer with a fragment", func(t *testing.T, o *unstructured.Unstructured) {
			issuer(first(t, o))["url"] = "https://login.example.com/#a"
		}, https},
		{"an issuer with a user", func(t *testing.T, o *unstructured.Unstructured) {
			issuer(first(t, o))["url"] = "https://alice@login.example.com"
		}, https},
		{"an issuer with a user and a port", func(t *testing.T, o *unstructured.Unstructured) {
			issuer(first(t, o))["url"] = "HTTPS://alice:secret@login.example.com:8443/realms/team-a"
		}, https},
		{"an issuer with a long s in its scheme", func(t *testing.T, o *unstructured.Unstructured) {
			issuer(first(t, o))["url"] = "httpſ://login.example.com/realms/team-a"
		}, https},
		{"an issuer with a long s in its upper-case scheme", func(t *testing.T, o *unstructured.Unstructured) {
			issuer(first(t, o))["url"] = "HTTPſ://login.example.com/realms/team-a"
		}, https},
		{"a discovery URL with a user", func(t *testing.T, o *unstructured.Unstructured) {
			issuer(first(t, o))["discoveryURL"] = "https://alice@10.96.0.50/.well-known/openid-configuration"
		}, https},
		{"a discovery URL with a query", func(t *testing.T, o *unstructured.Unstructured) {
			issuer(first(t, o))["discoveryURL"] = "https://10.96.0.50/.well-known/openid-configuration?realm=a"
		}, https},
		{"a discovery URL with a fragment", func(t *testing.T, o *unstructured.Unstructured) {
			issuer(first(t, o))["discoveryURL"] = "https://10.96.0.50/.well-known/openid-configuration#a"
		}, https},
		{"a discovery URL with a long s in its scheme", func(t *testing.T, o *unstructured.Unstructured) {
			issuer(first(t, o))["discoveryURL"] = "httpſ://10.96.0.50/.well-known/openid-configuration"
		}, https},
		{"an https issuer without a host", func(t *testing.T, o *unstructured.Unstructured) {
			issuer(first(t, o))["url"] = "https:login.example.com"
		}, https},
		{"an http discovery URL", func(t *testing.T, o *unstructured.Unstructured) {
			issuer(first(t, o))["discoveryURL"] = "http://10.96.0.50/.well-known/openid-configuration"
		}, https},
		{"a discovery URL", func(t *testing.T, o *unstructured.Unstructured) {
			issuer(first(t, o))["discoveryURL"] = "https://10.96.0.50/.well-known/openid-configuration"
		}, ""},
		{"a discovery URL equal to the issuer", func(t *testing.T, o *unstructured.Unstructured) {
			issuer(first(t, o))["discoveryURL"] = issuer(first(t, o))["url"]
		}, differ},
		{"two audiences without a policy", func(t *testing.T, o *unstructured.Unstructured) {
			issuer(first(t, o))["audiences"] = []any{"a", "b"}
		}, matchAny},
		{"two audiences with an empty policy", func(t *testing.T, o *unstructured.Unstructured) {
			issuer(first(t, o))["audiences"] = []any{"a", "b"}
			issuer(first(t, o))["audienceMatchPolicy"] = ""
		}, matchAny},
		{"an empty audience", func(t *testing.T, o *unstructured.Unstructured) {
			issuer(first(t, o))["audiences"] = []any{""}
		}, "should be at least 1 chars long"},
		{"two audiences that match any", func(t *testing.T, o *unstructured.Unstructured) {
			issuer(first(t, o))["audiences"] = []any{"a", "b"}
			issuer(first(t, o))["audienceMatchPolicy"] = "MatchAny"
		}, ""},
		{"an audience twice", func(t *testing.T, o *unstructured.Unstructured) {
			issuer(first(t, o))["audiences"] = []any{"a", "a"}
			issuer(first(t, o))["audienceMatchPolicy"] = "MatchAny"
		}, "Duplicate value"},
		{"an issuer twice", func(t *testing.T, o *unstructured.Unstructured) {
			second(t, o, issuer(first(t, o))["url"].(string))
		}, issuerOnce},
		{"two issuers", func(t *testing.T, o *unstructured.Unstructured) {
			second(t, o, "https://login.example.com/realms/team-b")
		}, ""},
		{"a discovery URL twice", func(t *testing.T, o *unstructured.Unstructured) {
			issuer(first(t, o))["discoveryURL"] = "https://10.96.0.50/.well-known/openid-configuration"
			second(t, o, "https://login.example.com/realms/team-b")
		}, discoveryOn},
		{"nine authenticators", func(t *testing.T, o *unstructured.Unstructured) {
			for _, realm := range []string{"b", "c", "d", "e", "f", "g", "h", "i"} {
				second(t, o, "https://login.example.com/realms/"+realm)
			}
		}, "must have at most 8 items"},
		{"a claim rule with both", func(t *testing.T, o *unstructured.Unstructured) {
			first(t, o)["claimValidationRules"] = []any{map[string]any{"claim": "hd", "expression": "true"}}
		}, exactlyOne},
		{"a claim rule with neither", func(t *testing.T, o *unstructured.Unstructured) {
			first(t, o)["claimValidationRules"] = []any{map[string]any{"message": "m"}}
		}, exactlyOne},
		{"a required claim", func(t *testing.T, o *unstructured.Unstructured) {
			first(t, o)["claimValidationRules"] = []any{map[string]any{"claim": "hd", "requiredValue": "example.com"}}
		}, ""},
		{"a required claim with a message", func(t *testing.T, o *unstructured.Unstructured) {
			first(t, o)["claimValidationRules"] = []any{map[string]any{"claim": "hd", "message": "m"}}
		}, "message is set only with expression"},
		{"an expression with a required value", func(t *testing.T, o *unstructured.Unstructured) {
			first(t, o)["claimValidationRules"] = []any{map[string]any{"expression": "true", "requiredValue": "v"}}
		}, "requiredValue is set only with claim"},
		{"a claim required twice", func(t *testing.T, o *unstructured.Unstructured) {
			first(t, o)["claimValidationRules"] = []any{map[string]any{"claim": "hd"}, map[string]any{"claim": "hd", "requiredValue": "v"}}
		}, "each claim appears once"},
		{"an expression twice", func(t *testing.T, o *unstructured.Unstructured) {
			first(t, o)["claimValidationRules"] = []any{map[string]any{"expression": "true"}, map[string]any{"expression": "true", "message": "m"}}
		}, "each expression appears once"},
		{"a username with both", func(t *testing.T, o *unstructured.Unstructured) {
			mappings(first(t, o))["username"] = map[string]any{"claim": "sub", "prefix": "", "expression": "claims.sub"}
		}, exactlyOne},
		{"a username with neither", func(t *testing.T, o *unstructured.Unstructured) {
			mappings(first(t, o))["username"] = map[string]any{}
		}, exactlyOne},
		{"a username claim without a prefix", func(t *testing.T, o *unstructured.Unstructured) {
			mappings(first(t, o))["username"] = map[string]any{"claim": "sub"}
		}, prefix},
		{"a username claim with an empty prefix", func(t *testing.T, o *unstructured.Unstructured) {
			mappings(first(t, o))["username"] = map[string]any{"claim": "sub", "prefix": ""}
		}, ""},
		{"a username expression with a prefix", func(t *testing.T, o *unstructured.Unstructured) {
			mappings(first(t, o))["username"] = map[string]any{"expression": "claims.sub", "prefix": "oidc:"}
		}, noPrefix},
		{"groups with both", func(t *testing.T, o *unstructured.Unstructured) {
			mappings(first(t, o))["groups"] = map[string]any{"claim": "roles", "prefix": "", "expression": "claims.roles"}
		}, atMostOne},
		{"a groups claim without a prefix", func(t *testing.T, o *unstructured.Unstructured) {
			mappings(first(t, o))["groups"] = map[string]any{"claim": "roles"}
		}, prefix},
		{"a uid with both", func(t *testing.T, o *unstructured.Unstructured) {
			mappings(first(t, o))["uid"] = map[string]any{"claim": "sub", "expression": "claims.sub"}
		}, atMostOne},
		{"a uid claim", func(t *testing.T, o *unstructured.Unstructured) {
			mappings(first(t, o))["uid"] = map[string]any{"claim": "sub"}
		}, ""},
		{"an extra key without a domain", func(t *testing.T, o *unstructured.Unstructured) {
			mappings(first(t, o))["extra"] = []any{map[string]any{"key": "tenant", "valueExpression": "claims.tenant"}}
		}, domainPath},
		{"an extra key in capitals", func(t *testing.T, o *unstructured.Unstructured) {
			mappings(first(t, o))["extra"] = []any{map[string]any{"key": "example.com/Tenant", "valueExpression": "claims.tenant"}}
		}, domainPath},
		{"an extra key with a space", func(t *testing.T, o *unstructured.Unstructured) {
			mappings(first(t, o))["extra"] = []any{map[string]any{"key": "example.com/a b", "valueExpression": "claims.tenant"}}
		}, domainPath},
		{"an extra key of every character a path may hold", func(t *testing.T, o *unstructured.Unstructured) {
			mappings(first(t, o))["extra"] = []any{map[string]any{"key": "a-1.example.com/a/-._~%!$&'()*+,;=:", "valueExpression": "claims.tenant"}}
		}, ""},
		{"an extra key of kubernetes.io", func(t *testing.T, o *unstructured.Unstructured) {
			mappings(first(t, o))["extra"] = []any{map[string]any{"key": "kubernetes.io/tenant", "valueExpression": "claims.tenant"}}
		}, reserved},
		{"an extra key of a subdomain of k8s.io", func(t *testing.T, o *unstructured.Unstructured) {
			mappings(first(t, o))["extra"] = []any{map[string]any{"key": "authn.k8s.io/tenant", "valueExpression": "claims.tenant"}}
		}, reserved},
		{"an extra key of a domain that ends in k8s.io", func(t *testing.T, o *unstructured.Unstructured) {
			mappings(first(t, o))["extra"] = []any{map[string]any{"key": "myk8s.io/tenant", "valueExpression": "claims.tenant"}}
		}, ""},
		{"an extra key twice", func(t *testing.T, o *unstructured.Unstructured) {
			mappings(first(t, o))["extra"] = []any{map[string]any{"key": "example.com/a", "valueExpression": "claims.a"}, map[string]any{"key": "example.com/a", "valueExpression": "claims.b"}}
		}, "Duplicate value"},
		{"a user rule twice", func(t *testing.T, o *unstructured.Unstructured) {
			first(t, o)["userValidationRules"] = []any{map[string]any{"expression": "true"}, map[string]any{"expression": "true", "message": "m"}}
		}, "Duplicate value"},
		{"the ServiceAccount issuer as an issuer", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, issuer(first(t, o))["url"], "spec", "serviceAccount", "issuer")
		}, "kube-apiserver refuses a JWT issuer.url equal to serviceAccount.issuer"},
		{"the ServiceAccount issuer as the second issuer", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, second(t, o, "https://login.example.com/realms/team-b")["issuer"].(map[string]any)["url"], "spec", "serviceAccount", "issuer")
		}, "kube-apiserver refuses a JWT issuer.url equal to serviceAccount.issuer"},
		{"the egress selector's gate off", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, map[string]any{"StructuredAuthenticationConfigurationEgressSelector": false}, "spec", "apiServer", "featureGates")
		}, gate},
		{"the egress selector's gate on", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, map[string]any{"StructuredAuthenticationConfigurationEgressSelector": true}, "spec", "apiServer", "featureGates")
		}, ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			obj := example(t, "controlplane-oidc.yaml", "jwt")
			c.mutate(t, obj)
			expect(t, k8s.Create(context.Background(), obj, client.DryRunAll, client.FieldValidation("Strict")), c.want)
		})
	}
}

func TestTheSchemaDefaultsTheIssuersEgressToCluster(t *testing.T) {
	needsEnvtest(t)
	ensureNamespaces(t, "team-a")
	obj := example(t, "controlplane-oidc.yaml", "egress")
	if _, found := issuerOf(t, obj)["egressSelectorType"]; found {
		t.Fatal("the example sets egressSelectorType")
	}
	expect(t, k8s.Create(context.Background(), obj, client.DryRunAll), "")
	if got := issuerOf(t, obj)["egressSelectorType"]; got != "cluster" {
		t.Errorf("egressSelectorType is stored as %v, want cluster", got)
	}
}

func issuerOf(t *testing.T, o *unstructured.Unstructured) map[string]any {
	t.Helper()
	return under(first(t, o), "issuer")
}

func TestTheSchemaAcceptsWhatKubeAPIServerOfEveryMinorAccepts(t *testing.T) {
	needsEnvtest(t)
	ensureNamespaces(t, "team-a")
	issuer := func(o map[string]any) map[string]any { return under(o, "issuer") }
	for _, c := range []struct {
		name   string
		mutate func(*testing.T, *unstructured.Unstructured)
	}{
		{"an upper-case scheme", func(t *testing.T, o *unstructured.Unstructured) {
			issuer(first(t, o))["url"] = "HTTPS://login.example.com/realms/team-a"
		}},
		{"an @ in the issuer's path", func(t *testing.T, o *unstructured.Unstructured) {
			issuer(first(t, o))["url"] = "https://login.example.com/realms/team-a@acme"
		}},
		{"an empty query", func(t *testing.T, o *unstructured.Unstructured) {
			issuer(first(t, o))["url"] = "https://login.example.com/realms/team-a?"
		}},
		{"an empty fragment", func(t *testing.T, o *unstructured.Unstructured) {
			issuer(first(t, o))["url"] = "https://login.example.com/realms/team-a#"
		}},
		{"an upper-case scheme in the discovery URL", func(t *testing.T, o *unstructured.Unstructured) {
			issuer(first(t, o))["discoveryURL"] = "HTTPS://10.96.0.50/.well-known/openid-configuration"
		}},
		{"an @ in the discovery URL's path", func(t *testing.T, o *unstructured.Unstructured) {
			issuer(first(t, o))["discoveryURL"] = "https://10.96.0.50/realms/team-a@acme/.well-known/openid-configuration"
		}},
		{"an empty discoveryURL", func(t *testing.T, o *unstructured.Unstructured) {
			issuer(first(t, o))["discoveryURL"] = ""
		}},
		{"two empty discoveryURLs", func(t *testing.T, o *unstructured.Unstructured) {
			issuer(first(t, o))["discoveryURL"] = ""
			copied := runtime.DeepCopyJSONValue(first(t, o)).(map[string]any)
			issuer(copied)["url"] = "https://login.example.com/realms/team-b"
			set(t, o, append(authenticators(t, o), copied), "spec", "apiServer", "authentication", "jwt")
		}},
		{"an empty audienceMatchPolicy", func(t *testing.T, o *unstructured.Unstructured) {
			issuer(first(t, o))["audienceMatchPolicy"] = ""
		}},
		{"an empty egressSelectorType", func(t *testing.T, o *unstructured.Unstructured) {
			issuer(first(t, o))["egressSelectorType"] = ""
		}},
		{"a required claim with an empty message", func(t *testing.T, o *unstructured.Unstructured) {
			first(t, o)["claimValidationRules"] = []any{map[string]any{"claim": "hd", "message": ""}}
		}},
		{"an expression with an empty requiredValue", func(t *testing.T, o *unstructured.Unstructured) {
			first(t, o)["claimValidationRules"] = []any{map[string]any{"expression": "true", "requiredValue": ""}}
		}},
		{"the ServiceAccount issuer with a slash more than an issuer", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, issuer(first(t, o))["url"].(string)+"/", "spec", "serviceAccount", "issuer")
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			obj := example(t, "controlplane-oidc.yaml", "upstream")
			c.mutate(t, obj)
			expect(t, k8s.Create(context.Background(), obj, client.DryRunAll, client.FieldValidation("Strict")), "")
			data, err := json.Marshal(authenticators(t, obj))
			if err != nil {
				t.Fatal(err)
			}
			var jwt []lodiav1alpha1.JWTAuthenticator
			if err := json.Unmarshal(data, &jwt); err != nil {
				t.Fatal(err)
			}
			upstream, err := authnconfig.Upstream(jwt)
			if err != nil {
				t.Fatal(err)
			}
			for i := range upstream {
				upstream[i].Issuer.EgressSelectorType = apiserverv1.EgressSelectorCluster
			}
			file, err := yaml.Marshal(apiserverv1.AuthenticationConfiguration{
				TypeMeta: metav1.TypeMeta{APIVersion: apiserverv1.SchemeGroupVersion.String(), Kind: "AuthenticationConfiguration"},
				JWT:      upstream,
			})
			if err != nil {
				t.Fatal(err)
			}
			var issuers []string
			if sa, found, _ := unstructured.NestedString(obj.Object, "spec", "serviceAccount", "issuer"); found {
				issuers = []string{sa}
			}
			if err := authnconfig.Validate(file, versions.Minors(), issuers, nil); err != nil {
				t.Errorf("kube-apiserver's validation refuses it: %v", err)
			}
		})
	}
}
