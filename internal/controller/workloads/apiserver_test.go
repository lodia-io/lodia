// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package workloads_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"maps"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilversion "k8s.io/apimachinery/pkg/util/version"
	apiserverv1beta1 "k8s.io/apiserver/pkg/apis/apiserver/v1beta1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/yaml"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/authnconfig"
	"go.lodia.io/lodia/internal/testenv"
)

type issuer struct {
	server *httptest.Server
	key    *ecdsa.PrivateKey
}

func encode(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

func newIssuer(t *testing.T) *issuer {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	is := &issuer{key: key}
	mux := http.NewServeMux()
	is.server = httptest.NewUnstartedServer(mux)
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"issuer":                                is.server.URL,
			"jwks_uri":                              is.server.URL + "/jwks",
			"id_token_signing_alg_values_supported": []string{"ES256"},
			"response_types_supported":              []string{"id_token"},
			"subject_types_supported":               []string{"public"},
			"authorization_endpoint":                is.server.URL + "/auth",
		})
	})
	mux.HandleFunc("/jwks", func(w http.ResponseWriter, _ *http.Request) {
		coordinate := func(i *big.Int) string { b := make([]byte, 32); i.FillBytes(b); return encode(b) }
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": []any{map[string]any{
			"kty": "EC", "crv": "P-256", "x": coordinate(key.X), "y": coordinate(key.Y), "kid": "k", "use": "sig", "alg": "ES256",
		}}})
	})
	is.server.StartTLS()
	t.Cleanup(is.server.Close)
	return is
}

func (is *issuer) token(t *testing.T, claims map[string]any) string {
	t.Helper()
	header, err := json.Marshal(map[string]any{"alg": "ES256", "kid": "k", "typ": "JWT"})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	all := map[string]any{"iss": is.server.URL, "aud": "lodia", "iat": now.Unix(), "exp": now.Add(10 * time.Minute).Unix()}
	maps.Copy(all, claims)
	payload, err := json.Marshal(all)
	if err != nil {
		t.Fatal(err)
	}
	signed := encode(header) + "." + encode(payload)
	sum := sha256.Sum256([]byte(signed))
	r, s, err := ecdsa.Sign(rand.Reader, is.key, sum[:])
	if err != nil {
		t.Fatal(err)
	}
	signature := make([]byte, 64)
	r.FillBytes(signature[:32])
	s.FillBytes(signature[32:])
	return signed + "." + encode(signature)
}

func (is *issuer) authenticators() []lodiav1alpha1.JWTAuthenticator {
	return []lodiav1alpha1.JWTAuthenticator{{
		Issuer: lodiav1alpha1.Issuer{
			URL:                  is.server.URL,
			CertificateAuthority: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: is.server.Certificate().Raw})),
			Audiences:            []string{"lodia"},
		},
		ClaimValidationRules: []lodiav1alpha1.ClaimValidationRule{{Expression: "claims.tenant == 'acme'", Message: "the token is not acme's"}},
		ClaimMappings: lodiav1alpha1.ClaimMappings{
			Username: lodiav1alpha1.PrefixedClaimOrExpression{Expression: "claims.sub"},
			Groups:   lodiav1alpha1.PrefixedClaimOrExpression{Expression: "dyn(claims.roles)"},
			UID:      lodiav1alpha1.ClaimOrExpression{Expression: "'uid-' + claims.sub"},
			Extra:    []lodiav1alpha1.ExtraMapping{{Key: "example.com/tenant", ValueExpression: "claims.tenant"}},
		},
		UserValidationRules: []lodiav1alpha1.UserValidationRule{{Expression: "!('suspended' in user.groups)", Message: "the user is suspended"}},
	}}
}

// tenantAPIServer is a kube-apiserver started on the file Workloads wrote,
// its cluster egress dialing directly, as konnectivity would from a node.
type tenantAPIServer struct {
	file   string
	config *rest.Config
	log    *syncBuffer
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func writeAtomically(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path+".tmp", data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path+".tmp", path); err != nil {
		t.Fatal(err)
	}
}

func startTenantAPIServer(t *testing.T, authentication []byte) (*tenantAPIServer, error) {
	t.Helper()
	dir := t.TempDir()
	s := &tenantAPIServer{file: filepath.Join(dir, "authentication-config.yaml"), log: &syncBuffer{}}
	writeAtomically(t, s.file, authentication)
	egress, err := yaml.Marshal(apiserverv1beta1.EgressSelectorConfiguration{
		TypeMeta:         metav1.TypeMeta{APIVersion: apiserverv1beta1.SchemeGroupVersion.String(), Kind: "EgressSelectorConfiguration"},
		EgressSelections: []apiserverv1beta1.EgressSelection{{Name: "cluster", Connection: apiserverv1beta1.Connection{ProxyProtocol: apiserverv1beta1.ProtocolDirect}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	egressFile := filepath.Join(dir, "egress-selector.yaml")
	writeAtomically(t, egressFile, egress)
	env := &envtest.Environment{UseExistingCluster: new(false)}
	api := env.ControlPlane.GetAPIServer()
	api.Err = s.log
	api.Configure().Set("authentication-config", s.file).Set("egress-selector-config-file", egressFile)
	s.config, err = env.Start()
	t.Cleanup(func() {
		if err := env.Stop(); err != nil {
			t.Error(err)
		}
	})
	return s, err
}

func (s *tenantAPIServer) failure() string {
	log := s.log.String()
	at := strings.Index(log, `"command failed"`)
	if at < 0 {
		return ""
	}
	failure, _, _ := strings.Cut(log[at:], "\n")
	if strings.HasSuffix(failure, "err=<") {
		failure, _, _ = strings.Cut(log[at:], "\n\t |")
	}
	return failure
}

func (s *tenantAPIServer) review(ctx context.Context, token string) (authenticationv1.UserInfo, error) {
	anonymous := rest.AnonymousClientConfig(s.config)
	anonymous.BearerToken = token
	c, err := kubernetes.NewForConfig(anonymous)
	if err != nil {
		return authenticationv1.UserInfo{}, err
	}
	r, err := c.AuthenticationV1().SelfSubjectReviews().Create(ctx, &authenticationv1.SelfSubjectReview{}, metav1.CreateOptions{})
	if err != nil {
		return authenticationv1.UserInfo{}, err
	}
	return r.Status.UserInfo, nil
}

func (s *tenantAPIServer) metric(t *testing.T, name string) string {
	t.Helper()
	c, err := kubernetes.NewForConfig(s.config)
	if err != nil {
		t.Fatal(err)
	}
	body, err := c.RESTClient().Get().AbsPath("/metrics").DoRaw(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	var lines []string
	for line := range strings.Lines(string(body)) {
		if strings.HasPrefix(line, name) {
			lines = append(lines, strings.TrimSpace(line))
		}
	}
	return strings.Join(lines, "\n")
}

func authenticationData(t *testing.T, cp *lodiav1alpha1.ControlPlane, holding string) []byte {
	t.Helper()
	var data string
	testenv.Eventually(t, func() error {
		data = authenticationConfig(t, cp).Data["authentication-config.yaml"]
		if !strings.Contains(data, holding) {
			return errors.New("the authentication configuration does not hold " + holding + ":\n" + data)
		}
		return nil
	})
	return []byte(data)
}

func envtestMinor(t *testing.T) string {
	t.Helper()
	out, err := exec.Command(filepath.Join(os.Getenv("KUBEBUILDER_ASSETS"), "kube-apiserver"), "--version").Output()
	if err != nil {
		t.Fatal(err)
	}
	v, err := utilversion.ParseGeneric(strings.TrimPrefix(strings.TrimSpace(string(out)), "Kubernetes "))
	if err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("%d.%d", v.Major(), v.Minor())
}

func TestARealAPIServerLoadsAndReloadsTheRenderedConfiguration(t *testing.T) {
	ctx := t.Context()
	first, second := newIssuer(t), newIssuer(t)
	cp := running(t, withJWT(first.authenticators()))
	hashed := hashes(t, cp)
	written := authenticationData(t, cp, first.server.URL)
	tenant, err := startTenantAPIServer(t, written)
	if err != nil {
		t.Fatalf("kube-apiserver %s does not start on the file Workloads wrote: %v\n%s", envtestMinor(t), err, tenant.log)
	}

	alice := map[string]any{"sub": "alice", "tenant": "acme", "roles": []string{"dev", "ops"}}
	var user authenticationv1.UserInfo
	testenv.Eventually(t, func() error {
		user, err = tenant.review(ctx, first.token(t, alice))
		return err
	})
	want := authenticationv1.UserInfo{Username: "alice", UID: "uid-alice", Groups: []string{"dev", "ops", "system:authenticated"}, Extra: map[string]authenticationv1.ExtraValue{"example.com/tenant": {"acme"}}}
	if fmt.Sprint(user) != fmt.Sprint(want) {
		t.Errorf("the first issuer's token is %+v, want %+v", user, want)
	}
	for name, claims := range map[string]map[string]any{
		"the claim rule":                    {"sub": "bob", "tenant": "other", "roles": []string{"dev"}},
		"the user rule":                     {"sub": "bob", "tenant": "acme", "roles": []string{"suspended"}},
		"the reserved names, a user":        {"sub": "system:admin", "tenant": "acme", "roles": []string{"dev"}},
		"the reserved names, a person's":    {"sub": "lodia:user:alice", "tenant": "acme", "roles": []string{"dev"}},
		"the reserved names, a group":       {"sub": "bob", "tenant": "acme", "roles": []string{"system:masters"}},
		"the reserved names, Lodia's group": {"sub": "bob", "tenant": "acme", "roles": []string{"lodia:cluster-admins"}},
	} {
		if refused, err := tenant.review(ctx, first.token(t, claims)); err == nil {
			t.Errorf("%s lets %+v in", name, refused)
		}
	}
	started := tenant.metric(t, "process_start_time_seconds")

	update(t, cp, withJWT(second.authenticators()))
	reloaded := authenticationData(t, cp, second.server.URL)
	writeAtomically(t, tenant.file, reloaded)
	began := time.Now()
	for {
		if _, err = tenant.review(ctx, second.token(t, alice)); err == nil || time.Since(began) > 3*time.Minute {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("the second issuer's token after the edit: %v\n%s", err, tenant.log)
	}
	t.Logf("the second issuer's token logs in %s after the file changed", time.Since(began).Round(100*time.Millisecond))
	if _, err := tenant.review(ctx, first.token(t, alice)); err == nil {
		t.Error("the first issuer's token logs in after the reload")
	}
	if now := tenant.metric(t, "process_start_time_seconds"); now != started {
		t.Errorf("the API server restarted: %s, then %s", started, now)
	}
	hash := fmt.Sprintf(`hash="sha256:%x"`, sha256.Sum256(reloaded))
	if info := tenant.metric(t, "apiserver_authentication_config_controller_last_config_info"); !strings.Contains(info, hash) {
		t.Errorf("the API server serves %s, want the ConfigMap's %s", info, hash)
	}
	if after := hashes(t, cp); fmt.Sprint(after) != fmt.Sprint(hashed) {
		t.Errorf("the restart hashes are %v, then %v", hashed, after)
	}
}

func TestWhatWouldStopKubeAPIServerNeverReachesTheConfigMap(t *testing.T) {
	is := newIssuer(t)
	cp := running(t, withJWT(is.authenticators()))
	written := authenticationData(t, cp, is.server.URL)
	minor := envtestMinor(t)

	unknown := regexp.MustCompile(`(?m)^jwt:$`).ReplaceAll(written, []byte("notAField: 1\njwt:"))
	if err := authnconfig.Validate(unknown, []string{minor}, nil, nil); err == nil || !strings.Contains(err.Error(), `unknown field "notAField"`) {
		t.Errorf("the file with an unknown field validates: %v", err)
	}
	tenant, err := startTenantAPIServer(t, unknown)
	if err == nil {
		t.Error("kube-apiserver starts on a file with an unknown field")
	} else if failure := tenant.failure(); !strings.Contains(failure, `strict decoding error: unknown field \"notAField\"`) {
		t.Errorf("kube-apiserver stops for another reason than the unknown field:\n%s", tenant.log)
	} else {
		t.Logf("kube-apiserver %s stops on an unknown field: %s", minor, failure)
	}

	newer := withJWT(is.authenticators())
	update(t, cp, func(s *lodiav1alpha1.ControlPlaneSpec) {
		newer(s)
		s.APIServer.Authentication.JWT[0].UserValidationRules = append(s.APIServer.Authentication.JWT[0].UserValidationRules, lodiav1alpha1.UserValidationRule{Expression: "!user.groups.includes('banned')"})
	})
	includes := authenticationData(t, cp, "includes")
	refused := authnconfig.Validate(includes, []string{minor}, nil, nil)
	tenant, err = startTenantAPIServer(t, includes)
	if supported := utilversion.MustParseGeneric(minor).AtLeast(utilversion.MajorMinor(1, 37)); supported {
		if err != nil || refused != nil {
			t.Errorf("kube-apiserver %s refuses includes: %v, %v", minor, err, refused)
		}
		return
	}
	if refused == nil || !strings.Contains(refused.Error(), "undeclared reference to 'includes'") {
		t.Errorf("a file with includes validates at %s: %v", minor, refused)
	}
	if err == nil {
		t.Errorf("kube-apiserver %s starts on a file with includes", minor)
	} else if failure := tenant.failure(); !strings.Contains(failure, "undeclared reference to 'includes'") {
		t.Errorf("kube-apiserver %s stops for another reason than includes:\n%s", minor, tenant.log)
	} else {
		t.Logf("kube-apiserver %s stops on includes: %s", minor, failure)
	}
}
