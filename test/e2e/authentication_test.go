// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

//go:build e2e

package e2e

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"maps"
	"math/big"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/distribution/reference"
	appsv1 "k8s.io/api/apps/v1"
	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/certs"
)

const (
	authnTenant = "team-authn"
	authnClass  = "e2e-authn"
	oidcIssuer  = "oidcissuer"
	oidcHost    = oidcIssuer + "." + oidcIssuer + ".svc"
)

// testIssuers are two OIDC issuers at two paths of one Service in the
// tenant, each with a key of the test's.
type testIssuers struct {
	ca   *certs.Certificate
	keys map[string]*ecdsa.PrivateKey
}

func newTestIssuers(t *testing.T) *testIssuers {
	t.Helper()
	ca, err := certs.NewCA("oidc-e2e", time.Now(), 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	is := &testIssuers{ca: ca, keys: map[string]*ecdsa.PrivateKey{}}
	for _, path := range []string{"first", "second"} {
		if is.keys[path], err = ecdsa.GenerateKey(elliptic.P256(), rand.Reader); err != nil {
			t.Fatal(err)
		}
	}
	return is
}

func issuerURL(path string) string { return "https://" + oidcHost + "/" + path }

func base64URL(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

// files are the discovery documents and keys the issuer serves, by the key of
// the ConfigMap that holds them and the path it is mounted at.
func (is *testIssuers) files(t *testing.T) (map[string]string, []corev1.KeyToPath) {
	t.Helper()
	data := map[string]string{}
	var items []corev1.KeyToPath
	for path, key := range is.keys {
		discovery, err := json.Marshal(map[string]any{
			"issuer":                                issuerURL(path),
			"jwks_uri":                              issuerURL(path) + "/jwks",
			"authorization_endpoint":                issuerURL(path) + "/auth",
			"id_token_signing_alg_values_supported": []string{"ES256"},
			"response_types_supported":              []string{"id_token"},
			"subject_types_supported":               []string{"public"},
		})
		if err != nil {
			t.Fatal(err)
		}
		coordinate := func(i *big.Int) string { b := make([]byte, 32); i.FillBytes(b); return base64URL(b) }
		jwks, err := json.Marshal(map[string]any{"keys": []any{map[string]any{
			"kty": "EC", "crv": "P-256", "x": coordinate(key.X), "y": coordinate(key.Y), "kid": path, "use": "sig", "alg": "ES256",
		}}})
		if err != nil {
			t.Fatal(err)
		}
		data[path+"-discovery"], data[path+"-jwks"] = string(discovery), string(jwks)
		items = append(items,
			corev1.KeyToPath{Key: path + "-discovery", Path: path + "/.well-known/openid-configuration"},
			corev1.KeyToPath{Key: path + "-jwks", Path: path + "/jwks"})
	}
	return data, items
}

func (is *testIssuers) token(t *testing.T, path string, claims map[string]any) string {
	t.Helper()
	header, err := json.Marshal(map[string]any{"alg": "ES256", "kid": path, "typ": "JWT"})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	all := map[string]any{"iss": issuerURL(path), "aud": "lodia", "iat": now.Unix(), "exp": now.Add(10 * time.Minute).Unix()}
	maps.Copy(all, claims)
	payload, err := json.Marshal(all)
	if err != nil {
		t.Fatal(err)
	}
	signed := base64URL(header) + "." + base64URL(payload)
	sum := sha256.Sum256([]byte(signed))
	r, s, err := ecdsa.Sign(rand.Reader, is.keys[path], sum[:])
	if err != nil {
		t.Fatal(err)
	}
	signature := make([]byte, 64)
	r.FillBytes(signature[:32])
	s.FillBytes(signature[32:])
	return signed + "." + base64URL(signature)
}

func (is *testIssuers) authenticators(t *testing.T, path string) []lodiav1alpha1.JWTAuthenticator {
	t.Helper()
	ca, err := certs.CertPEM(is.ca.Cert)
	if err != nil {
		t.Fatal(err)
	}
	return []lodiav1alpha1.JWTAuthenticator{{
		Issuer:               lodiav1alpha1.Issuer{URL: issuerURL(path), CertificateAuthority: string(ca), Audiences: []string{"lodia"}},
		ClaimValidationRules: []lodiav1alpha1.ClaimValidationRule{{Expression: "claims.tenant == 'acme'", Message: "the token is not acme's"}},
		ClaimMappings: lodiav1alpha1.ClaimMappings{
			Username: lodiav1alpha1.PrefixedClaimOrExpression{Expression: "'oidc:' + claims.sub"},
			Groups:   lodiav1alpha1.PrefixedClaimOrExpression{Expression: "dyn(claims.roles).map(r, 'oidc:' + r)"},
			UID:      lodiav1alpha1.ClaimOrExpression{Expression: "claims.sub"},
			Extra:    []lodiav1alpha1.ExtraMapping{{Key: "example.com/tenant", ValueExpression: "claims.tenant"}},
		},
		UserValidationRules: []lodiav1alpha1.UserValidationRule{{Expression: "!('oidc:suspended' in user.groups)", Message: "the user is suspended"}},
	}}
}

// deploy runs the issuer in the tenant, on node, behind the Service
// oidcissuer.oidcissuer.
func (is *testIssuers) deploy(ctx context.Context, t *testing.T, access *tenantAccess, image reference.NamedTagged, node string) {
	t.Helper()
	serving, err := is.ca.Issue(certs.Profile{CommonName: oidcHost, DNSNames: []string{oidcHost}, Usages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}, time.Now(), 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := certs.CertPEM(serving.Cert)
	if err != nil {
		t.Fatal(err)
	}
	key, err := certs.KeyPEM(serving.Key)
	if err != nil {
		t.Fatal(err)
	}
	data, items := is.files(t)
	labels := map[string]string{"app": oidcIssuer}
	create(ctx, t, access.client, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: oidcIssuer}})
	create(ctx, t, access.client, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: oidcIssuer, Name: "tls"},
		Type:       corev1.SecretTypeTLS,
		Data:       map[string][]byte{corev1.TLSCertKey: cert, corev1.TLSPrivateKeyKey: key},
	})
	create(ctx, t, access.client, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: oidcIssuer, Name: "files"}, Data: data})
	create(ctx, t, access.client, &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: oidcIssuer, Name: oidcIssuer},
		Spec:       corev1.ServiceSpec{Selector: labels, Ports: []corev1.ServicePort{{Name: "https", Port: 443, TargetPort: intstr.FromInt32(8443)}}},
	})
	create(ctx, t, access.client, &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: oidcIssuer, Name: oidcIssuer},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					NodeName: node,
					Containers: []corev1.Container{{
						Name:            oidcIssuer,
						Image:           image.String(),
						ImagePullPolicy: corev1.PullNever,
						Ports:           []corev1.ContainerPort{{Name: "https", ContainerPort: 8443}},
						ReadinessProbe:  &corev1.Probe{ProbeHandler: corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(8443)}}},
						VolumeMounts: []corev1.VolumeMount{
							{Name: "files", MountPath: "/etc/oidcissuer/files", ReadOnly: true},
							{Name: "tls", MountPath: "/etc/oidcissuer/tls", ReadOnly: true},
						},
					}},
					Volumes: []corev1.Volume{
						{Name: "files", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: "files"}, Items: items}}},
						{Name: "tls", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: "tls"}}},
					},
				},
			},
		},
	})
	until(ctx, t, 3*time.Minute, "the issuer's Deployment is available", func(ctx context.Context) error {
		d := &appsv1.Deployment{}
		if err := access.client.Get(ctx, client.ObjectKey{Namespace: oidcIssuer, Name: oidcIssuer}, d); err != nil {
			return err
		}
		if d.Status.AvailableReplicas < 1 {
			return fmt.Errorf("%d replicas available", d.Status.AvailableReplicas)
		}
		return nil
	})
}

func login(ctx context.Context, access *tenantAccess, token string) (authenticationv1.UserInfo, error) {
	anonymous := rest.AnonymousClientConfig(access.cfg)
	anonymous.BearerToken = token
	c, err := kubernetes.NewForConfig(anonymous)
	if err != nil {
		return authenticationv1.UserInfo{}, err
	}
	review, err := c.AuthenticationV1().SelfSubjectReviews().Create(ctx, &authenticationv1.SelfSubjectReview{}, metav1.CreateOptions{})
	if err != nil {
		return authenticationv1.UserInfo{}, err
	}
	return review.Status.UserInfo, nil
}

// apiServers are the API server's pods, by name, with their UIDs and restart
// counts.
func apiServers(ctx context.Context, t *testing.T, k8s client.Client, key client.ObjectKey) map[string]string {
	t.Helper()
	pods := &corev1.PodList{}
	if err := k8s.List(ctx, pods, client.InNamespace(key.Namespace), client.MatchingLabels{
		lodiav1alpha1.ControlPlaneLabel: key.Name, lodiav1alpha1.ComponentLabel: lodiav1alpha1.APIServerComponent,
	}); err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for _, pod := range pods.Items {
		var restarts []string
		for _, s := range append(pod.Status.InitContainerStatuses, pod.Status.ContainerStatuses...) {
			restarts = append(restarts, s.Name+"="+strconv.Itoa(int(s.RestartCount)))
		}
		out[pod.Name] = string(pod.UID) + " " + strings.Join(restarts, ",")
	}
	return out
}

// loadedHash is the hash of the authentication configuration that an API
// server pod serves, read from its metrics through a port-forward.
func loadedHash(ctx context.Context, t *testing.T, cfg *envconf.Config, access *tenantAccess, pod *corev1.Pod) string {
	t.Helper()
	address, stop := forward(t, cfg.Client().RESTConfig(), pod, 6443)
	defer stop()
	direct := rest.CopyConfig(access.cfg)
	host, _, err := net.SplitHostPort(strings.TrimPrefix(direct.Host, "https://"))
	if err != nil {
		t.Fatal(err)
	}
	direct.Host, direct.ServerName = "https://"+address, host
	c, err := kubernetes.NewForConfig(direct)
	if err != nil {
		t.Fatal(err)
	}
	body, err := c.RESTClient().Get().AbsPath("/metrics").DoRaw(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for line := range strings.Lines(string(body)) {
		if strings.HasPrefix(line, "apiserver_authentication_config_controller_last_config_info{") {
			_, hash, _ := strings.Cut(line, `hash="`)
			hash, _, _ = strings.Cut(hash, `"`)
			return hash
		}
	}
	t.Fatalf("pod %s serves no apiserver_authentication_config_controller_last_config_info", pod.Name)
	return ""
}

func TestStructuredAuthenticationOnKind(t *testing.T) {
	var (
		k8s     client.Client
		access  *tenantAccess
		node    string
		image   reference.NamedTagged
		issuers *testIssuers
	)
	key := client.ObjectKey{Namespace: authnTenant, Name: controlPlane}
	alice := map[string]any{"sub": "alice", "tenant": "acme", "roles": []string{"dev", "ops"}}
	f := features.New("structured authentication with an issuer inside the tenant").
		Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			k8s = newClient(t, cfg)
			issuers = newTestIssuers(t)
			createTenant(ctx, t, k8s, key, authnClass, func(s *lodiav1alpha1.ControlPlaneSpec) {
				s.APIServer = &lodiav1alpha1.APIServer{Authentication: &lodiav1alpha1.Authentication{JWT: issuers.authenticators(t, "first")}}
			})
			built, err := build(ctx, cfg.KubeconfigFile(), "./test/"+oidcIssuer)
			if err != nil {
				t.Fatal(err)
			}
			image = built
			return ctx
		}).
		Assess("the tenant turns Ready, a node joins and the issuer runs on it", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			until(ctx, t, 3*time.Minute, "Ready=True", func(ctx context.Context) error {
				return controlPlaneReady(ctx, k8s, key, &lodiav1alpha1.ControlPlane{})
			})
			access = openTenant(ctx, t, k8s, key)
			node = joinNodes(ctx, t, cfg, k8s, key, access, 1)[0]
			until(ctx, t, 3*time.Minute, "CoreDNS is available", func(ctx context.Context) error {
				return coreDNSAvailable(ctx, access.client)
			})
			loadImage(ctx, t, cfg.KubeconfigFile(), node, image)
			issuers.deploy(ctx, t, access, image, node)
			return ctx
		}).
		Assess("a token of the issuer logs in through status.endpoint as the user its claims map to, and the claim rule, the user rule and the reserved names refuse theirs", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			var user authenticationv1.UserInfo
			until(ctx, t, 3*time.Minute, "the first issuer's token logs in", func(ctx context.Context) error {
				var err error
				user, err = login(ctx, access, issuers.token(t, "first", alice))
				return err
			})
			want := authenticationv1.UserInfo{Username: "oidc:alice", UID: "alice", Groups: []string{"oidc:dev", "oidc:ops", "system:authenticated"}, Extra: map[string]authenticationv1.ExtraValue{"example.com/tenant": {"acme"}}}
			if fmt.Sprint(user) != fmt.Sprint(want) {
				t.Errorf("the token logs in as %+v, want %+v", user, want)
			}
			for rule, claims := range map[string]map[string]any{
				"the claim rule": {"sub": "bob", "tenant": "other", "roles": []string{"dev"}},
				"the user rule":  {"sub": "bob", "tenant": "acme", "roles": []string{"suspended"}},
			} {
				if refused, err := login(ctx, access, issuers.token(t, "first", claims)); err == nil {
					t.Errorf("%s lets %+v in", rule, refused)
				}
			}
			return ctx
		}).
		Assess("a move to the second issuer loads in every API server replica without a restart", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			before := apiServers(ctx, t, k8s, key)
			deployment := &appsv1.Deployment{}
			if err := k8s.Get(ctx, client.ObjectKey{Namespace: key.Namespace, Name: key.Name + "-" + lodiav1alpha1.APIServerComponent}, deployment); err != nil {
				t.Fatal(err)
			}
			hash := deployment.Spec.Template.Annotations["lodia.io/restart-hash"]
			cp := &lodiav1alpha1.ControlPlane{}
			if err := k8s.Get(ctx, key, cp); err != nil {
				t.Fatal(err)
			}
			cp.Spec.APIServer.Authentication.JWT = issuers.authenticators(t, "second")
			if err := k8s.Update(ctx, cp); err != nil {
				t.Fatal(err)
			}
			edited := time.Now()
			until(ctx, t, 5*time.Minute, "the second issuer's token logs in and the first's does not", func(ctx context.Context) error {
				if _, err := login(ctx, access, issuers.token(t, "second", alice)); err != nil {
					return err
				}
				if _, err := login(ctx, access, issuers.token(t, "first", alice)); err == nil {
					return fmt.Errorf("the first issuer's token still logs in")
				}
				return nil
			})
			t.Logf("the second issuer's token logs in %s after the edit", time.Since(edited).Round(time.Second))
			if after := apiServers(ctx, t, k8s, key); fmt.Sprint(after) != fmt.Sprint(before) {
				t.Errorf("the API server's pods are %v, then %v", before, after)
			}
			if err := k8s.Get(ctx, client.ObjectKeyFromObject(deployment), deployment); err != nil {
				t.Fatal(err)
			}
			if now := deployment.Spec.Template.Annotations["lodia.io/restart-hash"]; now != hash {
				t.Errorf("the restart hash is %s, then %s", hash, now)
			}
			written := &corev1.ConfigMap{}
			if err := k8s.Get(ctx, client.ObjectKey{Namespace: key.Namespace, Name: key.Name + "-authentication-config"}, written); err != nil {
				t.Fatal(err)
			}
			want := fmt.Sprintf("sha256:%x", sha256.Sum256([]byte(written.Data["authentication-config.yaml"])))
			pods := &corev1.PodList{}
			if err := k8s.List(ctx, pods, client.InNamespace(key.Namespace), client.MatchingLabels{
				lodiav1alpha1.ControlPlaneLabel: key.Name, lodiav1alpha1.ComponentLabel: lodiav1alpha1.APIServerComponent,
			}); err != nil {
				t.Fatal(err)
			}
			for i := range pods.Items {
				if got := loadedHash(ctx, t, cfg, access, &pods.Items[i]); got != want {
					t.Errorf("pod %s serves the configuration %s, want the ConfigMap's %s", pods.Items[i].Name, got, want)
				}
			}
			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			closeTenant(ctx, t, k8s, key, authnClass)
			return ctx
		}).
		Feature()
	testenv.Test(t, f)
}
