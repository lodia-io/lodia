// Copyright 2026 The Brygga Authors.
// SPDX-License-Identifier: Apache-2.0

package pki

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"maps"
	"net"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/apis/apiserver"
	apiserverv1 "k8s.io/apiserver/pkg/apis/apiserver/v1"
	"k8s.io/apiserver/pkg/apis/apiserver/validation"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	metav1ac "k8s.io/client-go/applyconfigurations/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	bryggav1alpha1 "github.com/segla-io/brygga/api/v1alpha1"
	"github.com/segla-io/brygga/internal/certs"
	"github.com/segla-io/brygga/internal/ownership"
)

// The CAs, by the suffix of their <cp>-cakey-<ca> Secret.
const (
	clusterCA    = "cluster"
	clientCA     = "client"
	frontProxyCA = "front-proxy"
	etcdCA       = "etcd"
)

// A CA's bundle proves the tenant had that CA, so a CA is created only when
// neither the bundle nor its key Secret exists.
var caBundles = []struct {
	ca, bundle string
}{
	{clusterCA, "ca-bundle"},
	{clientCA, "client-ca"},
	{frontProxyCA, "front-proxy-ca"},
	{etcdCA, "etcd-ca"},
}

// names are PKI's Secrets, <cp>-<name>, beside the etcd generations
// <cp>-etcd-<role>-<g>. They are reserved: PKI finds them by name, labelled or
// not.
var names = func() []string {
	names := []string{"cakey-cluster", "cakey-client", "cakey-front-proxy", "cakey-etcd"}
	for _, s := range bryggav1alpha1.PKISecrets {
		names = append(names, string(s))
	}
	return names
}()

const (
	// signingDuration is how long the controller manager's kubelet client
	// certificates live, and so how long a replaced kubelet signer stays in
	// the client CA bundle.
	signingDuration = 30 * 24 * time.Hour
	// serviceAccountKeyBits is RSA 2048: OIDC makes RS256 mandatory for every
	// provider, so it is what a federation partner accepts.
	serviceAccountKeyBits = 2048
	encryptionKeyBytes    = 32
	caKey                 = "ca.crt"
	kubeconfigKey         = "kubeconfig"
	signingKey            = "signing.key"
	encryptionConfigKey   = "encryption-config.yaml"

	providerNone      bryggav1alpha1.EncryptionProvider = "None"
	providerSecretbox bryggav1alpha1.EncryptionProvider = "secretbox"
	providerAESCBC    bryggav1alpha1.EncryptionProvider = "aescbc"
)

var (
	serverAuth       = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
	clientAuth       = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	serverClientAuth = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}
)

type tenant struct {
	cp *bryggav1alpha1.ControlPlane
	// existing holds PKI's Secrets that this ControlPlane or nobody controls;
	// foreign, those another owner keeps.
	existing    map[string]*corev1.Secret
	foreign     []foreign
	generations map[int]bool
	now         time.Time
	// wake is the earliest renewal, CA notice or CA expiry still ahead.
	wake time.Time
	// cas sign; caCerts holds every CA on the server, expired ones included.
	cas                 map[string]*certs.Certificate
	caCerts             map[string]*x509.Certificate
	missingCAs          []string
	expiredCAs          []string
	unreadable          []string
	unsupportedProvider bryggav1alpha1.EncryptionProvider
	endpoint            *endpoint
	konnectivityHost    string
	// writeErr is the pass's first failed write. No write follows it, and
	// pending names what the pass had to change but did not write.
	writeErr error
	pending  []string
}

type foreign struct {
	name  string
	owner metav1.OwnerReference
}

type endpoint struct {
	host string
	port int32
}

func (e *endpoint) url() string {
	return "https://" + net.JoinHostPort(e.host, strconv.Itoa(int(e.port)))
}

func newTenant(cp *bryggav1alpha1.ControlPlane, now time.Time) *tenant {
	t := &tenant{cp: cp, existing: map[string]*corev1.Secret{}, generations: map[int]bool{}, now: now, cas: map[string]*certs.Certificate{}, caCerts: map[string]*x509.Certificate{}}
	if e := cp.Status.Endpoint; e != nil {
		t.endpoint = &endpoint{host: e.APIServer.Host, port: e.APIServer.Port}
		t.konnectivityHost = e.Konnectivity.Host
	}
	return t
}

func (t *tenant) name(suffix string) string {
	return t.cp.Name + "-" + suffix
}

// reserved reports whether name is one of PKI's, and the etcd generation it
// belongs to, if any.
func (t *tenant) reserved(name string) (int, bool) {
	suffix, ok := strings.CutPrefix(name, t.cp.Name+"-")
	if !ok {
		return 0, false
	}
	if slices.Contains(names, suffix) {
		return 0, true
	}
	for _, role := range bryggav1alpha1.EtcdCertificateRoles {
		if n, ok := strings.CutPrefix(suffix, "etcd-"+role+"-"); ok {
			if g, err := strconv.Atoi(n); err == nil && g > 0 && strconv.Itoa(g) == n {
				return g, true
			}
			return 0, false
		}
	}
	return 0, false
}

// read takes the Secrets under PKI's names, labelled or not: the labelled ones
// whole, and the namespace's metadata to find the others, so that no list
// carries the data of every Secret in the namespace.
func (r *Reconciler) read(ctx context.Context, t *tenant) error {
	ns := t.cp.Namespace
	var labelled corev1.SecretList
	if err := r.Reader.List(ctx, &labelled, client.InNamespace(ns), client.MatchingLabels{bryggav1alpha1.ControlPlaneLabel: t.cp.Name}); err != nil {
		return err
	}
	all := &metav1.PartialObjectMetadataList{}
	all.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("SecretList"))
	if err := r.Reader.List(ctx, all, client.InNamespace(ns)); err != nil {
		return err
	}
	whole := make(map[string]*corev1.Secret, len(labelled.Items))
	for i := range labelled.Items {
		whole[labelled.Items[i].Name] = &labelled.Items[i]
	}
	for i := range all.Items {
		m := &all.Items[i]
		g, ok := t.reserved(m.Name)
		if !ok || t.held(m) {
			continue
		}
		s := whole[m.Name]
		if s == nil {
			s = &corev1.Secret{}
			if err := r.Reader.Get(ctx, client.ObjectKey{Namespace: ns, Name: m.Name}, s); err != nil {
				if apierrors.IsNotFound(err) {
					continue
				}
				return err
			}
		}
		if t.held(s) {
			continue
		}
		t.existing[s.Name] = s
		if g > 0 {
			t.generations[g] = true
		}
	}
	return nil
}

// held records a Secret another owner keeps.
func (t *tenant) held(obj metav1.Object) bool {
	owner := ownership.ControlledElsewhere(obj, t.cp)
	if owner != nil {
		t.foreign = append(t.foreign, foreign{name: obj.GetName(), owner: *owner})
	}
	return owner != nil
}

func (t *tenant) stamp(prefix string) string {
	return prefix + "@" + strconv.FormatInt(t.now.UnixNano(), 10)
}

func (t *tenant) wakeAt(at time.Time) {
	if at.After(t.now) && (t.wake.IsZero() || at.Before(t.wake)) {
		t.wake = at
	}
}

// renew schedules c's renewal, unless c already ends with its CA: a new
// certificate would not outlive it.
func (t *tenant) renew(c, ca *x509.Certificate) {
	if c.NotAfter.Before(ca.NotAfter) {
		t.wakeAt(certs.RenewalTime(c))
	}
}

// requeueAfter rounds up, so that no pass a deadline schedules starts before
// it.
func (t *tenant) requeueAfter() time.Duration {
	after := bindingsPeriod
	if !t.wake.IsZero() {
		after = min(after, t.wake.Sub(t.now))
	}
	return max((after + time.Second - 1).Truncate(time.Second), time.Second)
}

// foreignRequeue waits for the garbage collector when every foreign Secret
// is a previous ControlPlane's of this name, and for a person otherwise.
func (t *tenant) foreignRequeue() time.Duration {
	for _, f := range t.foreign {
		if !ownership.IsControlPlane(f.owner) || f.owner.Name != t.cp.Name {
			return time.Minute
		}
	}
	return 10 * time.Second
}

func (t *tenant) credentials() map[string]any {
	out := map[string]any{}
	for field, secret := range map[string]string{"adminKubeconfigSecretRef": t.secret(bryggav1alpha1.AdminKubeconfigSecret), "caBundleSecretRef": t.secret(bryggav1alpha1.CABundleSecret)} {
		if t.existing[secret] != nil {
			out[field] = map[string]any{"name": secret}
		}
	}
	return out
}

func data(s *corev1.Secret, key string) []byte {
	if s == nil {
		return nil
	}
	return s.Data[key]
}

func isImmutable(s *corev1.Secret) bool {
	return s.Immutable != nil && *s.Immutable
}

func (t *tenant) current(c, ca *certs.Certificate, p certs.Profile) bool {
	return certs.Verify(c.Cert, ca.Cert, t.now) == nil && certs.Matches(c.Cert, p) &&
		(t.now.Before(certs.RenewalTime(c.Cert)) || !c.Cert.NotAfter.Before(ca.Cert.NotAfter))
}

func (t *tenant) leaf(certPEM, keyPEM []byte, ca *certs.Certificate, p certs.Profile, lifetime time.Duration) (*certs.Certificate, error) {
	c, err := certs.Parse(certPEM, keyPEM)
	if err != nil || !t.current(c, ca, p) {
		if c, err = ca.Issue(p, t.now, lifetime); err != nil {
			return nil, err
		}
	}
	t.renew(c.Cert, ca.Cert)
	return c, nil
}

// issue writes the encryption configuration last, so that an unsupported
// provider blocks nothing else.
func (r *Reconciler) issue(ctx context.Context, t *tenant) error {
	for _, ca := range caBundles {
		if err := r.ca(ctx, t, ca.ca, ca.bundle); err != nil {
			return err
		}
	}
	cluster, cl, frontProxy, etcd := t.cas[clusterCA], t.cas[clientCA], t.cas[frontProxyCA], t.cas[etcdCA]

	if cluster != nil {
		if err := r.bundle(ctx, t, t.secret(bryggav1alpha1.CABundleSecret), cluster.Cert); err != nil {
			return err
		}
		if err := r.tls(ctx, t, t.secret(bryggav1alpha1.APIServerKubeletClientSecret), cluster, certs.Profile{CommonName: "kube-apiserver-kubelet-client", Organizations: []string{"brygga:kubelet-api-admins"}, Usages: clientAuth}); err != nil {
			return err
		}
		if t.endpoint != nil {
			if err := r.tls(ctx, t, t.secret(bryggav1alpha1.APIServerServingSecret), cluster, t.servingProfile()); err != nil {
				return err
			}
			if err := r.tls(ctx, t, t.secret(bryggav1alpha1.KonnectivityServerSecret), cluster, t.konnectivityProfile()); err != nil {
				return err
			}
		}
	}

	if cl != nil {
		if err := r.kubeletSigner(ctx, t); err != nil {
			return err
		}
	}
	if cl != nil && cluster != nil {
		internal := "https://" + bryggav1alpha1.APIServerName(t.cp.Name) + "." + t.cp.Namespace + ".svc:443"
		kubeconfigs := []kubeconfig{
			{"konnectivity-kubeconfig", "system:konnectivity-server", nil, "https://127.0.0.1:6443", false},
			{"controller-manager-kubeconfig", "system:kube-controller-manager", nil, internal, false},
			{"scheduler-kubeconfig", "system:kube-scheduler", nil, internal, false},
			{"brygga-kubeconfig", "brygga:operator", nil, internal, true},
		}
		if t.endpoint != nil {
			kubeconfigs = append(kubeconfigs, kubeconfig{"admin-kubeconfig", "kubernetes-admin", []string{"brygga:cluster-admins"}, t.endpoint.url(), true})
		}
		for _, k := range kubeconfigs {
			if err := r.kubeconfig(ctx, t, k); err != nil {
				return err
			}
		}
	}

	if frontProxy != nil {
		if err := r.bundle(ctx, t, t.secret(bryggav1alpha1.FrontProxyCASecret), frontProxy.Cert); err != nil {
			return err
		}
		if err := r.tls(ctx, t, t.secret(bryggav1alpha1.FrontProxyClientSecret), frontProxy, certs.Profile{CommonName: "front-proxy-client", Usages: clientAuth}); err != nil {
			return err
		}
	}

	if etcd != nil {
		if err := r.bundle(ctx, t, t.secret(bryggav1alpha1.EtcdCASecret), etcd.Cert); err != nil {
			return err
		}
		if err := r.tls(ctx, t, t.secret(bryggav1alpha1.APIServerEtcdClientSecret), etcd, certs.Profile{CommonName: "kube-apiserver-etcd-client", Usages: clientAuth}); err != nil {
			return err
		}
		if err := r.etcd(ctx, t); err != nil {
			return err
		}
	}

	if err := r.serviceAccountKey(ctx, t); err != nil {
		return err
	}
	return r.encryptionConfig(ctx, t)
}

// ca creates a CA only for a tenant with neither its key Secret nor its
// bundle: a new CA in place of a lost or expired one would cut every client
// or node off at once.
func (r *Reconciler) ca(ctx context.Context, t *tenant, name, bundle string) error {
	secret := t.name("cakey-" + name)
	key, trusted := t.existing[secret], t.existing[t.name(bundle)]
	if key == nil && trusted == nil {
		lifetime := r.CALifetime
		if name == clusterCA {
			lifetime = r.TrustAnchorLifetime
		}
		c, err := certs.NewCA(t.stamp(name+"-ca"), t.now, lifetime)
		if err != nil {
			return err
		}
		t.cas[name] = c
		r.notice(t, c.Cert)
		if err := r.applyTLS(ctx, t, secret, c, nil, false); err != nil {
			return err
		}
		if t.existing[secret] != nil {
			t.caCerts[name] = c.Cert
		}
		return nil
	}
	c := readCA(key)
	if c == nil {
		t.missingCAs = append(t.missingCAs, name)
		return nil
	}
	t.caCerts[name] = c.Cert
	if !t.now.Before(c.Cert.NotAfter) {
		t.expiredCAs = append(t.expiredCAs, name)
		return nil
	}
	t.cas[name] = c
	r.notice(t, c.Cert)
	r.applySecret(ctx, t, secret, corev1.SecretTypeTLS, map[string][]byte{corev1.TLSCertKey: key.Data[corev1.TLSCertKey], corev1.TLSPrivateKeyKey: key.Data[corev1.TLSPrivateKeyKey]}, nil, false)
	return nil
}

func readCA(s *corev1.Secret) *certs.Certificate {
	c, err := certs.Parse(data(s, corev1.TLSCertKey), data(s, corev1.TLSPrivateKeyKey))
	if err != nil || !c.Cert.IsCA || c.Cert.CheckSignatureFrom(c.Cert) != nil {
		return nil
	}
	return c
}

func (r *Reconciler) notice(t *tenant, ca *x509.Certificate) {
	t.wakeAt(ca.NotAfter.Add(-r.CAExpiryNotice))
	t.wakeAt(ca.NotAfter)
}

// kubeletSigner compares no subject, since every signer gets a new one, and
// writes a new signer only after the client CA bundle that trusts it.
func (r *Reconciler) kubeletSigner(ctx context.Context, t *tenant) error {
	name := t.secret(bryggav1alpha1.KubeletSignerSecret)
	ca := t.cas[clientCA]
	s := t.existing[name]
	c, err := certs.Parse(data(s, corev1.TLSCertKey), data(s, corev1.TLSPrivateKeyKey))
	if err != nil || !c.Cert.MaxPathLenZero || !t.current(c, ca, certs.Profile{CommonName: c.Cert.Subject.CommonName, CA: true}) {
		if c, err = ca.Issue(certs.Profile{CommonName: t.stamp("kubelet-signer"), CA: true}, t.now, r.LeafLifetime); err != nil {
			return err
		}
	}
	t.renew(c.Cert, ca.Cert)
	if err := r.bundle(ctx, t, t.secret(bryggav1alpha1.ClientCASecret), t.clientBundle(c)...); err != nil {
		return err
	}
	return r.applyTLS(ctx, t, name, c, nil, false)
}

func (r *Reconciler) tls(ctx context.Context, t *tenant, name string, ca *certs.Certificate, p certs.Profile) error {
	s := t.existing[name]
	c, err := t.leaf(data(s, corev1.TLSCertKey), data(s, corev1.TLSPrivateKeyKey), ca, p, r.LeafLifetime)
	if err != nil {
		return err
	}
	return r.applyTLS(ctx, t, name, c, nil, false)
}

func (r *Reconciler) applyTLS(ctx context.Context, t *tenant, name string, c *certs.Certificate, labels map[string]string, immutable bool) error {
	cert, err := certs.CertPEM(c.Cert)
	if err != nil {
		return err
	}
	key, err := certs.KeyPEM(c.Key)
	if err != nil {
		return err
	}
	r.applySecret(ctx, t, name, corev1.SecretTypeTLS, map[string][]byte{corev1.TLSCertKey: cert, corev1.TLSPrivateKeyKey: key}, labels, immutable)
	return nil
}

func (r *Reconciler) bundle(ctx context.Context, t *tenant, name string, bundle ...*x509.Certificate) error {
	pem, err := certs.Bundle(bundle...)
	if err != nil {
		return err
	}
	r.applySecret(ctx, t, name, corev1.SecretTypeOpaque, map[string][]byte{caKey: pem}, nil, false)
	return nil
}

// clientBundle keeps each replaced signer for one signing duration, since
// kubelets present only their leaf.
func (t *tenant) clientBundle(signer *certs.Certificate) []*x509.Certificate {
	ca := t.cas[clientCA].Cert
	bundle := []*x509.Certificate{ca, signer.Cert}
	previous, _ := certs.ParseCerts(data(t.existing[t.secret(bryggav1alpha1.ClientCASecret)], caKey))
	replaced := signer.Cert.NotBefore.Add(certs.Backdate)
	for _, c := range previous {
		if c.IsCA && !c.Equal(ca) && certs.Verify(c, ca, t.now) == nil && t.now.Before(replaced.Add(signingDuration)) {
			bundle = append(bundle, c)
		}
	}
	return bundle
}

func (t *tenant) servingProfile() certs.Profile {
	cp := t.cp
	domain := string(cp.Spec.Network.ClusterDomain)
	if domain == "" {
		domain = "cluster.local"
	}
	api := bryggav1alpha1.APIServerName(t.cp.Name)
	p := certs.Profile{CommonName: "kube-apiserver", Usages: serverAuth}
	for _, name := range []string{"kubernetes", "kubernetes.default", "kubernetes.default.svc", "kubernetes.default.svc." + domain} {
		p.SAN(name)
	}
	for _, cidr := range cp.Spec.Network.ServiceCIDRs {
		if prefix, err := netip.ParsePrefix(string(cidr)); err == nil {
			p.SAN(prefix.Masked().Addr().Next().String())
		}
	}
	for _, name := range []string{api, api + "." + cp.Namespace, api + "." + cp.Namespace + ".svc", "localhost", "127.0.0.1", t.endpoint.host} {
		p.SAN(name)
	}
	for _, san := range cp.Spec.Endpoint.CertSANs {
		p.SAN(string(san))
	}
	return p
}

func (t *tenant) konnectivityProfile() certs.Profile {
	p := certs.Profile{CommonName: "konnectivity-server", Usages: serverAuth}
	for _, name := range []string{"localhost", "127.0.0.1", t.konnectivityHost} {
		p.SAN(name)
	}
	return p
}

type kubeconfig struct {
	suffix string
	user   string
	groups []string
	server string
	// embed puts the certificates inline, for a kubeconfig that leaves the
	// pod world; otherwise they are files beside it, which client-go reloads.
	embed bool
}

func (r *Reconciler) kubeconfig(ctx context.Context, t *tenant, k kubeconfig) error {
	name := t.name(k.suffix)
	s := t.existing[name]
	certPEM, keyPEM := data(s, corev1.TLSCertKey), data(s, corev1.TLSPrivateKeyKey)
	if k.embed {
		certPEM, keyPEM, _ = certs.Embedded(data(s, kubeconfigKey))
	}
	c, err := t.leaf(certPEM, keyPEM, t.cas[clientCA], certs.Profile{CommonName: k.user, Organizations: k.groups, Usages: clientAuth}, r.LeafLifetime)
	if err != nil {
		return err
	}
	cert, err := certs.CertPEM(c.Cert)
	if err != nil {
		return err
	}
	key, err := certs.KeyPEM(c.Key)
	if err != nil {
		return err
	}
	ca, err := certs.CertPEM(t.cas[clusterCA].Cert)
	if err != nil {
		return err
	}
	rendered, err := certs.Kubeconfig(t.cp.Name, k.server, k.user, ca, cert, key, k.embed)
	if err != nil {
		return err
	}
	content := map[string][]byte{kubeconfigKey: rendered}
	if !k.embed {
		content[caKey], content[corev1.TLSCertKey], content[corev1.TLSPrivateKeyKey] = ca, cert, key
	}
	r.applySecret(ctx, t, name, corev1.SecretTypeOpaque, content, nil, false)
	return nil
}

// serviceAccountKey keeps <cp>-sa-keys, brygga-signer's file contract. A key
// it cannot read is never replaced: every token it signed would stop
// verifying.
func (r *Reconciler) serviceAccountKey(ctx context.Context, t *tenant) error {
	name := t.secret(bryggav1alpha1.ServiceAccountKeysSecret)
	if s := t.existing[name]; s != nil {
		if !isServiceAccountKey(s.Data[signingKey]) {
			t.unreadable = append(t.unreadable, name)
			return nil
		}
		r.applySecret(ctx, t, name, corev1.SecretTypeOpaque, maps.Clone(s.Data), nil, false)
		return nil
	}
	key, err := rsa.GenerateKey(rand.Reader, serviceAccountKeyBits)
	if err != nil {
		return err
	}
	pem, err := certs.KeyPEM(key)
	if err != nil {
		return err
	}
	r.applySecret(ctx, t, name, corev1.SecretTypeOpaque, map[string][]byte{signingKey: pem}, nil, false)
	return nil
}

func isServiceAccountKey(pem []byte) bool {
	key, err := certs.ParseKey(pem)
	rsaKey, ok := key.(*rsa.PrivateKey)
	return err == nil && ok && rsaKey.N.BitLen() >= serviceAccountKeyBits
}

// encryptionConfig puts a changed provider first with a new key, and keeps
// the previous providers so that what they encrypted stays readable. A
// configuration it cannot read is never replaced: the data its keys
// encrypted would be lost.
func (r *Reconciler) encryptionConfig(ctx context.Context, t *tenant) error {
	name := t.secret(bryggav1alpha1.EncryptionConfigSecret)
	want := providerSecretbox
	if api := t.cp.Spec.APIServer; api != nil && api.Encryption != nil && api.Encryption.Provider != "" {
		want = api.Encryption.Provider
	}
	if !slices.Contains([]bryggav1alpha1.EncryptionProvider{providerNone, providerSecretbox, providerAESCBC}, want) {
		t.unsupportedProvider = want
		return nil
	}
	var providers []apiserverv1.ProviderConfiguration
	if s := t.existing[name]; s != nil {
		existing, ok := readEncryptionConfig(s.Data[encryptionConfigKey])
		if !ok {
			t.unreadable = append(t.unreadable, name)
			return nil
		}
		if providers = existing.Resources[0].Providers; providerOf(providers[0]) == want {
			r.applySecret(ctx, t, name, corev1.SecretTypeOpaque, map[string][]byte{encryptionConfigKey: s.Data[encryptionConfigKey]}, nil, false)
			return nil
		}
	}
	first, err := newProvider(want)
	if err != nil {
		return err
	}
	cfg := apiserverv1.EncryptionConfiguration{
		TypeMeta:  metav1.TypeMeta{APIVersion: apiserverv1.SchemeGroupVersion.String(), Kind: "EncryptionConfiguration"},
		Resources: []apiserverv1.ResourceConfiguration{{Resources: []string{"secrets"}, Providers: append([]apiserverv1.ProviderConfiguration{first}, providers...)}},
	}
	rendered, err := yaml.Marshal(cfg)
	if err != nil {
		return err
	}
	r.applySecret(ctx, t, name, corev1.SecretTypeOpaque, map[string][]byte{encryptionConfigKey: rendered}, nil, false)
	return nil
}

// readEncryptionConfig accepts what the API server's own validator accepts
// and PKI writes: one resource entry, for secrets. Reload is on, as the API
// server Brygga runs reloads the file.
func readEncryptionConfig(content []byte) (*apiserverv1.EncryptionConfiguration, bool) {
	cfg := &apiserverv1.EncryptionConfiguration{}
	if err := yaml.Unmarshal(content, cfg); err != nil || cfg.APIVersion != apiserverv1.SchemeGroupVersion.String() || cfg.Kind != "EncryptionConfiguration" ||
		len(cfg.Resources) != 1 || !slices.Equal(cfg.Resources[0].Resources, []string{"secrets"}) {
		return nil, false
	}
	internal := &apiserver.EncryptionConfiguration{}
	if err := apiserverv1.Convert_v1_EncryptionConfiguration_To_apiserver_EncryptionConfiguration(cfg, internal, nil); err != nil {
		return nil, false
	}
	return cfg, len(validation.ValidateEncryptionConfiguration(internal, true)) == 0
}

func providerOf(p apiserverv1.ProviderConfiguration) bryggav1alpha1.EncryptionProvider {
	switch {
	case p.Secretbox != nil:
		return providerSecretbox
	case p.AESCBC != nil:
		return providerAESCBC
	case p.Identity != nil:
		return providerNone
	default:
		return ""
	}
}

func newProvider(provider bryggav1alpha1.EncryptionProvider) (apiserverv1.ProviderConfiguration, error) {
	if provider == providerNone {
		return apiserverv1.ProviderConfiguration{Identity: &apiserverv1.IdentityConfiguration{}}, nil
	}
	secret := make([]byte, encryptionKeyBytes)
	if _, err := rand.Read(secret); err != nil {
		return apiserverv1.ProviderConfiguration{}, err
	}
	keys := []apiserverv1.Key{{Name: "key1", Secret: base64.StdEncoding.EncodeToString(secret)}}
	switch provider {
	case providerSecretbox:
		return apiserverv1.ProviderConfiguration{Secretbox: &apiserverv1.SecretboxConfiguration{Keys: keys}}, nil
	case providerAESCBC:
		return apiserverv1.ProviderConfiguration{AESCBC: &apiserverv1.AESConfiguration{Keys: keys}}, nil
	default:
		return apiserverv1.ProviderConfiguration{}, fmt.Errorf("encryption provider %q is not supported", provider)
	}
}

// etcd keeps the current generation and the newest complete one below it,
// which the Etcd may still run on, and deletes every other. A new generation
// is numbered above every one present, labelled or not, so it never takes a
// name a leftover holds.
func (r *Reconciler) etcd(ctx context.Context, t *tenant) error {
	ca := t.cas[etcdCA]
	profiles := t.etcdProfiles()
	generations := slices.Sorted(maps.Keys(t.generations))
	current := 0
	if len(generations) > 0 {
		current = generations[len(generations)-1]
	}
	if kept := t.etcdGeneration(current, ca, profiles); kept != nil {
		for _, c := range kept {
			t.renew(c.Cert, ca.Cert)
		}
	} else {
		current++
		if err := r.issueEtcd(ctx, t, current, ca, profiles); err != nil {
			return err
		}
	}
	previous := 0
	for _, g := range generations {
		if g < current && t.etcdComplete(g) {
			previous = g
		}
	}
	for _, g := range []int{current, previous} {
		r.adopt(ctx, t, g)
	}
	for _, g := range generations {
		if g == current || g == previous {
			continue
		}
		for _, role := range bryggav1alpha1.EtcdCertificateRoles {
			s := t.existing[t.etcdName(role, g)]
			if s == nil || s.DeletionTimestamp != nil || t.writeErr != nil {
				continue
			}
			err := r.Delete(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: s.Name, Namespace: s.Namespace}},
				client.Preconditions{UID: &s.UID, ResourceVersion: &s.ResourceVersion})
			if client.IgnoreNotFound(err) != nil {
				t.writeErr = fmt.Errorf("delete Secret %s: %w", s.Name, err)
			}
		}
	}
	return nil
}

// adopt applies a kept generation whole, its data unchanged, wherever it
// lacks this ControlPlane's labels or owner reference: an orphaned generation
// would outlive its ControlPlane with its keys, and one without its label is
// invisible to the label-filtered cache.
func (r *Reconciler) adopt(ctx context.Context, t *tenant, g int) {
	labels := map[string]string{bryggav1alpha1.EtcdCertificatesLabel: strconv.Itoa(g)}
	for _, role := range bryggav1alpha1.EtcdCertificateRoles {
		if s := t.existing[t.etcdName(role, g)]; s != nil && s.DeletionTimestamp == nil {
			r.applySecret(ctx, t, s.Name, s.Type, s.Data, labels, true)
		}
	}
}

// issueEtcd writes generation g whole and immutable: etcd-druid rolls the
// members only for a new Secret name, never for one changed in place.
func (r *Reconciler) issueEtcd(ctx context.Context, t *tenant, g int, ca *certs.Certificate, profiles []etcdProfile) error {
	caPEM, err := certs.CertPEM(ca.Cert)
	if err != nil {
		return err
	}
	labels := map[string]string{bryggav1alpha1.EtcdCertificatesLabel: strconv.Itoa(g)}
	r.applySecret(ctx, t, t.etcdName("ca", g), corev1.SecretTypeOpaque, map[string][]byte{caKey: caPEM}, labels, true)
	for _, p := range profiles {
		c, err := ca.Issue(p.profile, t.now, r.LeafLifetime)
		if err != nil {
			return err
		}
		t.renew(c.Cert, ca.Cert)
		if err := r.applyTLS(ctx, t, t.etcdName(p.role, g), c, labels, true); err != nil {
			return err
		}
	}
	return nil
}

func (t *tenant) etcdComplete(g int) bool {
	for _, role := range bryggav1alpha1.EtcdCertificateRoles {
		if s := t.existing[t.etcdName(role, g)]; s == nil || s.DeletionTimestamp != nil {
			return false
		}
	}
	return true
}

func (t *tenant) etcdGeneration(g int, ca *certs.Certificate, profiles []etcdProfile) map[string]*certs.Certificate {
	if g == 0 || !t.etcdComplete(g) {
		return nil
	}
	s := t.existing[t.etcdName("ca", g)]
	if bundle, err := certs.ParseCerts(s.Data[caKey]); !isImmutable(s) || err != nil || len(bundle) != 1 || !bundle[0].Equal(ca.Cert) {
		return nil
	}
	kept := make(map[string]*certs.Certificate, len(profiles))
	for _, p := range profiles {
		s := t.existing[t.etcdName(p.role, g)]
		c, err := certs.Parse(s.Data[corev1.TLSCertKey], s.Data[corev1.TLSPrivateKeyKey])
		if !isImmutable(s) || err != nil || !t.current(c, ca, p.profile) {
			return nil
		}
		kept[p.role] = c
	}
	return kept
}

func (t *tenant) etcdName(role string, generation int) string {
	return bryggav1alpha1.EtcdCertificateSecretName(t.cp.Name, role, generation)
}

func (t *tenant) secret(s bryggav1alpha1.Secret) string {
	return s.Name(t.cp.Name)
}

type etcdProfile struct {
	role    string
	profile certs.Profile
}

// etcdProfiles are the certificates etcd-druid expects for the Etcd named
// <cp>-etcd, as hack/etcd-spike/pki.sh issued them.
func (t *tenant) etcdProfiles() []etcdProfile {
	n := bryggav1alpha1.EtcdName(t.cp.Name)
	forms := func(names ...string) []string {
		var out []string
		for _, s := range names {
			out = append(out, s, s+"."+t.cp.Namespace, s+"."+t.cp.Namespace+".svc", s+"."+t.cp.Namespace+".svc.cluster.local")
		}
		return out
	}
	local := []string{n + "-local"}
	loopback := []net.IP{net.ParseIP("127.0.0.1")}
	return []etcdProfile{
		{"server", certs.Profile{CommonName: n, Usages: serverClientAuth, DNSNames: slices.Concat(local, forms(n+"-client", "*."+n+"-peer"), []string{"localhost"}), IPAddresses: loopback}},
		{"peer", certs.Profile{CommonName: n + "-peer", Usages: serverClientAuth, DNSNames: forms(n+"-peer", "*."+n+"-peer")}},
		{"backup-restore", certs.Profile{CommonName: n + "-backup-restore", Usages: serverAuth, DNSNames: slices.Concat(local, forms(n+"-client"), []string{"localhost"}), IPAddresses: loopback}},
		{"client", certs.Profile{CommonName: n + "-client", Usages: clientAuth}},
	}
}

// applySecret writes a Secret unless it already holds what PKI writes. After
// the pass's first failed write it writes nothing, and records a Secret whose
// type or data had to change, or a generation whose labels or owner had to:
// the Datastore sees a generation by them.
func (r *Reconciler) applySecret(ctx context.Context, t *tenant, name string, typ corev1.SecretType, content map[string][]byte, labels map[string]string, immutable bool) {
	cp := t.cp
	labels = maps.Clone(labels)
	if labels == nil {
		labels = map[string]string{}
	}
	labels[bryggav1alpha1.ControlPlaneLabel] = cp.Name
	s := t.existing[name]
	hash := bryggav1alpha1.DataHash(content)
	changes := s == nil || s.Type != typ || !maps.EqualFunc(s.Data, content, bytes.Equal)
	if !changes && t.owns(s) && hasLabels(s, labels) && s.Annotations[bryggav1alpha1.DataHashAnnotation] == hash && (!immutable || isImmutable(s)) {
		return
	}
	if t.writeErr == nil {
		t.writeErr = r.write(ctx, t, name, typ, content, labels, hash, immutable)
	}
	if t.writeErr != nil && (changes || labels[bryggav1alpha1.EtcdCertificatesLabel] != "") {
		t.pending = append(t.pending, name)
	}
}

func (r *Reconciler) write(ctx context.Context, t *tenant, name string, typ corev1.SecretType, content map[string][]byte, labels map[string]string, hash string, immutable bool) error {
	cp := t.cp
	owner := metav1ac.OwnerReference().
		WithAPIVersion(bryggav1alpha1.GroupVersion.String()).
		WithKind(kind).
		WithName(cp.Name).
		WithUID(cp.UID).
		WithController(true).
		WithBlockOwnerDeletion(true)
	annotations := map[string]string{bryggav1alpha1.DataHashAnnotation: hash}
	secret := corev1ac.Secret(name, cp.Namespace).WithLabels(labels).WithAnnotations(annotations).WithOwnerReferences(owner).WithType(typ).WithData(content)
	if immutable {
		secret.WithImmutable(true)
	}
	if err := r.Apply(ctx, secret, client.FieldOwner(fieldManager), client.ForceOwnership); err != nil {
		return fmt.Errorf("apply Secret %s: %w", name, err)
	}
	t.existing[name] = &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cp.Namespace, Labels: labels, Annotations: annotations,
			OwnerReferences: []metav1.OwnerReference{{APIVersion: *owner.APIVersion, Kind: kind, Name: cp.Name, UID: cp.UID, Controller: owner.Controller}}},
		Type:      typ,
		Data:      content,
		Immutable: secret.Immutable,
	}
	return nil
}

func (t *tenant) owns(s *corev1.Secret) bool {
	return slices.ContainsFunc(s.OwnerReferences, func(o metav1.OwnerReference) bool {
		return o.UID == t.cp.UID && o.Controller != nil && *o.Controller
	})
}

func hasLabels(s *corev1.Secret, labels map[string]string) bool {
	for k, v := range labels {
		if s.Labels[k] != v {
			return false
		}
	}
	return true
}
