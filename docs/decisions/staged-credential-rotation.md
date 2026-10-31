# Credentials rotate in stages, and the routine ones never touch a node: the trust anchor is split from the client CA, and ServiceAccount tokens are signed by a sidecar

Decided 2026-10-25.
`lodia-signer` is built, and Workloads runs it beside the API server; the PKI controller issues the CA per purpose, the leaves, the kubeconfigs, the ServiceAccount key and the encryption configuration, renews leaves and the request CA at two thirds and the etcd set as generations; the rotation operations are not built.

## Decision

**A CA per purpose**, each a self-signed root: a CA signed by another would let every bundle that trusts the other accept its certificates ([CA reuse](https://github.com/kubernetes/website/blob/snapshot-initial-v1.37/content/en/docs/tasks/extend-kubernetes/configure-aggregation-layer.md#L300-L307)).
Each tenant has:

| CA | signs | who must trust it | lifetime |
|---|---|---|---|
| **cluster CA** (the trust anchor) | the API server's serving certificate, its kubelet client certificate, the tunnel and konnectivity serving certificates | every client and node: kubeconfigs, `cluster-info`, `kube-root-ca.crt`, the kubelet's `clientCAFile` | 10 years |
| **client CA** | every client identity but a person's: admin, controller manager, scheduler, Lodia; and an intermediate *kubelet signer* | only the API server, whose `--client-ca-file` bundle holds the client CA, the current kubelet signer and the request CA | 5 years |
| **request CA** | `KubeconfigRequest` certificates only ([short-lived-kubeconfigs](short-lived-kubeconfigs.md)) | only the API server, in its client bundle, which keeps a replaced request CA, at most the ten newest, until 24 h 5 min after its successor's issuance: the longest a request lives and the backdate | 1 year, as a leaf: renewed at two thirds, and replaced at once when its key Secret is missing or unreadable |
| **front-proxy CA** | the API server's aggregation client certificate | the API server and aggregated API servers | 5 years |
| **etcd CA** | etcd server, peer and the API server's etcd client certificate | etcd and the API server | 5 years |
| **tunnel CA** (gateway mode, from phase 2's wave 1b) | the short-lived client certificate of the in-cluster tunnel, which `apiserver-proxy` and Health present ([api-exposure](api-exposure.md)) | only the tunnel sidecar in each API server pod | 5 years |

The controller manager signs kubelet client certificates with the kubelet signer intermediate only, through the per-signer flags `--cluster-signing-kubelet-client-cert-file` and `-key-file`; no root CA key is ever mounted into a control plane pod.
It does not sign other signers' requests.
kubelet client certificates live 30 days (`--cluster-signing-duration=720h`), and kubelets rotate them on their own ([kube-controller-manager](https://kubernetes.io/docs/reference/command-line-tools-reference/kube-controller-manager/)).

**ServiceAccount tokens** are signed by `lodia-signer`, a sidecar in the API server pod that implements the external JWT signer API over a Unix socket on an `emptyDir` (`--service-account-signing-endpoint`); it serves the signing key and the verification keys from a Secret the PKI controller writes.
The external signer is beta and on by default in 1.35, and GA and locked on since 1.36, so it is on in every minor Lodia supports ([KEP-740](https://github.com/kubernetes/enhancements/blob/master/keps/sig-auth/740-service-account-external-signing/README.md), [`ExternalServiceAccountTokenSigner`](https://github.com/kubernetes/kubernetes/blob/v1.37.0/pkg/features/kube_features.go#L1612-L1616)).
The controller manager gets no ServiceAccount key at all: its `--service-account-private-key-file` only enables legacy secret-based tokens.
Each tenant has its own issuer, `https://<public API host>`.

The Secret `<cp>-sa-keys` is the signer's whole input; a Secret key cannot contain `/`, so its files are flat:

- `signing.key` — exactly one PEM private key (PKCS#8; PKCS#1 RSA and SEC1 EC are accepted): RSA of at least 2048 bits signs RS256, ECDSA P-256, P-384 and P-521 sign ES256, ES384 and ES512, the algorithms kube-apiserver accepts from a signer ([`validateJWTHeader`](https://github.com/kubernetes/kubernetes/blob/v1.35.0/pkg/serviceaccount/externaljwt/plugin/plugin.go#L148-L184));
- `verify-*.pub` — any number of PEM PKIX public keys, published for verification only;
- every other entry is ignored, including the kubelet's `..data` links, through which the signer reads the Secret so that one load never mixes two versions.

The published set is the signing key's public key and every `verify-*.pub`, deduplicated and ordered by key ID; no key is excluded from OIDC discovery.
A key's ID is kube-apiserver's own: the unpadded base64url SHA-256 of its PKIX DER ([`keyIDFromPublicKey`](https://github.com/kubernetes/kubernetes/blob/v1.35.0/pkg/serviceaccount/jwt.go#L99)), so a cluster that moves from a static key file to the signer keeps its tokens valid.
The key set's timestamp changes only when the published set does, and a Secret that does not load leaves the last good set in place: every replica reads the same Secret, so the signer keeps serving and stays ready rather than take every API server out at once.
The three stages of the ServiceAccount key rotation below are three states of this Secret: (1) `signing.key` K₁, `verify-k2.pub` K₂; (2) `signing.key` K₂, `verify-k1.pub` K₁; (3) `signing.key` K₂.

**Leaf certificates** renew at two thirds of their lifetime.
The API server reloads its serving certificate, client CA bundles, kubelet client certificate and, per handshake, its etcd client certificate; components read their kubeconfigs' certificates from mounted files, which client-go reloads; etcd reloads its certificates per connection.
A renewal therefore rolls nothing except two, both in the maintenance window: the konnectivity server's certificate, until a reload is proven, and the certificate of etcd's backup-restore sidecar, which keeps serving its first one.
etcd-druid rolls the members only when the `Etcd` names a new Secret, so the PKI controller renews etcd's certificates together, under new names: one roll, no API outage with three members, about 20 s with one ([etcd spike](../etcd-spike-2026-10.md), finding 3).
Each renewal is a generation of five Secrets ([A tenant namespace](../README.md#a-tenant-namespace)); Datastore points the `Etcd` at the newest complete generation, and PKI keeps the previous one and deletes older ones, so neither controller reads the other's state.

**Rotations**, started with `lodia.io/operation=<name>-start` and, where a stage waits for people, completed with `<name>-complete`; phases in `status.credentials.rotation`:

| rotation | Preparing | Prepared → Completing | touches nodes | completes |
|---|---|---|---|---|
| client CA | CA₂ joins the API server's client bundle (reloaded); Lodia re-issues every client certificate it holds and the admin kubeconfig from CA₂; the kubelet signer moves under CA₂ (the controller manager reloads signing files) | after one signing duration (30 days) every kubelet has renewed under CA₂; CA₁ leaves the bundle | no | on its own, or at once with `-complete` if the owner accepts losing stale kubeconfigs |
| ServiceAccount key | K₂ is published as a verification key; once every replica serves it, K₂ signs and K₁ only verifies | K₁ is dropped after the owner confirms that no client holds an old token | no | `-complete` |
| etcd encryption key | E₂ is added, then made first (the API server reloads the configuration every minute); the Tenant controller rewrites every encrypted resource | E₁ is removed | no | on its own |
| etcd CA, front-proxy CA, tunnel CA | the new CA joins the bundles; certificates are re-issued; etcd and the API server roll where a CA file is read once | the old CA leaves the bundles | no | on its own |
| request CA | CA₂ replaces CA₁ in the client bundle at once, without the 24 h a renewal keeps CA₁, and every `KubeconfigRequest` certificate stops working, which is how they are revoked | none | no | on its own |
| cluster CA (trust anchor) | CA₂ joins every bundle: `cluster-info`, `kube-root-ca.crt` (the controller manager restarts, its `--root-ca-file` is read once), the CA bundle Secret, kubeconfigs; node owners add CA₂ to `/etc/kubernetes/pki/ca.crt` and the kubelet's kubeconfig | serving, kubelet client, tunnel and konnectivity certificates move to CA₂; CA₁ leaves the bundles | **yes** | `-complete`, after the owner updated every node |

Only the last row needs people.
It is meant for a compromised or expiring trust anchor, once in a decade; every other rotation is routine.

## Why

- **Every project we studied makes CA rotation a manual event, and most make it touch every node or break existing kubeconfigs** ([analysis](../analysis.md)).
  The reason is that one CA usually both anchors trust and signs identities, so rotating identities means re-anchoring trust everywhere.
  Splitting the two is what HyperShift's documented CA hierarchy does ([control plane certificates](https://github.com/openshift/hypershift/blob/main/docs/content/how-to/pki/control-plane-certificates.md)), and it means the routine rotation (client identities) never reaches a node.
- **Nodes Lodia does not manage cannot be re-anchored by Lodia.**
  A node's trust is in files kubeadm wrote at join; only its owner can change them.
  Keeping those files valid across routine rotations is the only rotation model that works for nodes anywhere.
- **The API server reloads almost everything now**: serving certificates and client CA bundles through dynamic file controllers, the encryption configuration with automatic reload, structured authentication and authorization configuration ([kube-apiserver](https://kubernetes.io/docs/reference/command-line-tools-reference/kube-apiserver/), [encrypt data](https://kubernetes.io/docs/tasks/administer-cluster/encrypt-data/)).
  The ServiceAccount key was the exception — read once at startup, the reason KEP-740 exists — and the external signer removes it.
- **The root keys stay out of every pod.**
  The controller manager holds only the kubelet signer, a one-year intermediate.
  That does not narrow what it can sign: the API server accepts any certificate that chains to its client bundle, with the user from its CN and the groups from its O ([x509.go](https://github.com/kubernetes/kubernetes/blob/v1.37.0/staging/src/k8s.io/apiserver/pkg/authentication/request/x509/x509.go#L288-L310)), so whoever holds the signer can sign any identity while it is in the bundle, that is until it is replaced and its 30 days beside the new one are over.
- **The request CA is replaced at once, like the kubelet signer, where the other CAs never are while their bundle proves them.**
  A new CA in place of a lost one cuts off whatever trusts or presents the old one; the client bundle keeps a replaced request CA while what it signed lives, so its replacement cuts off nothing, and refusing to replace a lost one would leave the tenant without access for people over one deleted Secret.
  The cap of ten keeps a Secret deleted again and again from growing the bundle without bound.
- **Stages with both credentials valid** are how Gardener rotates without outage ([credentials rotation](https://github.com/gardener/gardener/blob/master/docs/usage/shoot-operations/shoot_credentials_rotation.md)); Lodia keeps the stages and their names.

## Rejected

- One cluster CA for everything (the kubeadm default) — every rotation becomes a node event.
- Mounting the root CA key into the controller manager — the default kubeadm layout; a compromised pod signs anything.
- The static multi-key ServiceAccount setup — works, but each step is a rolling restart of every API server.
- Automatic completion of the ServiceAccount key rotation — tokens extended for legacy clients can live a year; the owner decides when old ones may stop working.

## Consequences

- `lodia-signer` is a Lodia image in every API server pod; all replicas read the same Secret, and the PKI controller waits for every replica to report the new key set before switching the signing key.
- The phase 3 gate runs every rotation with two nodes `Ready` and a workload calling the API with a projected token throughout ([roadmap](../roadmap.md)).
- Kubelet serving certificates stay self-signed by default (no `serverTLSBootstrap`), so no CSR approver is needed in the tenant; turning it on is a later option with its own approver.
- More than ten replacements of the request CA within a day drop the oldest kept one early, which cuts off the certificates it signed.
- A certificate issued right after a new request CA is refused for a minute or two: the kubelet updates the mounted `<cp>-client-ca` within about a minute ([spike d](../hard-way.md#d-spike-serviceaccount-tokens-from-an-external-signer)), and the API server reloads it within another ([dynamic_cafile_content.go](https://github.com/kubernetes/kubernetes/blob/v1.37.0/staging/src/k8s.io/apiserver/pkg/server/dynamiccertificates/dynamic_cafile_content.go#L39)).
- Every CA but the kubelet signer and the request CA, which PKI renews, is announced a year before it expires, through [`CAValidityAcceptable`](../api/controlplane.md#conditions-and-their-reasons) and an alert, because its rotation is started by someone.

## Code

- `cmd/lodia-signer` — the sidecar: the socket, the keys directory, the probes, `--max-token-expiration` (8760h, so the API server keeps upstream's one-year extension) and `--refresh-hint` (1m).
- `internal/signer` — the `ExternalJWTSigner` service of [`k8s.io/externaljwt/apis/v1`](https://github.com/kubernetes/externaljwt/blob/v0.37.1/apis/v1/api.proto), the file contract above, and the reload on a changed directory.
  `signer_test.go` holds each invariant for RSA and ECDSA keys; `TestRotationUnderARunningAPIServer` in `apiserver_test.go` runs the three stages against one envtest kube-apiserver whose only signer it is.
- [The hard way, spike d](../hard-way.md#d-spike-serviceaccount-tokens-from-an-external-signer) — the same rotation by hand in kind, with a pod calling the API throughout.
- `internal/certs` — the CAs, leaves, bundles and kubeconfigs whose validity, renewal and order [A tenant namespace](../README.md#a-tenant-namespace) gives, from `crypto/x509`; PEM through client-go's `util/cert` and `util/keyutil`; every time is the caller's.
  `certs_test.go` holds the validity, the bundle order and that a pass issues from one clock.
- `internal/controller/pki` — the controller.
  `secrets.go` writes the Secrets of [A tenant namespace](../README.md#a-tenant-namespace): the kubelet signer is matched by its Secret rather than its subject, and a new one is written only once the client CA bundle trusts it; the bundle knows a request CA as a root under its subject and holds only a stored one, so a new request CA is written last in the pass and joins the bundle on the next, which the kubeconfig request reconciler waits for, and a refused write of its Secret, the one new on upgrade, blocks no other; the encryption configuration is written just before it, so that a provider PKI cannot render (`kms`) blocks nothing else; the etcd generations are immutable, each written once, and one not holding what it needs is replaced by the next, the newest complete generation below it kept.
  A CA other than the kubelet signer and the request CA is created only for a tenant that has neither its key Secret nor its bundle.
  `CAValidityAcceptable` announces a CA in its last year (`CAExpiryNotice`, a year in `internal/operator`).
  `reconciler_test.go` and `recovery_test.go` prove each of these against a running envtest API server: the subjects, usages, SANs and validity; that no Secret but `<cp>-cakey-*` and `<cp>-kubelet-signer` holds a CA's key; that a pass over an unchanged `ControlPlane` changes no `resourceVersion` and a moved `status.endpoint` re-issues exactly the serving certificate, the konnectivity server certificate and the admin kubeconfig; the renewal at two thirds with the CA key pairs other than the request CA's unchanged, and the old signer and request CA still in the bundle under their own subjects; a leaf clamped to its CA issued once, and after the CA expires a pass that writes nothing; etcd generations 1, 2 and 3 with 1 deleted after the third, a generation replaced, not repaired, when a member is deleted, held by a finalizer or foreign, and a partly written generation deleted while the last complete one is kept; a lost CA left in place and an unlabelled one kept; the client bundle written before the signer; the notice; `kms` reported with everything else issued; a renewal or an adoption that could not be written.
  `requestca_test.go` holds the request CA created for a tenant without one, replaced when its Secret is deleted, unreadable, a leaf, the kubelet signer, an intermediate under its name, a root under another or one under its name without a number, with its predecessor kept, adopted without labels, written last, so that five passes that cannot write it leave the bundle as it was while a due kubelet signer and the leaves renew, then stored by the next pass and added by the next whose bundle write succeeds, and each bundle rule keeping its own predecessors and not the other's: a request CA replaced 24 h 2 min ago kept and 24 h 6 min ago dropped, a signer replaced 31 days ago dropped; each request CA kept by its own successor's issuance, the newest ten after twenty replacements and through a pass that replaces none, and a foreign root, another client CA, a request CA newer than the current one or a twin of it, an intermediate under its name and a leaf in the old bundle dropped, with stamps compared as numbers.
  `ownership_test.go` holds a predecessor's Secrets and other owners' left alone, leftovers under PKI's names, adopted generations, unreadable key material kept, and failed reads that leave the status.
  `encryption_test.go` holds a new key's name, `key-<unix nanoseconds>` of its pass; a provider changed back that gets a key name of its own; and an existing `key1` kept as it is.
