# People get short-lived kubeconfigs through KubeconfigRequest; the standing admin credential belongs to a revocable group, not to system:masters

Decided 2026-10-25.
The admin kubeconfig in `lodia:cluster-admins`, Lodia's own credential and the three bindings made with a ten-minute `system:masters` certificate are built in the PKI controller; `KubeconfigRequest` is not.

## Decision

- A person who needs access creates a `KubeconfigRequest` naming the `ControlPlane`, the groups to carry and an expiration (default 1 h, maximum set by the operator configuration).
  A schema rule refuses a user or group that starts with `system:`, the prefix Kubernetes keeps for its own identities: nodes, components, ServiceAccounts ([authentication configuration](https://kubernetes.io/docs/reference/access-authn-authz/authentication/#api-server-authn-config)), and a user that starts with `lodia:`, where Lodia's own identity, `lodia:operator`, lives.
  The PKI controller issues a client certificate with that lifetime, writes a kubeconfig into a Secret named in `status.secretRef`, and deletes both the Secret and the request after expiry.
  Creating a `KubeconfigRequest` needs `create` on that resource in the tenant's namespace, which is how the platform decides who may ask.
- Every `ControlPlane` also gets `<name>-admin-kubeconfig`: a client certificate in the group `lodia:cluster-admins`, bound to `cluster-admin` by a ClusterRoleBinding PKI maintains, the one writer of that binding.
  Deleting the binding revokes nothing for long: PKI applies it again on its next pass; every admin kubeconfig is revoked by the client CA rotation (phase 3).
  Platforms on top use this credential for automation (registering the cluster in Argo CD, Sveltos or their own services); people use requests.
- No stored certificate carries `system:masters`.
  The API server's kubelet client certificate is in `lodia:kubelet-api-admins`, bound to the built-in `system:kubelet-api-admin` role.
  The ClusterRoleBindings for Lodia, the admin group and the kubelet client are created at bootstrap with a `system:masters` certificate PKI issues for ten minutes, uses and discards; the same certificate is the break-glass path when a binding was deleted.
  PKI does this once the API server is ready, checks the bindings with Lodia's own credential (`lodia:operator`, in `<name>-lodia-kubeconfig`, which dials the in-namespace Service) on every pass, and reports them in its condition `AccessReady`, which Tenant and Health wait on; `CertificatesReady` never waits for the API server, so the API server never waits for its own bindings.

## Why

- A certificate in `system:masters` cannot be revoked: the API server short-circuits authorization for the group, so a leaked admin kubeconfig is valid until the CA rotates.
  kubeadm moved its `admin.conf` to the group `kubeadm:cluster-admins` and keeps `system:masters` only in the break-glass `super-admin.conf`; the same page notes that the API server's kubelet client does not need the super-user group either ([certificate best practices](https://kubernetes.io/docs/setup/best-practices/certificates/)).
  The built-in role `system:kubelet-api-admin` exists for exactly that client ([RBAC default roles](https://kubernetes.io/docs/reference/access-authn-authz/rbac/)).
- Gardener stopped issuing static admin kubeconfigs and serves `AdminKubeconfigRequest` with an expiration instead ([shoot access](https://github.com/gardener/gardener/blob/master/docs/usage/shoot/shoot_access.md)); a CRD cannot offer a custom subresource, so a request object with a status is the equivalent shape, as cert-manager's `CertificateRequest`.

## Rejected

- Only short-lived access, no standing admin credential — automation on top needs a credential it can store; a revocable group is the compromise.
- OIDC as the only way in — Lodia supports structured authentication configuration for it, but a cluster must be reachable before an identity provider is wired.

## Consequences

- The kubeconfig Secret is readable by whoever can read Secrets in the namespace; the platform's RBAC decides, as for any Secret.
- Expiry cleanup is a small controller of its own inside PKI; a request whose `ControlPlane` is hibernated is answered, but the API is not reachable until wake-up, and `status` says so.

## Code

- `internal/controller/pki/secrets.go` — `<name>-admin-kubeconfig` (`kubernetes-admin` in `lodia:cluster-admins`, the public endpoint, the certificates embedded) and `<name>-lodia-kubeconfig` (`lodia:operator`, the in-namespace Service).
- `internal/controller/pki/access.go` — the three ClusterRoleBindings, applied as `lodia:operator` on every pass and, when that is forbidden, with a `system:masters` certificate issued for ten minutes in memory, and `AccessReady` ([reasons](../api/controlplane.md#conditions-and-their-reasons)).
- `internal/controller/pki/apiserver_test.go` — a kube-apiserver from the envtest binaries started with PKI's serving certificate, client CA bundle, front-proxy files and signing key: the controller manager's kubeconfig authenticates as `system:kube-controller-manager`; `AccessReady` turns True with the three bindings and stays True through a pass that cannot reach the tenant; a deleted `lodia:operator` binding is restored on the next pass, and a binding an admission policy refuses turns it False, `Refused`.
