# People get short-lived kubeconfigs through KubeconfigRequest; the standing admin credential belongs to a revocable group, not to system:masters

Decided 2026-10-25. Not built.

## Decision

- A person who needs access creates a `KubeconfigRequest` naming the
  `ControlPlane`, the groups to carry and an expiration (default 1 h, maximum
  set by the operator configuration). A schema rule refuses a user or group
  that starts with `system:`, the prefix Kubernetes keeps for its own
  identities: nodes, components, ServiceAccounts
  ([authentication configuration](https://kubernetes.io/docs/reference/access-authn-authz/authentication/#api-server-authn-config)),
  and a user that starts with `brygga:`, where Brygga's own identity,
  `brygga:operator`, lives. The PKI controller issues a client
  certificate with that lifetime, writes a kubeconfig into a Secret named in
  `status.secretRef`, and deletes both the Secret and the request after
  expiry. Creating a `KubeconfigRequest` needs `create` on that resource in
  the tenant's namespace, which is how the platform decides who may ask.
- Every `ControlPlane` also gets `<name>-admin-kubeconfig`: a client
  certificate in the group `brygga:cluster-admins`, bound to `cluster-admin`
  by a ClusterRoleBinding the Tenant controller maintains. Removing that
  binding revokes every admin kubeconfig at once. Platforms on top use this
  credential for automation (registering the cluster in Argo CD, Sveltos or
  their own services); people use requests.
- No stored certificate carries `system:masters`. The API server's kubelet
  client certificate is in `brygga:kubelet-api-admins`, bound to the built-in
  `system:kubelet-api-admin` role. The ClusterRoleBindings for Brygga, the
  admin group and the kubelet client are created at bootstrap with a
  `system:masters` certificate PKI issues for ten minutes, uses and discards;
  the same certificate is the break-glass path when a binding was deleted.
  PKI does this once the API server is ready, checks the bindings with
  Brygga's own credential (`brygga:operator`, in
  `<name>-brygga-kubeconfig`, which dials the in-namespace Service) on every
  pass, and reports them in its condition `AccessReady`, which Tenant and
  Health wait on; `CertificatesReady` never waits for the API server, so the
  API server never waits for its own bindings.

## Why

- A certificate in `system:masters` cannot be revoked: the API server
  short-circuits authorization for the group, so a leaked admin kubeconfig is
  valid until the CA rotates. kubeadm moved its `admin.conf` to the group
  `kubeadm:cluster-admins` and keeps `system:masters` only in the break-glass
  `super-admin.conf`; the same page notes that the API server's kubelet client
  does not need the super-user group either
  ([certificate best practices](https://kubernetes.io/docs/setup/best-practices/certificates/)).
  The built-in role `system:kubelet-api-admin` exists for exactly that client
  ([RBAC default roles](https://kubernetes.io/docs/reference/access-authn-authz/rbac/)).
- Gardener stopped issuing static admin kubeconfigs and serves
  `AdminKubeconfigRequest` with an expiration instead
  ([shoot access](https://github.com/gardener/gardener/blob/master/docs/usage/shoot/shoot_access.md));
  a CRD cannot offer a custom subresource, so a request object with a status
  is the equivalent shape, as cert-manager's `CertificateRequest`.

## Rejected

- Only short-lived access, no standing admin credential — automation on top
  needs a credential it can store; a revocable group is the compromise.
- OIDC as the only way in — Brygga supports structured authentication
  configuration for it, but a cluster must be reachable before an identity
  provider is wired.

## Consequences

- The kubeconfig Secret is readable by whoever can read Secrets in the
  namespace; the platform's RBAC decides, as for any Secret.
- Expiry cleanup is a small controller of its own inside PKI; a request
  whose `ControlPlane` is hibernated is answered, but the API is not reachable
  until wake-up, and `status` says so.

## Code

none yet.
