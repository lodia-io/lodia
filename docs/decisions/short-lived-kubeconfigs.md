# People get short-lived kubeconfigs for keys of their own, in their own name, through KubeconfigRequest; the standing admin credential belongs to a group RBAC governs, not to system:masters

Decided 2026-10-30.
The admin kubeconfig in `lodia:cluster-admins`, Lodia's own credential, the three bindings made with a ten-minute `system:masters` certificate, the `KubeconfigRequest` contract, its admission policy, the request CA and the reconciler that answers requests are built.

## Decision

- A person who needs access keeps a key of their own and creates a `KubeconfigRequest` with a PEM PKCS#10 request for it, the `ControlPlane`, their own username and an expiration ([fields](../api/controlplane.md#kubeconfigrequest)).
  Lodia uses the request's public key only: no key is generated or stored, so the kubeconfig in the request's status has none.
- The certificate's user is `lodia:user:<user>`.
  With `access: ClusterAdmin` it is also in `lodia:cluster-admins`; without it, it carries no group, and the tenant's RBAC on that user decides.
  The prefix keeps every requested name apart from Kubernetes' and Lodia's own (`system:…`, `kubernetes-admin`, `lodia:operator`), so no name needs a deny list, and a management ServiceAccount can ask too.
- The chart's ValidatingAdmissionPolicy `kubeconfigrequests.lodia.io` decides who may ask for what ([no-admission-webhooks](no-admission-webhooks.md)).
  `spec.user` must be the requester's username, and `ClusterAdmin` needs `create` on `controlplanes/adminkubeconfig` for that `ControlPlane`: a permission, not a served subresource, which a Role grants by `resourceNames`.
  Asking at all needs `create` on `kubeconfigrequests` in the namespace.
- The reconciler issues only when the policy and its binding exist under that name, the binding names the policy with `Deny` and no `matchResources` or `paramRef`, and the request was created at least 10 s after both; otherwise the request is `PolicyNotEnforced`, for good.
- The request CA, a root of its own per `ControlPlane` in the API server's client bundle, signs these certificates and nothing else ([staged-credential-rotation](staged-credential-rotation.md)).
- The spec never changes.
  The request is deleted at `creationTimestamp + expirationSeconds`, issued or not, and the certificate expires then; deleting the request earlier revokes nothing.
- Lodia logs each certificate's SHA-256 fingerprint with the request's namespace, name and UID when it issues; the tenant's audit log records the same fingerprint as the user's `authentication.kubernetes.io/credential-id` ([x509.go](https://github.com/kubernetes/kubernetes/blob/v1.35.10/staging/src/k8s.io/apiserver/pkg/authentication/request/x509/x509.go#L285-L299), [request.go](https://github.com/kubernetes/kubernetes/blob/v1.35.10/staging/src/k8s.io/apiserver/pkg/audit/request.go#L61-L69)), which joins the two logs.
- Every `ControlPlane` also gets `<name>-admin-kubeconfig`: a client certificate in the group `lodia:cluster-admins`, bound to `cluster-admin` by a ClusterRoleBinding PKI maintains, the one writer of that binding.
  Deleting the binding revokes nothing for long: PKI applies it again on its next pass; every admin kubeconfig is revoked by the client CA rotation (phase 3).
  Platforms on top use this credential for automation (registering the cluster in Argo CD, Sveltos or their own services); people use requests.
- No stored certificate carries `system:masters`.
  The API server's kubelet client certificate is in `lodia:kubelet-api-admins`, bound to the built-in `system:kubelet-api-admin` role.
  The ClusterRoleBindings for Lodia, the admin group and the kubelet client are created at bootstrap with a `system:masters` certificate PKI issues for ten minutes, uses and discards; the same certificate is the break-glass path when a binding was deleted.
  PKI does this once the API server is ready, checks the bindings with Lodia's own credential (`lodia:operator`, in `<name>-lodia-kubeconfig`, which dials the in-namespace Service) on every pass, and reports them in its condition `AccessReady`, which Tenant and Health wait on; `CertificatesReady` never waits for the API server, so the API server never waits for its own bindings.

## Why

- A request names its requester truthfully only if something besides the requester checks it.
  A CRD's rules see only the object and its old version ([compilation.go](https://github.com/kubernetes/kubernetes/blob/v1.35.10/staging/src/k8s.io/apiextensions-apiserver/pkg/apiserver/schema/cel/compilation.go#L153-L200)); an admission policy also sees the requester ([no-admission-webhooks](no-admission-webhooks.md)).
  Gardener's `shoots/adminkubeconfig` names its certificate after the requester ([kubeconfig.go](https://github.com/gardener/gardener/blob/v1.154.1/pkg/apiserver/registry/core/shoot/storage/kubeconfig.go#L164-L165)), and the CSR API records the requester itself ([strategy.go](https://github.com/kubernetes/kubernetes/blob/v1.35.10/pkg/registry/certificates/certificates/strategy.go#L78-L97)).
- A key in a Secret would break the tenancy rule: Secrets in tenant namespaces are readable by Lodia and the platform's administrators only ([Identity and tenancy](../README.md#identity-and-tenancy)), so the person could not read it, and whoever could would also read the CA keys beside it.
- People's certificates can be revoked apart from every component's only if they chain to a root of their own: the API server takes the certificates a client sends after its own as intermediates ([x509.go](https://github.com/kubernetes/kubernetes/blob/v1.35.10/staging/src/k8s.io/apiserver/pkg/authentication/request/x509/x509.go#L149-L154)), so dropping an intermediate whose root stays in the bundle revokes nothing.
  HyperShift separates its client CAs into trust domains of their own ([trust-chain-separation.md](https://github.com/openshift/enhancements/blob/5f520506401067ee54ab394dbfc6305336b7a52f/enhancements/hypershift/trust-chain-separation.md#L64-L67)).
- A missing policy admits everything ([no-admission-webhooks](no-admission-webhooks.md#consequences)), and it can be missing.
  Helm installs kinds it does not list, admission policies among them, after the Deployment ([kind_sorter.go](https://github.com/helm/helm/blob/v4.3.1/pkg/release/v1/util/kind_sorter.go#L31-L70), [#L156-L159](https://github.com/helm/helm/blob/v4.3.1/pkg/release/v1/util/kind_sorter.go#L156-L159)).
  Each API server checks every second for changed policies and recompiles them then ([policy_source.go](https://github.com/kubernetes/kubernetes/blob/v1.35.10/staging/src/k8s.io/apiserver/pkg/admission/plugin/policy/generic/policy_source.go#L198-L205)), so a binding is enforced about a second after it is written (0.8 s in envtest at 1.35); the 10 s cover that.
- A certificate in `system:masters` cannot be revoked: the API server short-circuits authorization for the group, so a leaked admin kubeconfig is valid until the CA rotates.
  kubeadm moved its `admin.conf` to the group `kubeadm:cluster-admins` and keeps `system:masters` only in the break-glass `super-admin.conf`; the same page notes that the API server's kubelet client does not need the super-user group either ([certificate best practices](https://kubernetes.io/docs/setup/best-practices/certificates/)).
  The built-in role `system:kubelet-api-admin` exists for exactly that client ([RBAC default roles](https://kubernetes.io/docs/reference/access-authn-authz/rbac/)).

## Rejected

- The CSR API with a Lodia signer — CSRs are cluster-scoped ([strategy.go](https://github.com/kubernetes/kubernetes/blob/v1.35.10/pkg/registry/certificates/certificates/strategy.go#L49-L52)), so every requester reads every request, every kubelet's included, and a namespace's RBAC cannot delegate them.
- An aggregated API server, Gardener's shape — a second API server, and while it is down no namespace in the cluster finishes deleting: the namespace controller keeps a failed discovery's error ([namespaced_resources_deleter.go](https://github.com/kubernetes/kubernetes/blob/v1.35.10/pkg/controller/namespace/deletion/namespaced_resources_deleter.go#L512-L517)) and finalizes a namespace only after a pass without one ([#L136-L145](https://github.com/kubernetes/kubernetes/blob/v1.35.10/pkg/controller/namespace/deletion/namespaced_resources_deleter.go#L136-L145)).
- A key Lodia generates, in a Secret or in status — the tenancy rule above, and the key would sit in etcd and in every backup.
- Free-form groups — a deny list leaves `system:nodes`, `kubeadm:cluster-admins` and anything bound later, and an `authorizer` check per group costs 350,000 of an expression's 1,000,000 ([cost.go](https://github.com/kubernetes/kubernetes/blob/v1.35.10/staging/src/k8s.io/apiserver/pkg/cel/library/cost.go#L88-L92), [config.go](https://github.com/kubernetes/kubernetes/blob/v1.35.10/staging/src/k8s.io/apiserver/pkg/apis/cel/config.go#L20-L22)); roles beyond cluster-admin are the tenant's RBAC on `lodia:user:<user>`.
- The requester's management groups in the certificate — they mean nothing in the tenant, and `system:masters` there would be `system:masters` here.
- The request's UID as the certificate's user UID — a user's UID names the user across credentials ([user.go](https://github.com/kubernetes/kubernetes/blob/v1.35.10/staging/src/k8s.io/apiserver/pkg/authentication/user/user.go#L24-L27)), and `credential-id` already names the credential.
- Only short-lived access, no standing admin credential — automation on top needs a credential it can store; a group RBAC governs is the compromise.
- OIDC as the only way in — Lodia supports structured authentication configuration for it, but a cluster must be reachable before an identity provider is wired.

## Consequences

- A management cluster administrator can delete or weaken the policy, which no ValidatingAdmissionPolicy can protect ([types.go](https://github.com/kubernetes/kubernetes/blob/v1.35.10/staging/src/k8s.io/api/admissionregistration/v1/types.go#L220-L223)); such an administrator reads the tenant's CA keys anyway.
- The requester fills `spec.user` themselves (`kubectl auth whoami`): a MutatingAdmissionPolicy could fill it, but it is beta and off in 1.35 ([kube_features.go](https://github.com/kubernetes/kubernetes/blob/v1.35.10/staging/src/k8s.io/apiserver/pkg/features/kube_features.go#L407-L410)).
- Revoking every person's certificate of a `ControlPlane` is a rotation of the request CA alone ([staged-credential-rotation](staged-credential-rotation.md)).
- A kubectl credential plugin, requests on behalf of another user and a viewer level are not built ([todo.md](../../todo.md)).

## Code

- `api/v1alpha1/access_types.go` — the `KubeconfigRequest` contract; `names.go` — `KubeconfigRequestPolicy`.
- `charts/lodia/files/policies/kubeconfigrequests.yaml` — the policy and its binding, which `templates/policies.yaml` renders with the chart's labels and fixed names, whether or not the chart installs the CRDs.
- `test/api/policy_test.go` — that policy file in envtest, with envtest's RBAC: alice is admitted for herself and refused for bob, by a message naming alice; `ClusterAdmin` is refused to alice, admitted to carol for the `ControlPlane` her Role names and refused for another; dave, who may not create requests, is refused by RBAC.
  `validation_test.go` holds the schema's bounds, the refusal of a request that is not exactly one PEM certificate request, and of a user with a control character, and that the spec cannot change.
- `internal/controller/pki/kubeconfigrequest.go` — the kubeconfig request reconciler: the policy check through the API reader, the request's key, the certificate from the request CA, the kubeconfig, the status written with the `resourceVersion` the pass read, the fingerprint logged after that write, and the deletion at the deadline with a UID precondition; it watches the `ControlPlane`s its requests name and their request CA and cluster CA bundle, so a wait ends when its input is written; the grace is 10 s in `internal/operator`.
  `kubeconfigrequest_test.go` runs it as PKI's ClusterRole: the certificate's subject, usages, validity from the deadline, key and chain to the request CA in `<cp>-client-ca`, with the request's own subject and extensions ignored; the kubeconfig's server, CA and missing key; each key type, RSA of 1024 and 8193 bits and P-224 refused; each reason, the final ones staying final once the policy is back; `PolicyNotEnforced` without the policy or binding, with a binding in `Warn`, with `matchResources`, a `paramRef` or another policy's name, and with either object newer than the request; a stale pass that conflicts and logs nothing; the deletion at the deadline and not of a request recreated under the name; a paused or deleting `ControlPlane` answered and not written; the waits ended by their inputs under a manager; a transient read or a failed write answering nothing.
  `kubeconfigrequest_internal_test.go` renders the largest kubeconfig `status.kubeconfig` can be asked to hold.
- `internal/controller/pki/apiserver_test.go` — a request's kubeconfig with the requester's key authenticates to the tenant's kube-apiserver as `lodia:user:<user>`, in `lodia:cluster-admins` and listing nodes only with `ClusterAdmin`, under the `credential-id` the reconciler logged.
- `internal/controller/pki/secrets.go` — `<name>-admin-kubeconfig` (`kubernetes-admin` in `lodia:cluster-admins`, the public endpoint, the certificates embedded) and `<name>-lodia-kubeconfig` (`lodia:operator`, the in-namespace Service).
- `internal/controller/pki/access.go` — the three ClusterRoleBindings, applied as `lodia:operator` on every pass and, when that is forbidden, with a `system:masters` certificate issued for ten minutes in memory, and `AccessReady` ([reasons](../api/controlplane.md#conditions-and-their-reasons)).
- `internal/controller/pki/apiserver_test.go` — a kube-apiserver from the envtest binaries started with PKI's serving certificate, client CA bundle, front-proxy files and signing key: the controller manager's kubeconfig authenticates as `system:kube-controller-manager`; `AccessReady` turns True with the three bindings and stays True through a pass that cannot reach the tenant; a deleted `lodia:operator` binding is restored on the next pass, and a binding an admission policy refuses turns it False, `Refused`.
