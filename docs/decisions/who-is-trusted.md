# The management cluster is the platform's and no tenant reaches its API; a tenant's administrator is untrusted inside the tenant

Decided 2026-11-12.
The bounded reads of a tenant, Health's, Tenant's and PKI's, and the chart's policies that refuse the delete of a `ControlPlane`, and of the CRD that holds them, without a confirmation are built, and so is the Cluster API provider's confirmation, given only for its `Cluster`'s deletion ([lodia-io/cluster-api-provider-lodia#46](https://github.com/lodia-io/cluster-api-provider-lodia/pull/46)).

## Decision

- **The management cluster is the platform's.**
  Its API, Lodia's objects and the tenant namespaces with their Secrets are reached by the platform's people and automation, never with a tenant's credentials.
- **A tenant's administrator is untrusted inside the tenant.**
  Lodia reads whatever a tenant's cluster serves as written by an adversary: within bounds that hold whatever it sends, and within a time that ends.
- **Lodia's objects are not given to tenants.**
  A platform that offers self-service puts an API of its own in front of `ControlPlane`, `Backup` and `KubeconfigRequest`; writing them with a tenant's credentials is not supported.
- **Deleting a `ControlPlane` needs a confirmation:** the annotation `lodia.io/deletion-confirmed: "true"` on it, without which the chart's admission policy `controlplanes.lodia.io` refuses the delete.
  Lodia's Cluster API provider sets it when its `Cluster` is deleted.
  A delete while the `ControlPlane`'s namespace is being deleted passes; no other exception exists: a collection's objects, a dry run and the garbage collector's delete of a `ControlPlane` whose owner is gone are refused like any delete ([what each does](../operations.md#delete-a-cluster)).
  The CRD `controlplanes.lodia.io` has a confirmation of its own, the same annotation on it, without which the policy `controlplanes-crd.lodia.io` refuses its delete; a confirmed delete of the CRD deletes every `ControlPlane` without theirs ([steps](../operations.md#deleting-the-controlplane-crd)).
  Nothing clears either annotation.

## Why

- **The management cluster holds every tenant.**
  A tenant namespace holds that tenant's CA keys and its class's backup credentials, which read every tenant's snapshots on the class ([Identity and tenancy](../README.md#identity-and-tenancy)).
  A `ControlPlane` names the `Backup` whose data it restores, the class whose bucket it writes and flags of its API server ([the fields](../api/controlplane.md#spec)), so whoever writes one reaches what its namespace reaches.
- **The threats need no access to Lodia.**
  `cluster-admin` inside a tenant is enough: its administrator writes the Nodes and Leases Health counts, and an APIService naming a Service of their own puts their server in place of a built-in group ([controller-manager-hold](controller-manager-hold.md)), so a read of the tenant can return anything, of any length.
- **A wrong selector or a GitOps prune deletes a tenant.**
  Its control plane goes, and its etcd volumes under a class that reclaims them ([deletion](../README.md#deletion)); a second act costs a platform's automation nothing.
- **Deleting the CRD takes every tenant, past the first policy.**
  The CRD's finalizer deletes each object straight from storage, with no admission ([crd_finalizer.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiextensions-apiserver/pkg/controller/finalizer/crd_finalizer.go#L182-L184), [#L220](https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiextensions-apiserver/pkg/controller/finalizer/crd_finalizer.go#L220)), so the first policy never sees them, whether the CRD is deleted by hand or by a GitOps prune.
  A collection's delete of CRDs removes the CRD at once instead, past the finalizer that only the CRD's own delete adds ([etcd.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiextensions-apiserver/pkg/registry/customresourcedefinition/etcd.go#L83-L84), [store.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiserver/pkg/registry/generic/registry/store.go#L1353)), and its `ControlPlane`s stay in etcd, unserved, until a CRD of that name is created again.
  A policy may match a CRD, only policies and bindings are excluded ([types.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/api/admissionregistration/v1/types.go#L224-L228)), so a second policy guards that one delete.
- **The namespace's own deletion passes, since the namespace controller deletes everything else in it anyway.**
  It deletes each kind by collection and goes on with the others when one is refused ([namespaced_resources_deleter.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/pkg/controller/namespace/deletion/namespaced_resources_deleter.go#L569-L575)), so refusing the `ControlPlane` there would leave one without its objects in a namespace stuck in `Terminating`.
  A policy sees the namespace of the object it admits as `namespaceObject` and the stored object as `oldObject`, for a `DELETE` too ([types.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/api/admissionregistration/v1/types.go#L318-L322)): the dispatcher reads the namespace for every request in a namespace ([dispatcher.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiserver/pkg/admission/plugin/policy/validating/dispatcher.go#L181-L197)) and hands its `deletionTimestamp` to the expression ([condition.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiserver/pkg/admission/plugin/cel/condition.go#L162-L185)).

## Rejected

- Tenants writing `ControlPlane`s in namespaces of their own — they would reach the class's backup credentials and every field that acts outside their cluster.
- Trusting a tenant's administrator — the threats need only `cluster-admin` inside the tenant.
- A guard on namespaces — a policy sees the request, its object and the stored one, its parameters and variables, the namespace and an authorizer alone ([types.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/api/admissionregistration/v1/types.go#L316-L328)), so it cannot tell which namespaces hold tenants, and Gardener's [Namespace Validator](https://github.com/gardener/gardener/blob/v1.155.0/docs/concepts/admission-controller.md#namespace-validator), which can, is a webhook ([no-admission-webhooks](no-admission-webhooks.md)).
  A platform that knows its tenant namespaces guards them with [a policy of its own](../operations.md#deleting-a-namespace-that-holds-tenants).

## Consequences

- Every read of a tenant is bounded in bytes, items and time.
  Health's, Tenant's and PKI's are, through one client ([controller-manager-hold](controller-manager-hold.md), [tenant-clients-speak-http1](tenant-clients-speak-http1.md), [one-client-for-a-tenant](one-client-for-a-tenant.md)).
- The platform's RBAC on the management cluster is the boundary: Secrets in tenant namespaces are readable by Lodia and the platform's administrators only ([Identity and tenancy](../README.md#identity-and-tenancy)).
- A compromised control plane pod reaches the management network, since its egress is not denied yet (`todo.md`).
- The issuers of a tenant's JWT authenticators, which its administrator may name, are fetched through the tenant's nodes, never from the management network ([structured-authentication](structured-authentication.md)).
- The confirmation stops a wrong selector or a prune, not a person who annotates first: a management cluster administrator can also weaken the policies ([no-admission-webhooks](no-admission-webhooks.md#consequences)).
- A Helm uninstall keeps every CRD and every object of theirs, and deletes the policies: until the chart is installed again nothing refuses a delete ([the chart](../README.md#installation)).
- Item 10 of the [review checklist](../review-checklist.md) checks every change against it.

## Code

- `internal/tenantclient` — the bounds every read of a tenant shares ([one-client-for-a-tenant](one-client-for-a-tenant.md#code)); `internal/controller/health` — Health's reads ([controller-manager-hold](controller-manager-hold.md#code)).
- `api/v1alpha1/labels.go` — `DeletionConfirmedAnnotation`.
- `api/policies/controlplanes.yaml` — the policies and their bindings that refuse the delete of a `ControlPlane` and of its CRD, copied into the chart by `make generate`.
- `test/api/policy_test.go` — that file in envtest: a delete refused with the `annotate` command in its message for no annotation, for other annotations alone, for `false`, `True`, an empty value and the key as a label, and admitted for `true` and in a namespace being deleted, which also shows that the policy sees `namespaceObject` on a `DELETE`; a dry run and a collection refused and admitted alike; a create and an update untouched; a dry run of the CRD's delete and a collection's delete of it refused and admitted the same way, and both of another CRD admitted; every policy of the file `Fail`ing and every binding `Deny`ing; the platform's policy of [operations](../operations.md#deleting-a-namespace-that-holds-tenants) refusing and admitting the deletion of a tenant namespace.
- `test/e2e/` — every delete of a `ControlPlane` goes through `deleteControlPlane`, which confirms first, and `TestPhase1Gate` shows the refusal on a running tenant.
