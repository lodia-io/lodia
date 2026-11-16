# The management cluster is the platform's and no tenant reaches its API; a tenant's administrator is untrusted inside the tenant

Decided 2026-11-12.
Health's bounded reads of a tenant are built; the confirmation before a `ControlPlane` is deleted is not.

## Decision

- **The management cluster is the platform's.**
  Its API, Lodia's objects and the tenant namespaces with their Secrets are reached by the platform's people and automation, never with a tenant's credentials.
- **A tenant's administrator is untrusted inside the tenant.**
  Lodia reads whatever a tenant's cluster serves as written by an adversary: within bounds that hold whatever it sends, and within a time that ends.
- **Lodia's objects are not given to tenants.**
  A platform that offers self-service puts an API of its own in front of `ControlPlane`, `Backup` and `KubeconfigRequest`; writing them with a tenant's credentials is not supported.
- **Deleting a `ControlPlane` needs a confirmation:** an annotation on it, without which an admission policy of the chart refuses the delete.
  Lodia's Cluster API provider sets it when its `Cluster` is deleted.

## Why

- **The management cluster holds every tenant.**
  A tenant namespace holds that tenant's CA keys and its class's backup credentials, which read every tenant's snapshots on the class ([Identity and tenancy](../README.md#identity-and-tenancy)).
  A `ControlPlane` names the `Backup` whose data it restores, the class whose bucket it writes and flags of its API server ([the fields](../api/controlplane.md#spec)), so whoever writes one reaches what its namespace reaches.
- **The threats need no access to Lodia.**
  `cluster-admin` inside a tenant is enough: its administrator writes the Nodes and Leases Health counts, and an APIService naming a Service of their own puts their server in place of a built-in group ([controller-manager-hold](controller-manager-hold.md)), so a read of the tenant can return anything, of any length.
- **A delete is one call.**
  A wrong selector or a GitOps prune deletes a tenant's control plane, and its etcd volumes under a class that reclaims them ([deletion](../README.md#deletion)); a second act costs a platform's automation nothing.

## Rejected

- Tenants writing `ControlPlane`s in namespaces of their own — they would reach the class's backup credentials and every field that acts outside their cluster.
- Trusting a tenant's administrator — the threats need only `cluster-admin` inside the tenant.

## Consequences

- Every read of a tenant is bounded in bytes, items and time.
  Health's are ([controller-manager-hold](controller-manager-hold.md), [tenant-clients-speak-http1](tenant-clients-speak-http1.md)); PKI's and Tenant's are not yet, and phase 3 bounds them ([roadmap](../roadmap.md)).
- The platform's RBAC on the management cluster is the boundary: Secrets in tenant namespaces are readable by Lodia and the platform's administrators only ([Identity and tenancy](../README.md#identity-and-tenancy)).
- A compromised control plane pod reaches the management network, since its egress is not denied yet (`todo.md`).
- Item 10 of the [review checklist](../review-checklist.md) checks every change against it.

## Code

- `internal/controller/health` — the bounded reads ([controller-manager-hold](controller-manager-hold.md#code)).
- `charts/lodia/files/policies/` — the admission policies; the one that refuses a delete is not written yet.
