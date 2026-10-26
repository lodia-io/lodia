# What we learned from existing solutions

Before designing Lodia we studied how the projects that run Kubernetes
control planes as a service do it: what they host where, how they store,
back up, rotate, upgrade and expose control planes, and what their users run
into. This page is the summary; every statement links the project's own
documentation, code or issue tracker. Survey date: 2026-10-23.

We describe problems as they appear across the category, not as faults of
one project: most of them are hard trade-offs that every project resolves
differently, and the projects below are the reason this space works at all.

## What we studied

| project | what it is | nodes |
|---|---|---|
| [Kamaji](https://kamaji.clastix.io/concepts/tenant-control-plane/) | control planes as Deployments on a management cluster; etcd, SQL or NATS datastores | `kubeadm join`, or Cluster API |
| [Gardener](https://gardener.cloud/docs/gardener/concepts/architecture/) | a full managed Kubernetes platform: control planes in seed namespaces, etcd per shoot through etcd-druid | machines Gardener creates |
| [HyperShift](https://github.com/openshift/hypershift/blob/main/ARCHITECTURE.md) | hosted OpenShift control planes, one control-plane-operator per hosted cluster | NodePools through Cluster API, or bring your own |
| [k0smotron](https://docs.k0smotron.io/stable/) | k0s control planes as StatefulSets, a Cluster API provider | k0s join tokens, Cluster API, SSH machine pools |
| [vCluster with private nodes](https://www.vcluster.com/docs/vcluster/deploy/worker-nodes/private-nodes/) | a virtual cluster control plane in one container that real nodes join | a kubeadm-based join script |
| [Kubermatic KKP](https://docs.kubermatic.com/kubermatic/main/architecture/) | seed clusters hosting user-cluster control planes, etcd per cluster | machine-controller |
| [Cluster API KubeadmControlPlane](https://github.com/kubernetes-sigs/cluster-api/blob/main/docs/proposals/20191017-kubeadm-based-control-plane.md) | the baseline: control planes on machines with stacked etcd | Cluster API |
| [Talos Omni](https://github.com/siderolabs/omni) | the reference for "we run your cluster's lifecycle" on Talos machines | Talos over a WireGuard link |
| [Cozystack](https://cozystack.io/docs/v1.6/kubernetes/) | a platform built on a hosted control plane operator with etcd per tenant | KubeVirt VMs through Cluster API |
| [k3k](https://github.com/rancher/k3k) | k3s control planes in pods, shared or virtual modes | agent pods or external workers |

## What we found across the category

**A change to one secret restarts the whole control plane.** Renewing a
certificate or changing a Secret commonly rolls every control plane
component, or replaces machines: k0smotron stamps a certificate fingerprint
onto the pod template
([certificates](https://docs.k0smotron.io/stable/certificates/)), vCluster's
rotation restarts the tenant cluster
([certificates](https://www.vcluster.com/docs/vcluster/manage/certificates)),
and KubeadmControlPlane rotates certificates by rolling machines
([auto-rotate](https://cluster-api.sigs.k8s.io/tasks/certs/auto-rotate-certificates-in-kcp)).
With every component in one pod, the API server restarts for a scheduler
flag.

**Nobody rotates a CA automatically, and a rotation often means touching
every node.** Gardener rotates in manual stages and rolls nodes
([credentials rotation](https://gardener.cloud/docs/gardener/shoot-operations/shoot_credentials_rotation/)),
HyperShift's root CAs are never auto-rotated
([control plane certificates](https://github.com/openshift/hypershift/blob/main/docs/content/how-to/pki/control-plane-certificates.md)),
k0smotron does not renew CAs
([certificates](https://docs.k0smotron.io/stable/certificates/)), vCluster's
CA rotation breaks existing kubeconfigs and joined nodes must re-join
([certificates](https://www.vcluster.com/docs/vcluster/manage/certificates)),
and Cluster API calls it non-trivial
([custom certificates](https://cluster-api.sigs.k8s.io/tasks/certs/using-custom-certificates)).

**A shared datastore makes tenant separation a property of code.** etcd has
one quota for the whole cluster and no quota per key prefix
([maintenance](https://etcd.io/docs/v3.6/op-guide/maintenance/)), so one
tenant's growth is every tenant's outage; prefix permissions are correct only
if every range end and every name-to-prefix mapping is
([RBAC](https://etcd.io/docs/v3.6/op-guide/authentication/rbac/)). Projects
that run hosted control planes at scale give each tenant its own etcd
(Gardener, HyperShift, KKP, Cozystack).

**Storage and topology choices are hard to change later.** A backing store
cannot change in place in vCluster
([backing store](https://www.vcluster.com/docs/vcluster/configure/vcluster-yaml/control-plane/components/backing-store/)),
a NATS store's size is fixed in k0smotron
([NATS](https://docs.k0smotron.io/stable/nats/)), KKP's expose strategy and
HyperShift's etcd type are immutable
([expose strategies](https://docs.kubermatic.com/kubermatic/main/tutorials-howtos/networking/expose-strategies/),
[API](https://github.com/openshift/hypershift/blob/main/docs/content/reference/api.md)).

**Admission webhooks couple availability.** A webhook with a failing
backend blocks what it guards; Gardener even reports user webhooks that block
hibernation or maintenance as a shoot constraint
([shoot status](https://github.com/gardener/gardener/blob/master/docs/usage/shoot/shoot_status.md)).
An operator whose own webhooks fail closed turns its outage into an outage
of every object it validates.

**Backups cover less than operators expect.** Snapshots that omit volumes
and certificates
([vCluster backup](https://www.vcluster.com/docs/vcluster/manage/backup-restore/)),
restore only at creation and with the saved encryption key
([HyperShift](https://hypershift-docs.netlify.app/how-to/aws/etc-backup-restore/)),
restores that pause the cluster
([KKP](https://docs.kubermatic.com/kubermatic/main/tutorials-howtos/etcd-backups/)),
no point-in-time recovery
([Gardener](https://gardener.cloud/docs/gardener/concepts/backup-restore/)).
Gardener's continuous full and delta snapshots with automatic restore are the
most complete.

**One load balancer per control plane is the cost driver.** Gardener
measured it (one LB per shoot, quotas, slow provisioning) and moved to SNI
multiplexing on one LB per seed
([GEP-08](https://github.com/gardener/enhancements/blob/main/geps/0008-shoot-apiserver-via-sni/README.md));
KKP offers an SNI tunnelling strategy
([expose strategies](https://docs.kubermatic.com/kubermatic/main/tutorials-howtos/networking/expose-strategies/)).
Once many control planes share one address, pods inside a tenant can no
longer reach `kubernetes.default` through it without SNI, and each project
ships a per-node proxy or rewrites the Service's endpoints.

**Listeners on nodes bind wider than they need to.** Talos Omni moved its
metrics to loopback and stopped embedded etcd listening by default in 1.13
([releases](https://github.com/siderolabs/omni/releases)); we found
konnectivity agents and kube-proxy health ports on every interface of nodes
we run ourselves. A node of a hosted control plane needs no inbound port at
all.

**Upgrade order is often left to the user.** Gardener, HyperShift and KKP
enforce version skew in their API or UI
([worker pool versions](https://github.com/gardener/gardener/blob/master/docs/usage/shoot-operations/worker_pool_k8s_versions.md),
[HyperShift updating](https://docs.okd.io/latest/hosted_control_planes/hcp-updating.html),
[KKP upgrades](https://docs.kubermatic.com/kubermatic/main/tutorials-howtos/operation/control-plane/upgrading-the-control-plane/));
in smaller projects the order of control plane, add-ons and nodes is a
manual plan ([k0smotron autopilot](https://docs.k0smotron.io/stable/update/hcp-autopilot/)).

**An outage between the control plane and its nodes evicts healthy
workloads.** Remote nodes stop renewing their leases when the path to their
control plane breaks, and the controller manager cannot tell that from dead
machines: it taints them and evicts their pods after the toleration.
Kubernetes stops evicting when every node goes silent at once, but a taint
added before can stay
([kubernetes#131481](https://github.com/kubernetes/kubernetes/issues/131481)).
Gardener guards its clusters with a dependency-watchdog that stops the
controller manager while node leases expire and the API server answers
([configuration](https://github.com/gardener/dependency-watchdog/blob/master/docs/deployment/configure.md)).

**A new operator release can roll the whole fleet at once.** Re-rendering
every control plane the moment an operator upgrades turns one changed
default into a fleet-wide rollout. Gardener bounds it with maintenance
windows and per-seed limits
([shoot maintenance](https://gardener.cloud/docs/gardener/shoot/shoot_maintenance/)).

**Hibernation is uneven.** Built into Gardener with schedules
([hibernation](https://gardener.cloud/docs/gardener/shoot/shoot_hibernate/)),
available as scaling to zero elsewhere, unavailable with vCluster private
nodes ([private nodes](https://www.vcluster.com/docs/vcluster/configure/vcluster-yaml/private-nodes)),
absent from the HyperShift, k0smotron and KKP documentation.

**Fresh control planes can roll twice.** When two steps of a reconcile
render the same Deployment (first without an add-on, then with it), a new
control plane gets two rollouts before it is ever ready, and every later
toggle of that add-on does the same.

## Where the bar is

| capability | who sets it |
|---|---|
| minimal footprint and datastore flexibility | Kamaji: one Deployment per tenant, etcd, SQL or NATS stores, and a read-only `WriteLimited` mode no one else has ([write permissions](https://kamaji.clastix.io/guides/write-permissions/)) |
| kubeadm-compatible nodes on a hosted control plane | Kamaji: any machine joins with `kubeadm join` ([worker nodes](https://kamaji.clastix.io/concepts/tenant-worker-nodes/)) |
| backup and restore | Gardener: continuous full + delta, automatic restore, control plane migration between seeds |
| credential rotation | Gardener: staged CA and ServiceAccount key rotation with status, automatic etcd key rotation, one-hour admin kubeconfigs |
| PKI design | HyperShift: a documented CA hierarchy, reconciliation-driven renewal, dual-key ServiceAccount rotation |
| upgrades | Gardener: maintenance windows, version classes, forced updates of expired versions |
| high availability | Gardener's failure tolerance types with replica and PDB conventions; HyperShift's zone spread and priority classes |
| networking | Gardener: one LB per seed, SNI on 443, apiserver-proxy for in-cluster access, no inbound node ports |
| tenancy invariants | HyperShift: unidirectional traffic, NetworkPolicy per namespace, no mutable CRDs in the guest |
| footprint transparency | HyperShift publishes per-control-plane sizing; k0smotron publishes a 100-control-plane benchmark |
| project governance | Cluster API: SIG governance and a published version support policy |

## What Lodia does about it

| we found | Lodia | decision |
|---|---|---|
| a secret change restarts everything | a Deployment per component; reloadable files are not hashed into pod templates, restart-only files roll only their consumer | [component-per-deployment](decisions/component-per-deployment.md) |
| CA rotation is manual and touches nodes | staged rotation of CA, ServiceAccount key and encryption key, with the old and new valid together | [staged-credential-rotation](decisions/staged-credential-rotation.md) |
| shared datastores make isolation a code property | etcd per tenant by default through etcd-druid; a shared store only by choice | [dedicated-etcd-by-default](decisions/dedicated-etcd-by-default.md) |
| choices are frozen at creation | migration between stores by snapshot, behind a write lock in the tenant | [dedicated-etcd-by-default](decisions/dedicated-etcd-by-default.md) |
| webhooks couple availability | no admission webhooks; CEL in the schema; conditions for the rest | [no-admission-webhooks](decisions/no-admission-webhooks.md) |
| backups cover less than expected | continuous snapshots per tenant, restore into a new `ControlPlane`, the encryption key kept with them | [dedicated-etcd-by-default](decisions/dedicated-etcd-by-default.md) |
| one LB per control plane | a shared Gateway by SNI; in-cluster access through apiserver-proxy | [api-exposure](decisions/api-exposure.md) |
| node listeners bind wide | nothing inbound on nodes; agents bind loopback, the kubelet its node IP | [api-exposure](decisions/api-exposure.md) |
| upgrade order left to the user | the skew-policy order enforced step by step | [upgrades-and-maintenance-windows](decisions/upgrades-and-maintenance-windows.md) |
| operator releases roll the fleet | Lodia-initiated changes roll in each tenant's window, N at a time | [upgrades-and-maintenance-windows](decisions/upgrades-and-maintenance-windows.md) |
| hibernation is uneven | hibernation with schedules, etcd included | [one-controlplane-per-cluster](decisions/one-controlplane-per-cluster.md) |
| an outage between control plane and nodes evicts healthy pods | Health holds the controller manager's leader lease while most nodes are silent | [controller-manager-hold](decisions/controller-manager-hold.md) |
| separate pods per component cost density | an optional `Compact` layout: one pod per tenant with a shared store | [compact-layout](decisions/compact-layout.md) |
| control planes roll twice at creation | every component rendered in one pass by one controller | [independent-controllers](decisions/independent-controllers.md) |
| a sequential reconcile makes failures global | six controllers with their own queues, coupled only by conditions | [independent-controllers](decisions/independent-controllers.md) |
| standing admin credentials in the super-user group | short-lived kubeconfigs; the standing admin group is revocable | [short-lived-kubeconfigs](decisions/short-lived-kubeconfigs.md) |

## What we take from each

- **Kamaji:** control planes as plain workloads on a management cluster,
  kubeadm-joined nodes, Gateway API exposure, a write lock in the tenant.
- **Gardener:** etcd-druid and its backups, staged credential rotation,
  maintenance windows, the dependency watchdog, the apiserver-proxy and SNI
  model, ManagedResource-style apply and prune, hibernation, short-lived admin
  kubeconfigs.
- **HyperShift:** a Deployment per component, its tenancy invariants, a
  documented PKI and sizing.
- **KKP:** declarative backup schedules and restores as objects.
- **Cluster API:** a published support policy; the provider contract Lodia's
  own Cluster API provider will implement.
- **Talos Omni:** nodes that need egress only.

Every fact above links its source; the decisions link the same sources where
they rest on them.
