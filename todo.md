# todo

Capture inbox.
Findings that fall outside the current task land here and are triaged into the Notion **Tasks** database at the end of a task.
Nothing here is a decision — a decision is a file in `docs/decisions/`.

## From the design (2026-10-23)

- Check whether kube-apiserver reloads `--proxy-client-cert-file` and whether konnectivity-server reloads `--cluster-cert`; each proven reload removes a rollout from certificate renewal.
- Propose upstream a task of etcd-druid that deletes a prefix in the store (dedicated-etcd-by-default): `retention.afterDelete`, a deleted `ControlPlane`'s last snapshots and a deleted or failed `Backup`'s copy under `…/backups/<Backup UID>` wait for it, and platforms use their bucket's lifecycle rules meanwhile, which must spare the copy of a `Backup` that exists (`docs/operations.md`).
- etcd-backup-restore restores without etcd's `RevisionBump` and `MarkCompacted`, so a member it restores keeps the snapshot's revisions; propose the bump upstream (dedicated-etcd-by-default; the etcd spike's finding 5 measured the stale cache).
- etcd-druid's scale at ~1000 `Etcd` objects with its default `concurrentSyncs: 3` is not documented; measure in phase 4 or ask the maintainers.

## From the skeleton (2026-10-24)

- kube-api-linter on `api/`, as `releases-and-support` lists: the header of `.golangci.yml` says the file is identical in every Segla repository, so the linter needs a run of its own, not a line in that file.
- Nothing bumps what is pinned by digest or commit: the workflow's actions and the base image in `.ko.yaml`; `.github/` has no Renovate or Dependabot configuration.
  Add one for `github-actions`, `gomod` and the base images, leaving the API module's `k8s.io` floor alone (releases-and-support).

## From lodia-signer (2026-10-23)

- The signer's socket is mode 0600 and owned by the signer's user (65532 on distroless `nonroot`); kube-apiserver can dial it only as that user or as root.
  When the Workloads controller runs the API server as non-root, both containers need the same user.
- The signer logs a failed load at error level when it reads a Secret volume while the kubelet removes the previous version; the next event reloads the complete set.
  If that noise matters, retry once before logging.

## From the etcd spike (2026-10-24)

- Upstream for etcd-druid: the etcd container's readiness probe takes its scheme from `backup.tls` while the wrapper serves HTTPS whenever client TLS is on; and a failing pre-scale snapshot shows only on its `EtcdOpsTask`, not on the `Etcd` (findings 1 and 4).
  Those `presync-snapshot-*` tasks also make etcd-druid reject a `Backup`'s snapshot task while they run, which the backup reconciler sends again.
  In turn, a `Backup`'s task that has not completed makes etcd-druid reject its pre-sync task ([reconcile_task.go](https://github.com/gardener/etcd-druid/blob/v0.39.0/internal/controller/etcdopstask/reconcile_task.go#L110-L115)), and after three rejected or failed attempts etcd-druid goes ahead without that snapshot, with only a warning event ([statefulset.go](https://github.com/gardener/etcd-druid/blob/v0.39.0/internal/component/statefulset/statefulset.go#L216-L226)); a slow `Backup` flush, or one sent again while the full snapshot Lease stays short of the seal, can use them up.
  etcd-druid takes that snapshot before every hibernation, every change of the replicas, Lodia's switch between 1 and 3 members included, and every change of the etcd-wrapper, backup-restore or init container's image ([statefulset.go](https://github.com/gardener/etcd-druid/blob/v0.39.0/internal/component/statefulset/statefulset.go#L130-L183)).
- Upstream for etcd-backup-restore: the sidecar keeps serving its first serving certificate after the Secret changes (finding 3).

## From deciding the design (2026-10-25)

- A `loadBalancer` Service that reports only a host name (AWS NLB and CLB) is `LoadBalancerHostnameUnsupported`; `--advertise-address` takes an IP, so the `kubernetes` endpoint needs another source there.
  Decide whether such a provider is supported, and how.
- client-go reloads a kubeconfig's CA file only with `ClientsAllowCARotation`, on by default since 1.36 ([known_features.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/client-go/features/known_features.go#L115-L117)): on 1.35 a trust anchor rotation restarts every component whose kubeconfig names the CA file; plan it into the phase 3 rotation.
- Phase 2: Argo CD has no generic health check for custom resources ([health checks](https://github.com/argoproj/argo-cd/blob/v3.5.6/docs/operator-manual/health.md#L61-L83)), and kstatus, through which Flux waits, keeps a permanently failing object InProgress until a timeout unless it reports `Stalled` ([kstatus](https://github.com/kubernetes-sigs/cli-utils/blob/v0.37.2/pkg/kstatus/status/status.go#L102-L183)); Lodia writes no kstatus `Reconciling`, so the docs ship an Argo CD Lua health check and a Flux `healthCheckExprs` entry ([kustomizations.md](https://github.com/fluxcd/kustomize-controller/blob/v1.10.1/docs/spec/v1/kustomizations.md#L351-L374)) for `ControlPlane`, both reading `status.observedGeneration` and `Ready`.

## From the Datastore controller (2026-10-25)

- The backup-restore sidecar's `spec.backup.resources` have no field in the `DatastoreClass` and stay etcd-druid's default (50m / 128Mi at v0.39.0); the operator configuration's `defaults.resources` should cover them once phase 1 measures.
- The shared `Placement` and `resources` types carry only size limits (`api/v1alpha1/controlplane_types.go`), not the rules of pod validation: the class's `placement` and `etcd.resources` have none yet.
  Until then a class value the apps API refuses in etcd-druid's StatefulSet shows as `status.datastore.members.updated` absent, since etcd-druid returns before it stamps `observedGeneration` ([reconciler.go](https://github.com/gardener/etcd-druid/blob/v0.39.0/internal/controller/etcd/reconciler.go#L127-L144)).
  The running members stay, later certificate generations stop reaching them until the class is fixed, and the reason is in the `Etcd`'s `status.lastErrors`.
- No schema can check what pod admission refuses in a pod the StatefulSet API accepted: a `priorityClassName` that names no PriorityClass, or one deleted later, and a LimitRange or ResourceQuota in the tenant namespace.
  The rolling update deletes the old pod first and then cannot create the new one ([stateful_set_control.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/pkg/controller/statefulset/stateful_set_control.go#L748-L760)), so a single member goes down at its next roll, and every certificate generation is a roll.
  `docs/operations.md` names the symptom.
- Backup credentials scoped to one tenant's prefix, so a tenant namespace's `<cp>-etcd-backup` reaches only that tenant's snapshots and not every tenant's on the class (`docs/README.md`, Identity and tenancy).
  A `Backup`'s copy, `<prefix>/<namespace UID>/backups/<Backup UID>`, sits under the same namespace UID as its source's snapshots, so one scope per namespace covers both.
- `make e2e` could check for Docker and `helm` before it creates its cluster.
- The cache keeps metadata only for Secrets because this controller watches them as `PartialObjectMetadata`; nothing stops a future controller from starting a typed Secret informer.
  A test or a cache transform should enforce the rule.

## From the PKI controller (2026-10-25)

- A changed `apiServer.encryption.provider` puts the new provider first with a new key and keeps the previous ones readable, but nothing rewrites the stored Secrets or drops the old provider, and with two API server replicas one may write with the new key before the other has reloaded the file (up to a minute).
  That is the etcd encryption key rotation of [staged-credential-rotation](docs/decisions/staged-credential-rotation.md); until it is built, document the change as unsupported, or refuse it in the schema.
  A `Backup` is unaffected: it takes the source's configuration after the copy, and that holds every provider PKI ever wrote.
  A change back to `None` puts a second `identity` behind the first (`encryptionConfig` in `internal/controller/pki/secrets.go`); harmless, and the rotation should drop it with the other old providers.
- The kubelet signer narrows nothing: whoever holds it can sign any identity the API server accepts.
  A signer outside the controller manager, serving only kubelet CSRs (those for `kubernetes.io/kube-apiserver-client-kubelet`, [types.go](https://github.com/kubernetes/api/blob/v0.37.1/certificates/v1/types.go#L155-L158)), would make the narrowing true; decide before the client CA rotation is built.

## From the PKI review (2026-10-25)

- `readEncryptionConfig` decodes leniently, while kube-apiserver decodes strictly and with defaults ([config.go](https://github.com/kubernetes/kubernetes/blob/v1.37.0/staging/src/k8s.io/apiserver/pkg/server/options/encryptionconfig/config.go#L140-L146), [loadConfig](https://github.com/kubernetes/kubernetes/blob/v1.37.0/staging/src/k8s.io/apiserver/pkg/server/options/encryptionconfig/config.go#L519-L536)).
- The previous etcd generation is kept and adopted on completeness alone, even when it does not chain to the current etcd CA (a restored predecessor's generation, for instance); keep it only if it chains.

## From the contracts audit (2026-10-25)

- Phase 3: `target.rotation.*`, which `Target` lacks, gets a stage enum of its own instead of sharing PKI's observed `RotationPhase`, and `rotation.ca` becomes `clusterCA`.
- Phase 3: `Hibernated`, with Lifecycle as its writer, and the handled `lodia.io/operation` value recorded in `status`.

## From the Tenant controller (2026-10-25)

- Nothing watches `<cp>-ca-bundle`: a rotated cluster CA reaches `cluster-info` within the apply period; the CA rotation of phase 3 should bring a Tenant pass.
- A tenant's own admission webhook that fails with a 5xx counts as no answer, so a True `AddonsApplied` stays while it keeps an object out.
- The one-minute requeue while the rollout is pending runs the full apply, not only the rollout read.
- Every pass builds a new tenant client, transport and RESTMapper.
- `--agent-identifiers=ipv4=$(HOST_IP)` names an agent by its node's IPv4 address only; IPv6 and dual-stack nodes need `ipv6=`.
- `AddonsAvailable` is not read again while applies are refused: the rollout read before stays.
- CoreDNS's replicas are Lodia's: an autoscaler is set back on every apply; a later phase lets one own them.

## From the Workloads controller (2026-10-25)

- JWT authentication, an audit policy, an admission and a scheduler configuration and `serviceAccount.publicDiscovery` are not rendered: they turn `APIServerReady` False with `Unsupported` until they are built.
- The NetworkPolicies isolate each component's ingress alone; the default-deny set and egress, the README's arrows, come later.
- The control plane pods set no securityContext: give them one that passes Pod Security `restricted`, the API server and the signer under one user (From lodia-signer, above).
- Workloads lists the management nodes uncached on every pass of a `nodePort` `ControlPlane`; an informer on Nodes, which carry no control-plane label and so need a cache rule of their own, would serve them from memory.
- In `nodePort` mode the Service takes a node port for 443 as well, which nothing dials: the endpoint names the node ports of 6443 and 8132, and in-namespace clients use the cluster IP.
- Workloads finds another owner's objects under its names in the cache, which holds only labelled ones.
  An unlabelled object it applied for a predecessor is taken over silently, with the `OwnerReferencesPermissionEnforcement` plugin as without it, now that Workloads may delete (before, the plugin refused the apply); one that only refers to another `ControlPlane` is applied over once and then held; one with another controller is refused by validation, which `APIServerReady` reports as `Refused`.
  Reading the targets uncached, as PKI reads its Secrets, would see them all.

## From the etcd NetworkPolicy (2026-10-26)

- The etcd's policy admits no metrics scraper, as the controller manager's and the scheduler's do not: a ServiceMonitor for the etcd and sidecar metrics needs its scraper admitted, the phase 2 arrow from the metrics scrapers to the health ports.

## From the chart (2026-10-26)

- etcd-druid's CRDs come from its chart's `crds/` directory, which Helm applies only when it installs a release (`docs/README.md`, Installation): a bump of the dependency needs its CRDs applied by hand first; decide whether the chart templates them or the release notes carry the step.
- The operator's pods set no requests or limits until the operator is measured.

## From #29's review (2026-10-26)

- Measure a highly available control plane's idle footprint; phase 2's scope and gate do not list it.

## From the backup prefix (2026-10-26)

- `docs/api/examples/datastore.yaml` puts a tenant of a shared etcd under `/lodia/<namespace>/<name>/`, while dedicated-etcd-by-default grants each tenant `/<prefix>/<uid>/`; the example should say the decision's prefix when the shared `Datastore` is built.
- The `v1/` prefix rule: the schema refuses a `DatastoreClass` backup prefix that contains `v1/` or ends in `v1` (the `@todo` in `api/v1alpha1/datastore_types.go`), since etcd-backup-restore v0.45.1 takes the last `v1/` anywhere in a key for its layout ([snapshot.go](https://github.com/gardener/etcd-backup-restore/blob/v0.45.1/pkg/snapstore/snapshot.go#L39-L59)).
  Remove the rule, its cases in `test/api/validation_test.go` and its mentions in the docs once the sidecar of the etcd-druid release Lodia pins reads a key relative to its store prefix.

## From the lossless rollouts (2026-10-27)

- Deleting a `ControlPlane` waits out the API server's 90 s grace period, since its etcd goes at the same moment and the requests in flight wait for it; the Datastore controller should delete the `Etcd` only after Workloads' API server pods are gone, which also brings the per-pull-request e2e job back under its budget (its overrun is accepted for now).
- `--endpoint-reconciler-type=none` skips kube-apiserver's check that the advertise address has the service range's family ([validation.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/cmd/kube-apiserver/app/options/validation.go#L111-L127)); Workloads should pick `inCluster` in the family of `serviceCIDRs[0]`, or report `WaitingForEndpoint` naming the family.
- Requests through the tunnel (logs, exec, webhooks, aggregated APIs) can fail for 1–3 s after a new API server pod turns ready: the agents reach its konnectivity-server through the Service, which sends them to ready pods only, so they connect only once the pod already takes requests, and a webhook with `failurePolicy: Fail` turns that into refused writes.
  The agents should connect before the pod turns ready, through an agent port that reaches pods not yet ready, and the pod's readiness should wait for them; konnectivity-server's `/readyz` waits for one agent ([readiness_manager.go](https://github.com/kubernetes-sigs/apiserver-network-proxy/blob/v0.37.0/pkg/server/readiness_manager.go#L28-L33)), which a tenant without nodes never has.
  The gate's loop should send such a request too.

## From the phase 2 contracts (2026-10-27)

- The `restore` verb (restore-replaces-the-source, not built): a ValidatingAdmissionPolicy in the chart could require `authorizer…check('restore')` on the `Backup` a new `ControlPlane`'s `restoreFrom` names, for platforms where the right to create `ControlPlane`s is wider than the right to read a namespace's Secrets.
- An IPv6 advertise address Lodia draws: once gateway mode has users with an IPv6 first service range, Workloads could draw a unique local address per `ControlPlane` and keep it beside its CAs, which a platform backs up anyway.
  `apiServer.advertiseAddress` then becomes optional, which breaks no manifest (api-exposure).

## From the docs review (2026-10-27)

- A runbook in `docs/operations.md` for moving a tenant from `Compact` to `Split`: a new `ControlPlane` restored from a `Backup` (dedicated), or a migration (shared) (compact-layout).
- konnectivity-server's Role in the tenant (`internal/controller/tenant/manifests/konnectivity.yaml` L13-L21) can update and delete every Lease in `kube-system`, the controller manager's and the scheduler's leader Leases included.
  The server needs its own Lease, `konnectivity-proxy-server-<server ID>` with a random ID at every start, and lists and deletes the expired ones labelled `k8s-app=konnectivity-server` ([server.go](https://github.com/kubernetes-sigs/apiserver-network-proxy/blob/v0.35.0/cmd/server/app/server.go#L160-L171), [options.go](https://github.com/kubernetes-sigs/apiserver-network-proxy/blob/v0.35.0/cmd/server/app/options/options.go#L413-L428), [gc_controller.go](https://github.com/kubernetes-sigs/apiserver-network-proxy/blob/v0.35.0/pkg/server/leases/gc_controller.go#L56-L92)), so `resourceNames` cannot narrow the Role, since they restrict no `create` ([RBAC](https://kubernetes.io/docs/reference/access-authn-authz/rbac/#referring-to-resources)).
  A lease namespace of their own can: the server and the agents take `--lease-namespace` at v0.35.0, v0.36.0 and v0.37.0, `kube-system` by default ([server options.go](https://github.com/kubernetes-sigs/apiserver-network-proxy/blob/v0.35.0/cmd/server/app/options/options.go#L164), [agent options.go](https://github.com/kubernetes-sigs/apiserver-network-proxy/blob/v0.35.0/cmd/agent/app/options/options.go#L142)), and the server's Role and the agents' (L38-L46) would move there.
- Place each tenant's daily full snapshot inside its maintenance window once phase 2's wave 2 brings the windows, as Gardener does for its shoots ([backup and restore](https://github.com/gardener/gardener/blob/master/docs/concepts/backup-restore.md)); today Lodia passes the class's `backup.fullSnapshotSchedule` to etcd-druid unchanged, `0 0 * * *` by default (dedicated-etcd-by-default).

## From high availability (2026-10-27)

- The e2e suites reach a tenant through `status.endpoint`, a kind node's address on the docker network `kind`, which Docker Desktop does not route to a macOS host, so `make e2e` and `make e2e-ha` run on Linux alone, as on GitHub's runners.
  Running `go test` in a container attached to that network would bring them back to macOS.
- Lodia sets etcd-druid's annotation `resources.druid.gardener.cloud/allow-unhealthy-pod-eviction` on a highly available `Etcd`, which etcd-druid marks as one that may be removed at any time and is not meant for general use ([poddisruptionbudget.go](https://github.com/gardener/etcd-druid/blob/v0.39.0/internal/component/poddistruptionbudget/poddisruptionbudget.go#L35-L38)).
  Check it with every etcd-druid upgrade, and ask upstream for a field of the `Etcd` in its place.
- Ask etcd-druid whether a scale-in can leave the remaining member running: the members' pod template carries a checksum of the ConfigMap whose `initial-cluster` names every member, so removing `highAvailability` restarts the last member: about 20 seconds without `Ready`, in which the controller manager and the scheduler lose their leases.
- kube-apiserver closes its connection to the external signer on SIGTERM ([kubernetes#142980](https://github.com/kubernetes/kubernetes/issues/142980)), so the API server's shutdown delay is a `preStop` sleep of 15 seconds with `--shutdown-delay-duration=0` (the `@todo` in `internal/controller/workloads/render.go`, api-exposure).
  Go back to `--shutdown-delay-duration=15s` without the sleep once a fix, such as kubernetes#143346, is in every Kubernetes patch Lodia supports.
- Removing `highAvailability` waits for etcd-druid to list `<cp>-etcd-0` as ready, but etcd-druid lists a member as ready until its lease is a minute old ([check_ready.go](https://github.com/gardener/etcd-druid/blob/v0.39.0/internal/health/etcdmember/check_ready.go#L88-L106), [values.yaml](https://github.com/gardener/etcd-druid/blob/v0.39.0/charts/values.yaml#L66)).
  A removal within that minute of member 0 failing reaches etcd-druid, which holds the scale-in until member 0 returns, so `DatastoreReady` reads `1 of 1 members ready`, and Lodia holds adding `highAvailability` back until that scale-in completes.
  Reading the pod's readiness would narrow the window, not close it, at the cost of RBAC on pods: etcd's readiness probe takes up to 25 seconds to fail ([builder.go](https://github.com/gardener/etcd-druid/blob/v0.39.0/internal/component/statefulset/builder.go#L603-L610)).

## From availability surviving a spec change (2026-10-28)

- A writer that refuses a new generation, `Unsupported`, `Refused` or `ObjectsControlledElsewhere`, turns its condition, and so `Ready`, False while the tenant still serves the previous one, though a readiness condition reports availability ([independent-controllers](docs/decisions/independent-controllers.md)).
  It needs its own answer to where a refusal shows while availability stays.
- The required `e2e-ha` failed twice in G2's step that deletes the etcd leader's pod, one request answered 500 each time: run 40764385375 on #43, one on a reused connection in each loop, outside and inside the tenant, and run 42450529023 on #56, one of 480 inside the tenant on new connections, sent 2.4 ms after the deletion; the step wants none, and both passed on a rerun.
  The answer is the tenant's API server passing on an error of its etcd client, `rpc error: code = Internal desc = server closed the stream without sending trailers`: 1 of 193,299 reads over 30 deletions at 50 requests a second, sent 5 ms after its deletion, and 0 of 39,740 over 30 at 10.
  On a TLS listener etcd serves gRPC under its HTTP server ([serve.go](https://github.com/etcd-io/etcd/blob/v3.5.27/server/embed/serve.go#L243)), where a call gRPC no longer serves ends without a status ([server.go](https://github.com/grpc/grpc-go/blob/v1.83.2/server.go#L1130-L1132), [handler_server.go](https://github.com/grpc/grpc-go/blob/v1.83.2/internal/transport/handler_server.go#L219-L226)); the client calls that `Internal` ([http2_client.go](https://github.com/grpc/grpc-go/blob/v1.82.1/internal/transport/http2_client.go#L1270)) and retries a read only on `Unavailable` ([retry.go](https://github.com/etcd-io/etcd/blob/v3.7.0/client/v3/retry.go#L54-L75)), and the API server answers 500 to an error that is no `metav1.Status` ([status.go](https://github.com/kubernetes/apiserver/blob/v0.37.0/pkg/endpoints/handlers/responsewriters/status.go#L60-L81)).
  Which of those two paths ends the call, and why a call sent milliseconds after the deletion meets it, is not pinned.
  `listen-client-http-urls` would give gRPC a listener of its own ([serve.go](https://github.com/etcd-io/etcd/blob/v3.5.27/server/embed/serve.go#L128)), but etcd-druid renders the configuration and does not offer it ([etcdconfig.go](https://github.com/gardener/etcd-druid/blob/v0.39.0/internal/component/configmap/etcdconfig.go#L55)).
  Decide whether G2 keeps holding zero failed requests across this step until upstream fixes it.
- Health compares each condition it applies with the cache unless it has just read it live, and its predicates ignore its own conditions, so a write the server holds and the cache does not yet, such as a `Reachable` False, stays until its next pass, a `health.period` (5 s by default) later.

## From KubeconfigRequest (2026-10-30)

- A kubectl exec credential plugin that generates the key, creates the request and caches the certificate, as gardenlogin caches Gardener's ([get_client_certificate.go](https://github.com/gardener/gardenlogin/blob/v0.11.0/cmd/get_client_certificate.go#L187-L188)); until then a person runs openssl and kubectl.
- Requests on behalf of another user, for a platform's backend: the policy could admit them with `impersonate` on `lodia.io` `users`; additive.
- A viewer level for `spec.access`: it needs a read-only role kept current with the tenant's resources; additive to the enum.

## From the backups (2026-11-01)

- Until etcd-backup-restore sorts snapshot sets by revision, a compaction Job of etcd-druid that lists the store before a `Backup`'s full snapshot lands and saves its own after it makes the copy take the compacted set, which ends below the seal, while the `Backup` reads `Completed`; the restore's seal check refuses such a `Backup` (`RestoreFailed`).
  The copier sorts the sets by their full snapshot's `CreatedOn` ([miscellaneous.go](https://github.com/gardener/etcd-backup-restore/blob/v0.45.1/pkg/miscellaneous/miscellaneous.go#L175-L177), the same on `master` on 2026-11-02), while the restore and the compactor take the full snapshot with the highest revision ([miscellaneous.go](https://github.com/gardener/etcd-backup-restore/blob/v0.45.1/pkg/miscellaneous/miscellaneous.go#L129-L150)).
  Propose upstream that it sort by the full snapshot's last revision, then by `CreatedOn`.
  When the fix is released: bump the etcd-backup-restore image the copy Job runs, which comes from etcd-druid's image vector ([images.yaml](https://github.com/gardener/etcd-druid/blob/v0.39.0/internal/images/images.yaml#L20-L25)); remove the `@todo` at the copy task's creation in `internal/controller/datastore/backup.go`; and add a test that would have caught it if one is feasible, such as an e2e step that saves a full snapshot of a lower revision after the `Backup`'s and checks that the copy holds the seal.

- A `Backup` started after the store moved through `<cp>-etcd-backup`, but before the sidecar's files hold the new keys, puts its full snapshot into the old store, while its copy Job reads the new keys when it starts and finds nothing there, which succeeds; the kubelet's update delay bounds the window ([secrets](https://github.com/kubernetes/website/blob/snapshot-initial-v1.37/content/en/docs/concepts/configuration/secret.md#L549-L553)).
  `docs/operations.md` has a person wait for that upload after the two moves no `Backup` sees; a move the `Backup` records is not covered.
- The backup reconciler dials the source's etcd in every pass that seals, and the Datastore controller dials a restored etcd in every pass of its `ControlPlane` once a member is ready: a client per `Etcd`, kept across passes and closed when the `Etcd` names a new certificate generation or goes, would save a TLS handshake per pass once fleets grow.

## From the restore's hold (2026-11-02)

- The `Compact` layout, once built, renders a held restore's pod without the controller-manager container on the hold the `Split` layout renders as 0 replicas (`release` and `ran` in `internal/controller/workloads/workloads.go`); `Compact` is `Unsupported` until then (restore-replaces-the-source, compact-layout).
- Health lists seven kinds in a held restore's tenant every period, a page of 500 each (`actedOn` in `internal/controller/health/health.go`); if inert tenants grow large, list them less often than the probe runs.

## From the restore (2026-11-02)

- PKI copies a restore's keys from the `Backup` of `restoreFrom.backupRef`'s name, not from the `Backup` UID `status.restore` names: a `ControlPlane` restored from a `Backup` that went without its keys, which takes a person removing `lodia.io/restore-source` by hand, would take the keys of a successor `Backup` of the same name and hold that one while its keys exist (`backupKeys` in `internal/controller/pki/secrets.go`, `restoring` in `internal/controller/datastore/backup.go`; round 3, R3-P2).

## From the gateway gate (2026-11-08)

- Move the gateway gate to Envoy Gateway v1.10.x at its first patch release: v1.10.0 is the first whose compatibility matrix covers Kubernetes v1.37 ([matrix.md](https://github.com/envoyproxy/gateway/blob/v1.10.0/site/content/en/news/releases/matrix.md#L13-L14)), and v1.9.3 is the version every experiment behind the design ran on.
- Workloads counts a `TLSRoute`'s `ResolvedRefs` False with reason `EndpointsNotFound` as resolved (the `@todo` in `internal/controller/workloads/gateway.go`): Envoy Gateway v1.9.3 sets it while the EndpointSlices of the route's Service list no endpoint for its port ([route.go](https://github.com/envoyproxy/gateway/blob/v1.9.3/internal/gatewayapi/route.go#L3119-L3125)), so while no API server pod has an address, and `Ready` would read `RouteNotAccepted` instead of `APIServerReady`'s reason; for an `HTTPRoute` it keeps `ResolvedRefs` True there, citing the Gateway API's conformance ([route.go](https://github.com/envoyproxy/gateway/blob/v1.9.3/internal/gatewayapi/route.go#L323-L330)).
  Once every Envoy Gateway release still supported upstream keeps a `TLSRoute`'s `ResolvedRefs` True there, and the gateway gate pins one: remove `noEndpoints`, the clause in `RoutesReady`'s comment in `api/v1alpha1/conditions.go` and in its row of `docs/api/controlplane.md`, and the tests that write `EndpointsNotFound` (`TestRoutesWithoutReadyEndpointsAreResolvedWhileTheAPIServerIsUnavailable` in `internal/controller/workloads`, `TestReadyInGatewayModeSaysWhyTheAPIServerIsDownWhileItsRoutesHaveNoEndpoints` in `internal/controller`).
  The upstream fix is [envoyproxy/gateway#10564](https://github.com/envoyproxy/gateway/pull/10564), open on 2026-11-08.

## From Health's node sampling (2026-11-08)

- `health-nodes` lists the Nodes every 30 seconds and the Leases every period, each in one response with no `limit`, so a response grows with the tenant's nodes.
  If tenants of thousands of nodes appear, page both lists.

## From the CI of the refactoring (2026-11-09)

- `make test` gives envtest's API servers 60 s to stop instead of the default 20 s (the `@todo` in `Makefile`): a tenant API server in `internal/controller/tenant` twice took longer, after its test's checks had passed, and nobody has measured why.
  768 stops in loaded Linux containers took at most 1.12 s, but it did reproduce outside CI: on macOS, in both of two runs of `go test -count=1 ./internal/controller/tenant/` on 2026-11-10, as `timeout waiting for process kube-apiserver to stop` in a test's cleanup after its checks passed, without a dump, since `LODIA_APISERVER_DUMPS` was not set.
  `verify` now keeps the log and the goroutines of a tenant API server still running 20 s into its stop, in the artifact `apiserver-dumps` (`testenv.StartDumping`, the `@todo` in `internal/controller/tenant/tenant_test.go`).
  Read the first dump, fix what holds it past SIGTERM, then remove `KUBEBUILDER_CONTROLPLANE_STOP_TIMEOUT` from `make test`, `StartDumping` and the artifact's step.

## From Workloads' hold (2026-11-09)

- A renewed restart-only certificate rolls its component at once, or at the grant on a tenant Workloads holds, not in the tenant's window ([upgrades-and-maintenance-windows](docs/decisions/upgrades-and-maintenance-windows.md), "Who waits for the window"): holding it for the window is for phase 3's rotations, and takes a field each way between Workloads and Lifecycle, a digest of the restart hashes and a clock of the certificate the pods still run.

## From the Cluster API provider (2026-11-10)

- Phase 3: a dedicated identity for Cluster API.
  `<cluster>-kubeconfig`, which the provider copies from `<cp>-admin-kubeconfig`, carries Lodia's admin credential (`kubernetes-admin` in `lodia:cluster-admins`), which is also the platform's automation credential, while Cluster API's contract says that kubeconfig must not be shared with other users or applications ([control-plane.md](https://github.com/kubernetes-sigs/cluster-api/blob/v1.14.3/docs/book/src/developer/providers/contracts/control-plane.md#L872-L874)).
  A second standing kubeconfig from PKI for it, so that revoking one does not revoke the other.

## From class edits in the window (2026-11-11)

- An edit that removes `etcd.autoCompaction` from a class that set it is refused at the grant: the apply of the `Etcd` sends an empty `spec.sharedConfig` where Lodia's apply set its fields before, it comes back null, and etcd-druid's schema refuses it (`spec.sharedConfig in body must be of type object: "null"`), so `DatastoreReady` is `Refused` and certificate generations stop reaching the `Etcd`.
  `apply` drops empty scheduling constraints for that reason, and an empty `sharedConfig` needs the same.
- A person's edit of the `Etcd`'s `spec.backup.store` to a value the schema of `status.datastore.snapshots` refuses fails every status apply while `keep` or `copied` carries the store: while the class waits for its grant or is held for its credentials, while the operator revision is held, and while the recorded class is gone, until a class of its name is created again and granted; `DatastoreReady`, `BackupReady` and `status.datastore` stop being written meanwhile.
  etcd-druid's schema takes any provider or none, `Local` among them, no container, and a prefix and an endpoint of any length ([store.go](https://github.com/gardener/etcd-druid/blob/v0.39.0/api/core/v1alpha1/store.go#L13-L35)); the status takes S3, GCS, ABS and Swift, a bucket of at least one character, a prefix of 1 to 512 characters and an endpoint of at most 512.

## For wave 2, observability

- Health's own metrics, under metrics-are-part-of-the-api: the lag of Health's pass is the watchdog of the hold's margin, and the rest, failed probes and silent nodes, is to be named there.
  The hold itself is `lodia_controlplane_condition{type="ControllerManagerHeld"}`.
- `LodiaMaintenanceHalted`, as many tenants failed in their slots as `fleet.maxConcurrentRollouts` ([operations](docs/operations.md#alerts-shipped-in-the-chart)).
  This alert and the next read the slot from `status.maintenance.slotSince`, not from `Progressing`'s reason, since a slot is also held under `Unsupported`, which ranks above `MaintenanceFailed`, and `TimeZoneUnsupported`, which ranks above `Maintaining`.
- An alert for a slot held long: a tenant whose change neither finishes nor fails, a tenant API that stops answering mid-rollout or a Tenant waiting after the grant on a cause the grant did not remove, stays `Maintaining` with its slot; from `Progressing`'s `lastTransitionTime` ([upgrades-and-maintenance-windows](docs/decisions/upgrades-and-maintenance-windows.md), "The fleet").

## For the release that first drops a minor

- A minor dropped from the version table stays renderable for one more release, as the source of an upgrade ([upgrades-and-maintenance-windows](docs/decisions/upgrades-and-maintenance-windows.md), "End of support"): build it in the release that first drops one, since no minor is dropped yet and the flag would have no data and no test that means anything.
