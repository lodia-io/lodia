# todo

Capture inbox.
Findings that fall outside the current task land here and are triaged into the Notion **Tasks** database at the end of a task.
Nothing here is a decision — a decision is a file in `docs/decisions/`.

## From the design (2026-10-23)

- List the Gateway implementations whose conformance reports pass `TLSRoute` in passthrough (Envoy Gateway, Istio, Cilium, kgateway, NGINX Gateway Fabric, Traefik), for the install docs.
- Check whether kube-apiserver reloads `--proxy-client-cert-file` and whether konnectivity-server reloads `--cluster-cert`; each proven reload removes a rollout from certificate renewal.
- Propose upstream a task of etcd-druid that deletes a prefix in the store (dedicated-etcd-by-default): `retention.afterDelete`, a deleted `ControlPlane`'s last snapshots and a deleted `Backup`'s copy wait for it, and platforms use their bucket's lifecycle rules meanwhile.
- etcd-backup-restore restores without etcd's `RevisionBump` and `MarkCompacted`, so a member it restores keeps the snapshot's revisions; propose the bump upstream (dedicated-etcd-by-default; the etcd spike's finding 5 measured the stale cache).
- etcd-druid's scale at ~1000 `Etcd` objects with its default `concurrentSyncs: 3` is not documented; measure in phase 4 or ask the maintainers.

## From the skeleton (2026-10-24)

- kube-api-linter on `api/`, as `releases-and-support` lists; it is not in `.golangci.yml` yet.
- The workflow's actions are pinned by commit with nothing to bump them; Dependabot or Renovate for `github-actions` and `gomod`, leaving the API module's `k8s.io` floor alone (releases-and-support).

## From lodia-signer (2026-10-23)

- The signer's socket is mode 0600 and owned by the signer's user (65532 on distroless `nonroot`); kube-apiserver can dial it only as that user or as root.
  When the Workloads controller runs the API server as non-root, both containers need the same user.
- `.ko.yaml` pins the base image by digest with nothing to bump it; add it to the Renovate configuration with the other base images.
- The signer logs a failed load at error level when it reads a Secret volume while the kubelet removes the previous version; the next event reloads the complete set.
  If that noise matters, retry once before logging.

## From the etcd spike (2026-10-24)

- Upstream for etcd-druid: the etcd container's readiness probe takes its scheme from `backup.tls` while the wrapper serves HTTPS whenever client TLS is on; and a failing pre-scale snapshot shows only on its `EtcdOpsTask`, not on the `Etcd` (findings 1 and 4).
- Upstream for etcd-backup-restore: the sidecar keeps serving its first serving certificate after the Secret changes (finding 3).

## From deciding the design (2026-10-25)

- `KubeconfigRequest`'s schema refuses only `system:masters`; [short-lived-kubeconfigs](docs/decisions/short-lived-kubeconfigs.md) now refuses every user and group under `system:` and every user under `lodia:`.
  The rule and its envtest cases come with the controller that answers requests.
- `loadBalancer` mode with a load balancer that reports only a host name (AWS NLB and CLB): `--advertise-address` takes an IP, so the `kubernetes` endpoint needs another source there; decide before that mode is supported on such a provider.
- client-go reloads a kubeconfig's CA file only with `ClientsAllowCARotation`, on by default since 1.36 ([known_features.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/client-go/features/known_features.go#L115-L117)): on 1.35 a trust anchor rotation restarts every component whose kubeconfig names the CA file; plan it into the phase 3 rotation.
- Phase 2: Argo CD has no generic health check for custom resources ([health checks](https://github.com/argoproj/argo-cd/blob/v3.5.6/docs/operator-manual/health.md#L61-L83)), and kstatus, through which Flux waits, keeps a permanently failing object InProgress until a timeout unless it reports `Stalled` ([kstatus](https://github.com/kubernetes-sigs/cli-utils/blob/v0.37.2/pkg/kstatus/status/status.go#L102-L183)); Lodia writes no kstatus `Reconciling`, so the docs ship an Argo CD Lua health check and a Flux `healthCheckExprs` entry ([kustomizations.md](https://github.com/fluxcd/kustomize-controller/blob/v1.10.1/docs/spec/v1/kustomizations.md#L351-L374)) for `ControlPlane`, both reading `status.observedGeneration` and `Ready`.

## From the Datastore controller (2026-10-25)

- When restore lands in phase 2's wave 1b, test the `VolumesExist` remedy end to end: the claims kept, a `Backup` of the old cluster, a new `ControlPlane` with `restoreFrom` that serves the old data.
- The backup-restore sidecar's `spec.backup.resources` have no field in the `DatastoreClass` and stay etcd-druid's default (50m / 128Mi at v0.39.0); the operator configuration's `defaults.resources` should cover them once phase 1 measures.
- The shared `Placement` and `resources` types get the rules of pod validation with Workloads, which renders them for the `ControlPlane` in phase 1: the class's `placement` and `etcd.resources` have none yet.
  Until then a class value the apps API refuses in etcd-druid's StatefulSet shows as `status.datastore.members.updated` absent, since etcd-druid returns before it stamps `observedGeneration` ([reconciler.go](https://github.com/gardener/etcd-druid/blob/v0.39.0/internal/controller/etcd/reconciler.go#L127-L144)).
  The running members stay, later certificate generations stop reaching them until the class is fixed, and the reason is in the `Etcd`'s `status.lastErrors`.
- No schema can check what pod admission refuses in a pod the StatefulSet API accepted: a `priorityClassName` that names no PriorityClass, or one deleted later, and a LimitRange or ResourceQuota in the tenant namespace.
  The rolling update deletes the old pod first and then cannot create the new one ([stateful_set_control.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/pkg/controller/statefulset/stateful_set_control.go#L748-L760)), so a single member goes down at its next roll, and every certificate generation is a roll.
  `docs/operations.md` names the symptom.
- A class edit that reaches the `Etcd` rolls its members at once; the decision wants it in the tenant's maintenance window, which Lifecycle gates in phase 2.
  The `Etcd` records the class generation it was rendered from for that.
- `<cp>-etcd-backup` stays when a class drops its backup store; harmless, the `ControlPlane` garbage-collects it.
- Whether etcd-backup-restore reads rotated store credentials without a restart: etcd-druid mounts `<cp>-etcd-backup` and puts no checksum of it in the pod template ([builder.go](https://github.com/gardener/etcd-druid/blob/v0.39.0/internal/component/statefulset/builder.go#L219-L226)), so a new copy rolls nothing; the rotation in `docs/operations.md` revokes the old keys once every copy holds the new ones, which is safe only if the sidecar re-reads them.
- Backup credentials scoped to one tenant's prefix, so a tenant namespace's `<cp>-etcd-backup` reaches only that tenant's snapshots and not every tenant's on the class (`docs/README.md`, Identity and tenancy).
- `make e2e` could check for Docker and `helm` before it creates its cluster.
- The cache keeps metadata only for Secrets because this controller watches them as `PartialObjectMetadata`; nothing stops a future controller from starting a typed Secret informer.
  A test or a cache transform should enforce the rule.

## From the PKI controller (2026-10-25)

- A changed `apiServer.encryption.provider` puts the new provider first with a new key and keeps the previous ones readable, but nothing rewrites the stored Secrets or drops the old provider, and with two API server replicas one may write with the new key before the other has reloaded the file (up to a minute).
  That is the etcd encryption key rotation of [staged-credential-rotation](docs/decisions/staged-credential-rotation.md); until it is built, document the change as unsupported, or refuse it in the schema.
- `apiServer.encryption.provider: kms` is accepted by the schema; PKI issues everything else and reports `CertificatesReady` False with `Unsupported`.
  Refuse it in CEL until the provider is built.
- The kubelet signer narrows nothing: whoever holds it can sign any identity the API server accepts.
  A signer outside the controller manager, serving only kubelet CSRs (those for `kubernetes.io/kube-apiserver-client-kubelet`, [types.go](https://github.com/kubernetes/api/blob/v0.37.1/certificates/v1/types.go#L155-L158)), would make the narrowing true; decide before the client CA rotation is built.
- Lodia reaches a tenant's API server through its in-namespace Service and ignores `HTTPS_PROXY` for that connection; the chart's documentation should say so for sites that set a proxy on every pod.

## From the PKI review (2026-10-25)

- No PKI-level test pins one clock per pass: `leaf()` issuing at `time.Now()` passes the whole suite; a test that delays one apply past a second boundary would pin it.
- `TestAPredecessorsSecretsAreNeitherReadNorWritten` passes with the metadata lookup disabled, since the labelled predecessor Secrets mask its unlabelled CA key; give that path its own case.
- In a blocked pass the CAs are never loaded: a forbidden `lodia:operator` then reports "client CA missing", and the bind error replaces the bounded requeue with exponential backoff.
  With `APIServerReady` True and `<cp>-lodia-kubeconfig` missing or foreign, `AccessReady` says "APIServerReady is not True for this generation", which the state contradicts.
- `readEncryptionConfig` decodes leniently, while kube-apiserver decodes strictly and with defaults ([config.go](https://github.com/kubernetes/kubernetes/blob/v1.37.0/staging/src/k8s.io/apiserver/pkg/server/options/encryptionconfig/config.go#L140-L146), [loadConfig](https://github.com/kubernetes/kubernetes/blob/v1.37.0/staging/src/k8s.io/apiserver/pkg/server/options/encryptionconfig/config.go#L519-L536)); with `kms` requested, an unreadable configuration is reported as `Unsupported`, not `SecretUnreadable`.
- The previous etcd generation is kept and adopted on completeness alone, even when it does not chain to the current etcd CA (a restored predecessor's generation, for instance); keep it only if it chains.

## From the contracts audit (2026-10-25)

- Phase 2: `KubeconfigRequest` gets a named status writer.
- Phase 3: `target.rotation.*`, which `Target` lacks, gets a stage enum of its own instead of sharing PKI's observed `RotationPhase`, and `rotation.ca` becomes `clusterCA`.
- Phase 3: `Hibernated`, with Lifecycle as its writer, and the handled `lodia.io/operation` value recorded in `status`.

## From the Tenant controller (2026-10-25)

- Nothing watches `<cp>-ca-bundle`: a rotated cluster CA reaches `cluster-info` within the apply period; the CA rotation of phase 3 should bring a Tenant pass.
- `addons.konnectivity.version` pins the agents' image here; the server's is Workloads'.
- A tenant's own admission webhook that fails with a 5xx counts as no answer, so a True `AddonsApplied` stays while it keeps an object out.
- The one-minute requeue while the rollout is pending runs the full apply, not only the rollout read.
- Every pass builds a new tenant client, transport and RESTMapper.
- `--agent-identifiers=ipv4=$(HOST_IP)` names an agent by its node's IPv4 address only; IPv6 and dual-stack nodes need `ipv6=`.
- `AddonsAvailable` is not read again while applies are refused: the rollout read before stays.
- CoreDNS's replicas are Lodia's: an autoscaler is set back on every apply; a later phase lets one own them.

## From the Workloads controller (2026-10-25)

- JWT authentication, an audit policy, an admission and a scheduler configuration, `serviceAccount.publicDiscovery` and `apiServer.advertiseAddress` outside gateway mode are not rendered: they turn `APIServerReady` False with `Unsupported` until phase 2 builds them.
- The NetworkPolicies isolate each component's ingress alone; the default-deny set and egress, the README's arrows, come in phase 2.
- The control plane pods set no securityContext: give them one that passes Pod Security `restricted`, the API server and the signer under one user (From lodia-signer, above).
- Workloads lists the management nodes uncached on every pass of a `nodePort` `ControlPlane`; an informer on Nodes, which carry no control-plane label and so need a cache rule of their own, would serve them from memory.
- In `nodePort` mode the Service takes a node port for 443 as well, which nothing dials: the endpoint names the node ports of 6443 and 8132, and in-namespace clients use the cluster IP.
- Workloads finds another owner's objects under its names in the cache, which holds only labelled ones.
  An unlabelled object it applied for a predecessor is taken over silently, with the `OwnerReferencesPermissionEnforcement` plugin as without it, now that Workloads may delete (before, the plugin refused the apply); one that only refers to another `ControlPlane` is applied over once and then held; one with another controller is refused by validation, which `APIServerReady` reports as `Refused`.
  Reading the targets uncached, as PKI reads its Secrets, would see them all.

## From the etcd NetworkPolicy (2026-10-26)

- The etcd's policy admits no metrics scraper, as the controller manager's and the scheduler's do not: a ServiceMonitor for the etcd and sidecar metrics needs its scraper admitted, the phase 2 arrow from the metrics scrapers to the health ports.
- The Datastore controller reads its previous `DatastoreReady` from the cache: a pass that starts before the cache holds the previous pass's write applies a transition a second time, moving `lastTransitionTime` by the gap, a second in envtest, where a test that compared the ControlPlane's resourceVersion right after a transition to True caught it once in fifteen runs.

## From the chart (2026-10-26)

- etcd-druid's CRDs come from its chart's `crds/` directory, which Helm applies only when it installs a release (`docs/README.md`, Installation): a bump of the dependency needs its CRDs applied by hand first; decide whether the chart templates them or the release notes carry the step.
- The operator's pods set no requests or limits until the operator is measured.

## From #29's review (2026-10-26)

- The idle table leaves out a control plane pod that is not Running with every container ready (`pods` in `test/e2e/conformance_test.go`, through `running` in `datastore_test.go`): the sum can come out short, and `pods[0]` panics when none is left.
  Leave out only terminating pods and fail on the rest.
- A `CONFORMANCE_FOCUS` that selects no spec passes; hydrophone's `--extra-ginkgo-args --fail-on-empty=true` would fail it ([suite.go](https://github.com/onsi/ginkgo/blob/v2.32.0/internal/suite.go#L527-L530)).
- `--conformance-image` repeats what hydrophone v0.8.0 derives from the tenant's server version ([root.go](https://github.com/kubernetes-sigs/hydrophone/blob/v0.8.0/cmd/root.go#L205-L219)); drop it.
- The local serial run's one failure needs no chasing: the pod generation spec's 20 s wait for `observedGeneration` ([pods.go](https://github.com/kubernetes/kubernetes/blob/v1.37.0/test/e2e/node/pods.go#L531)) ran out while that network pulled `nginx:1.27.0-2` in 28.8 s, and the spec passed with the images pulled beforehand and in CI.
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
- With no `status.endpoint.inCluster` yet, Tenant should report `WaitingForEndpoint` instead of returning the error of parsing an empty address.

## From the phase 2 contracts (2026-10-27)

- The `restore` verb (restore-replaces-the-source, not built): a ValidatingAdmissionPolicy in the chart could require `authorizer…check('restore')` on the `Backup` a new `ControlPlane`'s `restoreFrom` names, for platforms where the right to create `ControlPlane`s is wider than the right to read a namespace's Secrets.
- A `Backup` that comes back without its status, from a restore of the management cluster that gives new UIDs, would be sealed again from a source that may be empty.
  The restore's validation proposed the annotation `lodia.io/sealed-uid`, so that such a `Backup` is never sealed again; the decision recovers a management cluster only from a backup that keeps UIDs and did not take it.
  Settle it with the backups of wave 1b.
- `docs/operations.md` gets the replace checklist and a Velero recipe for a clone beside a live source, with the restore in wave 1b (restore-replaces-the-source).
- Health's own metrics, which the availability research suggested and the decisions did not name: failed probes, silent nodes, and the lag of Health's pass, the watchdog of the hold's margin.
  Name them under metrics-are-part-of-the-api with wave 2, Health, or drop them; the hold itself is `lodia_controlplane_condition{type="ControllerManagerHeld"}`.
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
  Until then a spec change lifts a refusal of `APIServerReady` for as long as Workloads waits for its inputs, since it counts its Deployments meanwhile.
