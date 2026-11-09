# End-to-end tests

The suite runs only against a kind cluster whose kubeconfig is named on the command line, in `E2E_KUBECONFIG` ([decision](../../docs/decisions/e2e-on-a-named-cluster.md)).

`make e2e` creates the kind cluster `lodia-e2e-local` (`E2E_CLUSTER` changes the name), runs the suite against it and deletes it.

To keep a cluster between runs:

```sh
mkdir -p work/e2e
go tool kind create cluster --name lodia-e2e-dev --kubeconfig work/e2e/dev.kubeconfig
E2E_KUBECONFIG=work/e2e/dev.kubeconfig make e2e
E2E_KUBECONFIG=$PWD/work/e2e/dev.kubeconfig go test -tags e2e -count=1 ./test/e2e/... -run TestExamplesOnAKindCluster
go tool kind delete cluster --name lodia-e2e-dev --kubeconfig work/e2e/dev.kubeconfig
```

`--kubeconfig` makes kind write that file alone and leave `~/.kube/config` untouched.
`go test` runs in the package directory, so it needs an absolute `E2E_KUBECONFIG`; `make e2e` resolves a relative one, and runs `go test -v` so that each scenario's lines are in the log.

Before the first test, the suite installs Lodia as a user does, and needs Docker and `helm` on the `PATH` for it: it builds `cmd/lodia`, `cmd/lodia-signer` and `cmd/lodia-tunnel` with ko straight into the kind cluster (`KO_DOCKER_REPO=kind.local`, as the hard way does), then runs `helm dependency build` and installs [`charts/lodia`](../../charts/lodia) into `lodia-system` with those three images.
The chart brings the CRDs and etcd-druid v0.39.0, its dependency; the operator runs in the cluster under the chart's RBAC, and every test asserts against it.
The suite leaves the chart installed.

- `TestDatastoreOnKind`: PKI issues the etcd certificate generations, and generation 2 is the one it issues under the same CA once a member Secret of generation 1 is deleted, the roll a tenant sees in production.
- `TestOperatorFromTheChart`: the operator's Deployment is Available, a running replica holds the leader Lease `lodia.io`, and a namespace that enforces Pod Security `restricted` admits the operator's pod in a dry run.
- `TestExamplesOnAKindCluster`: the CRDs the chart installed accept every example in [`docs/api/examples`](../../docs/api/examples).
- `TestAPIServerOnKind`: a `ControlPlane` in `nodePort` mode reports `APIServerReady=True`: a real kube-apiserver, controller manager and scheduler on PKI's certificates and the Datastore's etcd.
- `TestPhase1Gate`: the per-pull-request part of the [phase 1 gate](../../docs/roadmap.md#phase-1--walking-skeleton).
  A `ControlPlane` in `nodePort` mode, at the version of kind's default node image, turns `Ready`, and the suite logs the time from its creation, which must be under 3 minutes.
  Anonymous requests reach the health paths and `cluster-info` and nothing else: `/version`, which RBAC grants unauthenticated users ([policy.go](https://github.com/kubernetes/kubernetes/blob/v1.37.0/plugin/pkg/auth/authorizer/rbac/bootstrappolicy/policy.go#L383-L388), [binding](https://github.com/kubernetes/kubernetes/blob/v1.37.0/plugin/pkg/auth/authorizer/rbac/bootstrappolicy/policy.go#L723)), answers 401.
  A node joins (below) and turns `Ready`; a pod of agnhost, pinned by digest, runs on it, and its logs, an `exec` and a port-forward reach it through the konnectivity tunnel: before the three and after each, the suite reads konnectivity-server's dial count, the count of its `konnectivity_network_proxy_server_dial_duration_seconds` histogram, observed once per established dial ([metrics.go](https://github.com/kubernetes-sigs/apiserver-network-proxy/blob/v0.37.0/pkg/server/metrics/metrics.go#L76-L85), [server.go](https://github.com/kubernetes-sigs/apiserver-network-proxy/blob/v0.37.0/pkg/server/server.go#L1067)), on its health port, 8134 ([server.go](https://github.com/kubernetes-sigs/apiserver-network-proxy/blob/v0.37.0/cmd/server/app/server.go#L585)), through a port-forward to the API server pod, and wants each call to add one dial.
  `exec` runs twice, over client-go's SPDY transport and over WebSocket, kubectl's default; the port-forward uses SPDY, and kubectl's WebSocket port-forward reaches the kubelet through the same transport and tunnel ([exec](https://github.com/kubernetes/kubernetes/blob/v1.37.0/pkg/registry/core/pod/rest/subresources.go#L208-L230), [port-forward](https://github.com/kubernetes/kubernetes/blob/v1.37.0/pkg/registry/core/pod/rest/subresources.go#L293-L306)), so the dial count covers it too.
  An API server rollout then loses no request, from outside the tenant or from inside it: two loops of [`test/apiloop`](../apiloop) run through a rollout that a new `apiServer.extraArgs` entry starts, one in a pod in the management cluster that dials `status.endpoint.apiServer`, the other in a pod on the joined node that dials `https://kubernetes.default`.
  The suite builds the loop with ko as it builds the operator, and copies its image from a node of the kind cluster into the joined node's containerd with `ctr`.
  Each loop reads the tenant's `kubernetes` EndpointSlice ten times a second on new connections and ten times a second on reused ones, with plain HTTP clients that give up after 5 seconds.
  They follow an answer's `Retry-After` once, as client-go does ([with_retry.go](https://github.com/kubernetes/client-go/blob/v0.37.1/rest/with_retry.go#L308-L318)), which a draining API server sends ([api-exposure](../../docs/decisions/api-exposure.md)).
  They retry nothing else, since client-go would also retry a GET whose connection broke, which hides a lost request ([request.go](https://github.com/kubernetes/client-go/blob/v0.37.1/rest/request.go#L1066-L1078)).
  Each loop prints each failure and, every second, its counts.
  Between the counts before the change and those once the old pod is gone and 5 seconds more have passed, the suite wants no request failed, no answer with an empty slice, and answers to at least 95% of the requests the rate sends, and it logs how many followed a `Retry-After`.
  Disabling `addons.coreDNS` removes CoreDNS's objects from the tenant and from the inventory, and enabling it brings them back.
  Deleting the `ControlPlane` leaves in its namespace, of every namespaced kind the API server serves, Events aside, which outlive their objects, only the etcd's claim, which the class's `Retain` keeps, and what every namespace holds: the ConfigMap `kube-root-ca.crt` and the ServiceAccount `default`.

The suite reaches the tenant's API server through `status.endpoint`, as `<cp>-admin-kubeconfig`, whose server it is, and, as the platform, creates a bootstrap token Secret there ([nodes-join-with-kubeadm](../../docs/decisions/nodes-join-with-kubeadm.md)).
That address is a kind node's on the docker network `kind`, so the suite runs where that network is routable, as on GitHub's Linux runners.
It port-forwards to a pod only for what no Service serves: konnectivity-server's health port and the tenant's pod `web`.
Each connection to a forwarded address gets a port-forward of its own: client-go ends a port-forward, with every connection through it, on the first forwarding error ([portforward.go](https://github.com/kubernetes/client-go/blob/v0.37.1/tools/portforward/portforward.go#L545-L550)), and an exec's connection meets one when the API server has closed it before the client's last write (`write: broken pipe`).

A node joins as in [the hard way](../../docs/hard-way.md#11-join-a-node).
The suite starts a container of kind's node image, `<cluster>-tenant-node-1`, on the docker network `kind`, waits for its entrypoint to report the boot as kind does ([cgroups.go](https://github.com/kubernetes-sigs/kind/blob/v0.33.0/pkg/cluster/internal/providers/common/cgroups.go#L33-L47)), and runs `kubeadm join <status.endpoint.apiServer> --token … --discovery-token-ca-cert-hash sha256:…` in it.
`--patches` names [`testdata/kubeadm-patches`](testdata/kubeadm-patches), whose `kubeletconfiguration` patch, the node's own configuration over the cluster's ([patches](https://github.com/kubernetes/website/blob/snapshot-initial-v1.37/content/en/docs/setup/production-environment/tools/kubeadm/control-plane-flags.md?plain=1#L185-L205), [--patches](https://github.com/kubernetes/kubernetes/blob/v1.37.0/cmd/kubeadm/app/cmd/options/generic.go#L104-L114)), carries the kubelet settings kind gives its nodes ([config.go](https://github.com/kubernetes-sigs/kind/blob/v0.33.0/pkg/cluster/internal/kubeadm/config.go#L596-L615)), and the `ControlPlane` sets `addons.kubeProxy.conntrack.maxPerCore: 0`.
`--ignore-preflight-errors` names the one check a container fails, `SystemVerification`: on GitHub's runners it finds the host kernel's configuration in none of the places it looks, and `modprobe configs` fails ([kernel_validator.go](https://github.com/kubernetes/system-validators/blob/v1.12.1/validators/kernel_validator.go#L191-L245), the version kubeadm v1.37.0 uses).
Ignoring it skips kubeadm's OS and cgroups validators too, which run under the same name; a check's name is the finest grain kubeadm ignores by ([checks.go](https://github.com/kubernetes/kubernetes/blob/v1.37.0/cmd/kubeadm/app/preflight/checks.go#L573-L588), [checks_linux.go](https://github.com/kubernetes/kubernetes/blob/v1.37.0/cmd/kubeadm/app/preflight/checks_linux.go#L48-L54)).
The CNI is kindnet from the node image's manifest, with the tenant's pod range.
The suite removes the containers it started when it ends, pass or fail, and each one before it starts it; `make e2e`, `make e2e-ha`, `make e2e-restore`, `make e2e-gateway`, `make e2e-upgrade`, `make e2e-hold` and `make conformance` remove them as well, since a `go test` timeout or a panic never reaches the suite's end.

`make e2e` gives the suite 30 minutes.

## Gate G1: a minor upgrade

`TestMinorUpgradeGate` checks the minor upgrade of [gate G1](../../docs/roadmap.md#phase-2--production) under the two loops of the phase 1 gate.
It sits behind the build tag `upgrade` with `e2e`, and `make e2e-upgrade` runs it on a kind cluster of its own as `make e2e-restore` runs G3; the `e2e-upgrade` job of [`ci.yml`](../../.github/workflows/ci.yml) runs that on every pull request.
It reuses G3's versitygw store ([Gate G3](#gate-g3-a-backup-and-a-restore)) for the class `e2e-upgrade`, and needs nothing else of G3.

A `ControlPlane` at v1.36.4 turns `Ready`, a container of the kind node image `v1.36.4`, by digest, joins it, and a pod runs on it.
kube-proxy's and CoreDNS's images of v1.37.1 are pulled into the node, and both loops and two watches, of the `ControlPlane` and of the namespace's Deployments, start before `spec.version` moves to v1.37.1:

1. The `Backup` of 1.37 turns `Completed` with `status.kubernetesVersion` v1.36.4, no owner reference, before the target moves.
2. While the API server reports v1.37.1 and the addons still the old minor, `logs` and `exec` reach the pod, the agents having reconnected to the new konnectivity-server ([metrics.go](https://github.com/kubernetes-sigs/apiserver-network-proxy/blob/v0.37.0/pkg/server/metrics/metrics.go#L126-L133)).
3. The upgrade ends: `status.kubernetes.version` v1.37.1, no `upgrade`, `Progressing` False, `Idle`, `Ready` True, the tenant's kube-proxy at v1.37.1, the node kept at v1.36.4 and reported as outdated.

The test wants the two watches to have seen the three Deployments and never the controller manager or the scheduler newer than the API server, in `status.components` or, in their images, than what `status.components.apiServer.version` reports, nor `status.addons` newer than the oldest component; and between the counts before the edit and counts each loop reports later than the end, no request failed, none found the `kubernetes` slice empty, and answers to at least 95% of the requests sent.
The test does not ask `Ready` to stay `True` through the upgrade, and logs each change of it instead.

## Nightly: the chain and a highly available upgrade

[`nightly-upgrade.yml`](../../.github/workflows/nightly-upgrade.yml) runs two tests under the same tag every night, on demand, and on a pull request that changes the workflow or `test/e2e/upgrade_nightly_test.go`.
Each is a job of its own, with its own kind cluster.

`TestMinorUpgradeChain` (`make e2e-upgrade E2E_UPGRADE_TEST=TestMinorUpgradeChain`) takes a tenant from v1.35.8 to v1.37.1, the table's `Latest` of 1.36 between them: a `Backup` of each minor completes before the target moves, two kind nodes of v1.35.8 join at the first minor, one of v1.36.4 after the first step, and one of v1.37.0 at the end, and all four stay.

`TestHighAvailabilityUpgrade` (`make e2e-upgrade-ha`, on the cluster shape of G2) is G1's edit on a tenant with `failureTolerance: Zone`, under the two loops.

## Gate G2: a highly available control plane

`TestHighAvailabilityGate` checks the second item of the [phase 2 gate](../../docs/roadmap.md#phase-2--production).
It sits behind a build tag of its own, `ha` with `e2e`, since it needs a cluster of another shape: [`testdata/kind-ha.yaml`](testdata/kind-ha.yaml), a control plane node and three workers in the zones `a`, `b` and `c`.
`make e2e-ha` runs it on such a cluster of its own as `make conformance` runs `TestConformance`, and the `e2e-ha` job of [`ci.yml`](../../.github/workflows/ci.yml) runs that on every pull request.

The tenant is the phase 1 gate's with `highAvailability.failureTolerance: Zone`, and must turn `Ready` within 5 minutes.
The test then wants each component's two pods and the three etcd members each in a zone of its own, a PodDisruptionBudget with `maxUnavailable: 1` per component and etcd-druid's with `minAvailable: 2`, all four with `unhealthyPodEvictionPolicy: AlwaysAllow`.
A node joins, the two loops of the phase 1 gate start, and a watch of the `ControlPlane` records each change of `Ready` and its inputs at the time it arrives.
Two steps follow, the second once the first has recovered:

1. One API server pod is deleted; the step ends 30 seconds after the pod is gone and two replicas are ready.
2. The pod of the etcd leader, the member whose role `Etcd.status.members` gives as `Leader`, is deleted; the step ends 30 seconds after its replacement runs and etcd-druid reports three members ready, one of them the leader.

In each step `Ready` must not leave `True` for longer than 5 seconds, Health's period in the gate, and neither loop may lose a request.
The test logs each recorded change and, for each condition, the longest time it was not `True`.

## Gate G3: a Backup and a restore

`TestRestoreGate` checks the backup and restore item of the [phase 2 gate](../../docs/roadmap.md#phase-2--production).
It sits behind a build tag of its own, `restore` with `e2e`, and `make e2e-restore` runs it on a kind cluster of its own as `make e2e-ha` runs G2; the `e2e-restore` job of [`ci.yml`](../../.github/workflows/ci.yml) runs that on every pull request.

Before any class exists, the test runs versitygw v1.10.0, by digest, in the namespace `lodia-e2e-s3` with its posix backend on an emptyDir and random root keys, waits for its Deployment to be `Available`, and creates the bucket `lodia-e2e` with an `amazon/aws-cli` Job, by digest, that must be `Complete`.
The credentials go into `lodia-system` as `accessKeyID`, `secretAccessKey`, `region` and `s3ForcePathStyle`, the keys etcd-backup-restore reads for S3 ([s3_snapstore.go](https://github.com/gardener/etcd-backup-restore/blob/v0.45.1/pkg/snapstore/s3_snapstore.go#L106-L244)), and the class `e2e-restore` stores at `http://versitygw.lodia-e2e-s3.svc:7070` under the prefix `e2e`, with a delta snapshot every 10 seconds.

1. The `ControlPlane` `a`, in `nodePort` mode, turns `Ready`, and `BackupReady` True, `BackupSucceeded`.
2. In `a`, the test writes two namespaces with three Secrets of 32 random bytes and a ConfigMap each, whose data hashes it logs, a CRD and one object, a Role and its binding, and what binds a cluster to its world: a bootstrap token, a Node `source-node` with its Lease and a CSR, a LoadBalancer Service, a PersistentVolume with the `Delete` policy and a CronJob.
   It keeps `a`'s cluster and client CAs and a token of the ServiceAccount `restore-a/default`.
3. The `Backup` `a-1` turns `Ready`, `Completed`: its `status.source` is `a`'s UID, its prefix `e2e/<namespace UID>/backups/<Backup UID>`, its seal at a revision; it carries its UID in `lodia.io/sealed-uid` and the finalizer `lodia.io/restore-source`, `a-1.encryption-config` is immutable and controlled by it, and its two tasks are gone.
   An `amazon/aws-cli` Job lists the bucket under that prefix, which must hold a full snapshot whose last revision, the third field of the object name `Full-<start>-<last>-<time>` ([snapstore.go](https://github.com/gardener/etcd-backup-restore/blob/v0.45.1/pkg/types/snapstore.go#L119-L121)), is at least the seal's.
4. The test writes the ConfigMap `restore-a/after-backup` in `a`.
5. The `ControlPlane` `b`, with `a`'s namespace, class, version and network and `restoreFrom.backupRef` `a-1`, `Held` by default, turns `DatastoreReady` True with `status.restore` naming `a-1` and `a`, and `APIServerReady` True with its controller manager held at 0 replicas; `AddonsApplied` turns True, `ControllersLeading` False, `AwaitingRelease`, naming the LoadBalancer Service, the PersistentVolume and the CronJob, and `Ready` is False.
6. `b` serves the Secrets and ConfigMaps with `a`'s hashes, and the Widget, the Role and its binding with `a`'s UIDs and contents.
   It lacks `after-backup`, `source-node`, its Lease, the CSR and the bootstrap token Secret, which `a` still holds, and its `extension-apiserver-authentication` holds its own client CA and none of `a`'s.
   `a`'s admin certificate gets 401 from `b`, and `b`'s from `a`; `a`'s bootstrap token gets 401 from `b`, not from `a`; `a-sa-keys` and `b-sa-keys` hold different signing keys, and a TokenReview authenticates `a`'s ServiceAccount token on `a`, not on `b`.
   A Secret written to `b` is read from its etcd through a port-forward to `b-etcd-0`, with the client certificate and CA the `Etcd` names: its value starts with the API server's secretbox prefix `k8s:enc:secretbox:v1:` ([config.go](https://github.com/kubernetes/kubernetes/blob/v1.37.0/staging/src/k8s.io/apiserver/pkg/server/options/encryptionconfig/config.go#L63)) and the name of the key with a colon ([config.go](https://github.com/kubernetes/kubernetes/blob/v1.37.0/staging/src/k8s.io/apiserver/pkg/server/options/encryptionconfig/config.go#L699)), the first key of `b-encryption-config`, which `a-1.encryption-config` lacks.
7. `b`, set to `release: Released`, reports `ControllersLeading` False, `SourceExists`, and `APIServerReady` held for the source alone at that generation, its controller manager still at 0 replicas.
8. Once `a` is deleted and gone, `b`'s controller manager asks for one replica, `ControllersLeading` and `Ready` turn True, and `restore-a/kube-root-ca.crt` in `b` holds `b`'s CA and none of `a`'s.
9. A node joins `b` with a token made in `b`, as in the phase 1 gate, CoreDNS turns available, and hydrophone runs the smoke subset (below) in `b`.
10. The class's `Retain` kept `a`'s claims; a new `ControlPlane` `a`, restoring `a-1` and `Released` at creation, reports `DatastoreReady` False, `VolumesExist`, naming them; the test deletes `a-1` there, before the new `a` can have an `Etcd`, then the claims, and the new `a` creates its `Etcd`.
11. `a-1` must not be gone before `b` and the new `a` both have `status.restore` naming it.
    The new `a` turns `DatastoreReady` and `APIServerReady` True and serves `restore-a/secret-0` with `a`'s hash; `a-1.encryption-config` and the tasks `restore-<ControlPlane UID>` of `b` and the new `a` are gone, and the bucket still holds the copy under `a-1`'s prefix.

The smoke subset is 16 conformance specs, each matched by one pattern `\Q<text>\E.*\[Conformance\]` that matches exactly one spec of [conformance.yaml](https://github.com/kubernetes/kubernetes/blob/v1.37.0/test/conformance/testdata/conformance.yaml) at v1.37.0.
The text `Projected secret should be consumable from pods in volume` begins four other specs, so its pattern quotes `[Conformance]` too, which the e2e framework appends to that text first, ahead of `[NodeConformance]` ([ginkgowrapper.go](https://github.com/kubernetes/kubernetes/blob/v1.37.0/test/e2e/framework/ginkgowrapper.go#L455-L480)).
The patterns and the conformance image, by digest, are v1.37.0's, so the test stops at its start when kind's node image is of another version.
Hydrophone runs as in the [nightly](#nightly-conformance-and-the-idle-control-plane), for at most 15 minutes, with:

- `--busybox-image`, by digest, the image of the pod's container that hands out the results ([deploy.go](https://github.com/kubernetes-sigs/hydrophone/blob/v0.8.0/pkg/conformance/deploy.go#L177-L180));
- `--focus`, the 16 patterns joined by `|`;
- `--extra-ginkgo-args=--fail-on-empty=true`, so a focus that selects nothing fails ([suite.go](https://github.com/onsi/ginkgo/blob/v2.32.0/internal/suite.go#L527-L530)).

It must exit 0, and `junit_01.xml` must hold 16 test cases `[It] <spec>` with the status `passed` ([junit_report.go](https://github.com/onsi/ginkgo/blob/v2.32.0/reporters/junit_report.go#L204-L234)), one for each pattern.
The specs pull their own images, by the tags the conformance image names ([manifest.go](https://github.com/kubernetes/kubernetes/blob/v1.37.0/test/utils/image/manifest.go#L211-L245)).

Every wait is bounded and waits for a condition; no duration is asserted.

## The gateway gate

`TestGatewayGate` checks the `gateway` mode of [api-exposure](../../docs/decisions/api-exposure.md) on every pull request: tenants behind a shared Gateway by SNI, a node that joins by hostname, pods that reach their API through `apiserver-proxy` and the tunnel, and one owner per hostname.
It sits behind a build tag of its own, `gateway` with `e2e`, since its setup runs before the chart's: the operator looks for `TLSRoute` and `Gateway` only when it starts.
`make e2e-gateway` runs it on a kind cluster of its own as `make e2e-restore` runs G3, and the `e2e-gateway` job of [`ci.yml`](../../.github/workflows/ci.yml) runs that on every pull request.

Before the chart, the suite:

- applies the Gateway API's standard CRDs from the module `go.mod` requires, `config/crd/standard/` under `go list -m -f '{{.Dir}}' sigs.k8s.io/gateway-api`, so they follow the import and `go.sum` verifies them;
- installs Envoy Gateway v1.9.3 as its docs do for Gateway API CRDs managed apart: its own CRDs rendered from `gateway-crds-helm` and applied server-side, then `gateway-helm` with `crds.enabled=false` ([install-helm.md](https://github.com/envoyproxy/gateway/blob/v1.9.3/site/content/en/v1.9/install/install-helm.md#L118-L136));
  both charts and both images come by digest through `mirror.gcr.io`, since Docker Hub limits the pulls of shared runners, and each image is pulled into the kind node, up to five times, before the chart starts;
- creates the GatewayClass `eg`;
- starts a CoreDNS container, `<cluster>-dns`, on the docker network `kind`, which answers every name under `lodia.test` with the kind node's address and forwards the rest, checks that it does, and forwards `lodia.test` to it from the management cluster's CoreDNS, which Health's probe uses.

The test, in order:

1. The `ControlPlane` `team-gw-a`, with the hostnames `api.`, `konnectivity.` and `tunnel.team-gw-a.lodia.test` on the listener `tls` of the Gateway `gateway/tenants`, which does not exist yet, reports `RoutesReady` False, `GatewayNotFound`, `APIServerReady` False, `WaitingForEndpoint`, and no `status.endpoint`.
2. The test creates the Gateway: a `TLS` listener in `Passthrough` mode on 443 for `*.lodia.test`, routes allowed from the three tenant namespaces by their `kubernetes.io/metadata.name` label, and `spec.addresses` the kind node's address, which Envoy Gateway puts in its Service's `externalIPs` ([resource_provider.go](https://github.com/envoyproxy/gateway/blob/v1.9.3/internal/infrastructure/kubernetes/proxy/resource_provider.go#L369-L379)), so that `<node>:443` reaches Envoy.
   Once it is `Programmed`, the `ControlPlane` turns `Ready` with `RoutesReady` True, `Accepted`, and three `TLSRoute`s in its namespace, and the operator's pods are those it had before the Gateway, none restarted.
3. The test reaches the tenant through the admin kubeconfig with the Gateway's address as its server and the API hostname as `tls-server-name`, since nothing on the runner resolves `lodia.test`; exec and port-forward ignore `rest.Config.Dial`, and every client honours `ServerName` ([spdy.go](https://github.com/kubernetes/client-go/blob/v0.37.1/transport/spdy/spdy.go#L39-L54), [roundtripper.go](https://github.com/kubernetes/client-go/blob/v0.37.1/transport/websocket/roundtripper.go#L113-L119)).
   A node joins as in the phase 1 gate, at `status.endpoint.apiServer`, a hostname, its container started with `--dns` naming the DNS container, which kind's entrypoint also passes on to pods with `dnsPolicy: Default` ([entrypoint](https://github.com/kubernetes-sigs/kind/blob/v0.33.0/images/base/files/usr/local/bin/entrypoint#L446-L475)).
   Before `kubeadm join` the test copies the image the tenant's `apiserver-proxy` DaemonSet names into the node's containerd, since ko builds it into the kind cluster and no registry serves it.
   The `kubernetes` EndpointSlice then holds `240.0.0.1:6443`, and the node's `lo` holds `240.0.0.1/32` labelled `lo:lodia`.
4. The phase 1 gate's pod `web` runs, and its logs, an `exec` over each transport and a port-forward each add one konnectivity dial.
   The two loops of the phase 1 gate start, one in the management cluster dialing `status.endpoint.apiServer` through the Gateway, the other on the node dialing `https://kubernetes.default`, and on the node `lodia-tunnel` holds the listening socket at `240.0.0.1:6443`.
5. G1: an API server rollout, as in the phase 1 gate, and neither loop may lose a request.
6. A rollout of `apiserver-proxy`, which an annotation on its pod template starts, runs a new pod on the node beside the old one, the address is on `lo` in every sample, one each half second, and once the old pod is gone both of the in-cluster loop's clients get answers again.
   The old proxy's exit cuts the connections it still holds, so the test logs the requests that failed meanwhile and asserts none.
7. A TLS session to `tunnel.team-gw-a.lodia.test` through the Gateway, whose server verifies against the cluster CA, ends at its first read with the sidecar's `certificate required` alert, since it presents no client certificate, while the in-cluster loop loses no request.
8. Health's other two paths: new connections to port 6444, then 8132, are refused on the management node with an `iptables` rule, so that the tunnel's path and then konnectivity's fail alone; `Reachable` turns False, `ExternalProbeFailed`, its message naming the path, and back to True when the rule goes.
9. One owner per hostname: `team-gw-b`, with `team-gw-a`'s three hostnames, reports `RoutesReady` False, `HostnameConflict`, naming `team-gw-a`'s `ControlPlane`, by its UID, as their owner, and holds no `TLSRoute`, and each hostname has one route in the cluster.
   `team-gw-c`, with hostnames of its own, then turns `Ready`, which Health's probe through the Gateway proves, while Envoy's `listener_manager.lds.update_rejected`, read through a port-forward from its admin interface on `127.0.0.1:19000` ([bootstrap.go](https://github.com/envoyproxy/gateway/blob/v1.9.3/internal/xds/bootstrap/bootstrap.go#L35-L38)), stays 0 and `update_success` rises; the test fails if either is missing.
   Deleting `team-gw-a` hands the hostnames to `team-gw-b`, whose `RoutesReady` turns True with its three routes, and which turns `Ready`.

## Nightly: G6 and conformance behind the Gateway

[`nightly-gateway.yml`](../../.github/workflows/nightly-gateway.yml) runs two tests in the same build tag and kind cluster setup as the gate, every night, on demand, and on a pull request that changes the workflow, `test/e2e/gateway_nightly_test.go` or `test/g6client`.
Each is a job of its own.

`TestGatewayOverhead` (`make e2e-gateway-g6`) is gate G6 ([roadmap](../../docs/roadmap.md#phase-2--production)): the `ControlPlane` `team-g6` behind the Gateway, one API server replica, one joined node, and a test-owned NodePort Service `dev-g6-direct` to the API server.
[`test/g6client`](../g6client) runs in a pod on the node: authenticated `GET /version` over HTTP/2 to the three paths, sequentially, interleaved with the starting path rotated, on one reused connection per path (M1, 1,000 warm-up and 20,000 requests each) and on a new connection per request (M2, 200 and 3,000).
The paths are the direct one, the management node's address and the NodePort; the Gateway, its address on 443 with the API hostname as TLS server name; and the tunnel, the `kubernetes` ClusterIP, through `apiserver-proxy`.
p99 is the nearest rank, and the difference from the direct path has a 95% bootstrap interval of the pairs, 1,000 resamples with a fixed seed.
It wants Δp99 under 5 ms for the Gateway and for the tunnel in both mixes, no error, and one dial per path in M1; the table, with the environment, goes to `g6-results.txt` in the artifacts.

`TestGatewayConformance` (`make conformance-gateway`) is the conformance suite of the [nightly](#nightly-conformance-and-the-idle-control-plane) on a tenant behind the Gateway, with the two nodes that nightly joins and hydrophone given the gate's kubeconfig (`server` the Gateway's address, `tls-server-name` the API hostname); `CONFORMANCE_FOCUS` and the artifacts are the ones of that nightly.

## Nightly: highAvailability removed and added back

`TestHighAvailabilityRemovedAndRestored`, in the same build tag and on the same cluster shape as G2, runs every night in [`nightly-ha.yml`](../../.github/workflows/nightly-ha.yml), on demand, and on a pull request that changes the workflow or the test; `make e2e-ha E2E_HA_TEST=TestHighAvailabilityRemovedAndRestored` runs it locally.
A tenant with `failureTolerance: Zone` turns `Ready` with three etcd members and two replicas of each component.
Removing `highAvailability` must then leave it `Ready` with one member, one replica of each component and no PodDisruptionBudget, and adding it back must bring the three members, the two replicas and the budgets back, each within 10 minutes.
A step ends once etcd-druid has completed its scale and the StatefulSet runs every member on its current revision, ready, since etcd-druid's count of ready members trails them ([A tenant namespace](../../docs/README.md#a-tenant-namespace)).
The recorder of G2 watches both steps and logs every change, with the events of the control plane's objects, every change of their pods and the last lines a restarted container logged.
Removing `highAvailability` restarts the remaining etcd member ([A tenant namespace](../../docs/README.md#a-tenant-namespace)), so that step only logs the restarts and wants `Ready` back `True`.
Adding it back must keep `Ready` and every condition it is built from `True` throughout; a restart of any control plane container fails it.

## Nightly: gate G4, nodes cut off from the Gateway

`TestControllerManagerHold` checks the hold item of the [phase 2 gate](../../docs/roadmap.md#phase-2--production) ([controller-manager-hold](../../docs/decisions/controller-manager-hold.md)).
It sits behind the build tag `hold` with `e2e`, on the cluster shape of G2.
[`nightly-hold.yml`](../../.github/workflows/nightly-hold.yml) runs it every night, on demand, and on a pull request that changes the workflow or the test; `make e2e-hold` runs it locally on a kind cluster of its own, without the other gates, since it creates the Gateway `gateway/tenants` as the gateway gate does.

The Gateway's address is the control plane node's, and Envoy Gateway's Service takes `externalTrafficPolicy: Local` by default ([shared_types.go](https://github.com/envoyproxy/gateway/blob/v1.9.3/api/v1alpha1/shared_types.go#L376-L381)), so a node that runs no Envoy pod forwards nothing ([virtual IPs](https://kubernetes.io/docs/reference/networking/virtual-ips/#external-traffic-policy)), and kind removes the control plane taint only from a cluster of one node ([init.go](https://github.com/kubernetes-sigs/kind/blob/v0.33.0/pkg/cluster/internal/create/actions/kubeadminit/init.go#L124-L126)), so `kind-ha.yaml` keeps Envoy off that node.
The test gives the GatewayClass an `EnvoyProxy` with `externalTrafficPolicy: Cluster`; the gates on one node keep the default.

The tenant is `highAvailability.failureTolerance: Zone` behind the Gateway, and must turn `Ready` within 8 minutes.
Two nodes join by hostname as in the gateway gate.
The Deployment `web` (agnhost, four replicas, spread over the hostnames) runs on both, each pod with the tolerations for `not-ready` and `unreachable` of 300 seconds that admission adds ([admission.go](https://github.com/kubernetes/kubernetes/blob/v1.37.0/plugin/pkg/admission/defaulttolerationseconds/admission.go#L44-L45)).
The test records the UID of every pod in the tenant, and from then on every change of the tenant's Nodes (`Ready`, taints), of the Lease `kube-system/kube-controller-manager`, of the pods, and of the `ControlPlane`'s `Ready`, its inputs and `ControllerManagerHeld`, each with the time the test receives it.

A, both nodes cut:

1. The two nodes are cut within a second of each other, with `iptables -t raw -I PREROUTING 1 -d <ip> -j DROP` and the same on `OUTPUT` in the node's container, towards each address the three endpoint hostnames resolve to.
   A `curl` with a 2 second timeout from each node to each hostname must time out.
2. The Lease's holder is `lodia` within 30 seconds of the later cut, and `ControllerManagerHeld` is True, `NodesSilent`, within a period (5 seconds) of that, with `status.health.controllerManagerHold` counting two of two nodes silent.
3. The container of the controller manager that led has finished within 15 seconds of the holder changing, and no other identity holds the Lease until its release.
4. After 10 minutes the nodes are reconnected.
   The Lease is released, with no holder and a duration of 1 second, 2 minutes after `status.health` first counts fewer silent nodes than the fraction, and at most two periods later; a controller manager holds it within 10 seconds of that.
5. From the cut to a minute after the release no node carries `node.kubernetes.io/unreachable` or `node.kubernetes.io/not-ready` or turns `Ready=Unknown`, every pod has the UID it had, and `Ready` stays True.

B, one node cut, once the tenant has recovered:

6. The Lease is never `lodia`'s and `ControllerManagerHeld` stays False, `NotHeld`.
7. The node turns `Ready=Unknown` between 49.6 seconds and 70 seconds after the cut: the grace period of 60 seconds, less the 10.4 seconds by which the last renewal can precede the cut, and 10 seconds for the two periods of the node monitor, which sees a renewal up to a period late and marks the node on its next pass ([node_lifecycle_controller.go](https://github.com/kubernetes/kubernetes/blob/v1.37.0/pkg/controller/nodelifecycle/node_lifecycle_controller.go#L952-L962); [the margin](../../docs/decisions/controller-manager-hold.md#decision)).
   It carries `node.kubernetes.io/unreachable:NoExecute` within 10 seconds of that.
8. Its `web` pods are deleted 300 seconds, 15 either way, after the taint, and replaced by pods that run on the other node within a minute.
   The DaemonSet pods of the node and the `web` pods of the other one are not deleted.
9. Reconnected, the node turns `Ready` and the taints go within 5 minutes, and the operator's pods are those it had, none restarted.

The bounds are those of the margin, not exact times, since kubelet jitter decides them.
The test lifts the rules when a step ends, pass or fail, and once more when it ends; the node containers and the DNS container go as the gateway gate's do.

## Nightly: conformance and the idle control plane

`TestConformance` checks the conformance item of the [phase 1 gate](../../docs/roadmap.md#phase-1--walking-skeleton), and records the idle footprint its last item measured without asserting it.
It sits behind a build tag of its own, `conformance` with `e2e`, so the per-pull-request suite never builds it; [`nightly.yml`](../../.github/workflows/nightly.yml) runs it every night, on demand, and on a pull request that changes the workflow or the test.
`make conformance` runs it as `make e2e` runs the suite: a kind cluster of its own (`E2E_CLUSTER`), the suite's setup with the operator from the chart, `TestConformance` alone, then the node containers and the cluster removed.
`CONFORMANCE_FOCUS`, a regular expression, takes the place of the focus `\[Conformance\]` and runs the e2e tests it matches, conformance or not ([root.go](https://github.com/kubernetes-sigs/hydrophone/blob/v0.8.0/cmd/root.go#L147-L149), [deploy.go](https://github.com/kubernetes-sigs/hydrophone/blob/v0.8.0/pkg/conformance/deploy.go#L97-L100)), to check the plumbing in a quarter of an hour:

```sh
make conformance E2E_CLUSTER=lodia-e2e-conformance
make conformance E2E_CLUSTER=lodia-e2e-conformance CONFORMANCE_FOCUS='Pods should support retrieving logs from the container over websockets'
```

`go test -artifacts` keeps what the test writes in a directory under `work/e2e/_artifacts/`, which the log's `=== ARTIFACTS TestConformance` line names ([ArtifactDir](https://github.com/golang/go/blob/go1.26.0/src/testing/testing.go#L1325-L1342); Go 1.26 leaves the test's name out of it, [testing.go](https://github.com/golang/go/blob/go1.26.0/src/testing/testing.go#L1380-L1387)): the table of the idle control plane, `idle-control-plane.txt`, and hydrophone's `e2e.log` and `junit_01.xml`; the workflow uploads them.

The tenant has the gate's shape, and two containers of kind's node image, `<cluster>-tenant-node-1` and `-2`, join it as the gate's one does.
Once the `ControlPlane` is `Ready`, both nodes are `Ready` with a ready konnectivity agent and CoreDNS is available, the test leaves the tenant idle for 3 minutes, then reads the resource metrics of the management kubelets, `/metrics/resource`, through the management API server's node proxy, twice, 5 minutes apart, for every container of the control plane's pods.
CPU is the rate of `container_cpu_usage_seconds_total`, the container's cumulative CPU time in core-seconds, between the two samples' timestamps; memory is `container_memory_working_set_bytes` at the second, the working set: recently accessed, dirty and kernel memory ([resource_metrics.go](https://github.com/kubernetes/kubernetes/blob/v1.37.0/pkg/kubelet/metrics/collectors/resource_metrics.go#L51-L63), [collected](https://github.com/kubernetes/kubernetes/blob/v1.37.0/pkg/kubelet/metrics/collectors/resource_metrics.go#L228-L246), [types.go](https://github.com/kubernetes/kubernetes/blob/v1.37.0/staging/src/k8s.io/kubelet/pkg/apis/stats/v1alpha1/types.go#L234-L256), at v1.37.0, the version of kind's node image the management cluster runs).
The table, per container and summed, goes to the test log and the artifacts; [Capacity](../../docs/operations.md#capacity) states it.

Then hydrophone v0.8.0, one of the two runners the CNCF names for conformance ([instructions.md](https://github.com/cncf/k8s-conformance/blob/e40cba7bf280b59739c51e314e77b2fb28c6e1d9/instructions.md#L11-L13)), runs the suite in the tenant, as `go run sigs.k8s.io/hydrophone@v0.8.0`: as a `go tool` it would raise `k8s.io/utils` and `k8s.io/kube-openapi` in `go.mod`.
Its flags:

- `--conformance`, the focus `\[Conformance\]` with no skip ([root.go](https://github.com/kubernetes-sigs/hydrophone/blob/v0.8.0/cmd/root.go#L145-L149), [deploy.go](https://github.com/kubernetes-sigs/hydrophone/blob/v0.8.0/pkg/conformance/deploy.go#L96-L104)), what a certification run takes ([instructions.md](https://github.com/cncf/k8s-conformance/blob/e40cba7bf280b59739c51e314e77b2fb28c6e1d9/instructions.md#L32)): every conformance test runs, none is skipped, retried or focused away; `CONFORMANCE_FOCUS` replaces it with `--focus`;
- no `--conformance-image`: hydrophone takes `registry.k8s.io/conformance:<the tenant's version>` from the tenant's server version ([root.go](https://github.com/kubernetes-sigs/hydrophone/blob/v0.8.0/cmd/root.go#L217-L219));
- `--extra-ginkgo-args=--fail-on-empty=true` with `CONFORMANCE_FOCUS`, so a focus that selects nothing fails ([suite.go](https://github.com/onsi/ginkgo/blob/v2.32.0/internal/suite.go#L527-L530));
- `--parallel 4`, four Ginkgo processes ([flags.go](https://github.com/kubernetes-sigs/hydrophone/blob/v0.8.0/pkg/types/flags.go#L32), [deploy.go](https://github.com/kubernetes-sigs/hydrophone/blob/v0.8.0/pkg/conformance/deploy.go#L125-L127)), with no skip added ([cmd.go](https://github.com/kubernetes/kubernetes/blob/v1.37.0/test/conformance/image/go-runner/cmd.go#L33-L40)): the specs marked `Serial` still run alone, on the first process once the others have finished ([ordering.go](https://github.com/onsi/ginkgo/blob/v2.32.0/internal/ordering.go#L174-L192), [suite.go](https://github.com/onsi/ginkgo/blob/v2.32.0/internal/suite.go#L505-L511), [ginkgowrapper.go](https://github.com/kubernetes/kubernetes/blob/v1.37.0/test/e2e/framework/ginkgowrapper.go#L396-L398); Ginkgo v2.32.0 is the version Kubernetes v1.37.0 builds the conformance image with).
  Kind's own parallel conformance jobs run 25 processes on 4 CPUs and skip the serial specs instead ([kind-presubmits.yaml](https://github.com/kubernetes/test-infra/blob/9b2551f8f32815d93a471123109af4ed36ce827a/config/jobs/kubernetes-sigs/kind/kind-presubmits.yaml#L78-L120), [e2e-k8s.sh](https://github.com/kubernetes-sigs/kind/blob/v0.33.0/hack/ci/e2e-k8s.sh#L253-L261), [ginkgo-e2e.sh](https://github.com/kubernetes/kubernetes/blob/v1.37.0/hack/ginkgo-e2e.sh#L181-L193)).
  The suite waits far more than it computes: with one process it took 1h57m on a 16-CPU workstation, where the kind containers kept a third of a CPU busy; with four it took 44 minutes on GitHub's runner of 2 CPUs, and the nightly job 65;
- `--kubeconfig`, the admin kubeconfig, so that hydrophone reaches the tenant as the suite does; the conformance pod in the tenant uses its in-cluster configuration;
- `--output-dir`, the test's artifact directory, where it downloads `e2e.log` and `junit_01.xml` from the pod by exec ([download.go](https://github.com/kubernetes-sigs/hydrophone/blob/v0.8.0/pkg/conformance/client/download.go#L38-L50));
- `--disable-progress-status`: the progress report execs into the pod every 30 seconds and stops hydrophone when the log it reads lacks the spec count ([logs.go](https://github.com/kubernetes-sigs/hydrophone/blob/v0.8.0/pkg/conformance/client/logs.go#L218-L221), [stops](https://github.com/kubernetes-sigs/hydrophone/blob/v0.8.0/pkg/conformance/client/logs.go#L80-L81)); the streamed log shows the progress.

Hydrophone exits with the conformance container's exit code ([root.go](https://github.com/kubernetes-sigs/hydrophone/blob/v0.8.0/cmd/root.go#L185-L199)), which the image's runner sets to 1 when Ginkgo fails ([main.go](https://github.com/kubernetes/kubernetes/blob/v1.37.0/test/conformance/image/go-runner/main.go#L43-L45), [Wait](https://github.com/kubernetes/kubernetes/blob/v1.37.0/test/conformance/image/go-runner/main.go#L84-L87)), and with 1 on an error of its own ([main.go](https://github.com/kubernetes-sigs/hydrophone/blob/v0.8.0/main.go#L27-L29)); the test passes only on 0.
`make conformance` gives the test 110 minutes, and the nightly job has 120.
