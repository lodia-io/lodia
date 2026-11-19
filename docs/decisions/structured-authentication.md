# A tenant's JWT authenticators are upstream's `JWTAuthenticator`, reached through the tenant's nodes and reloaded without a restart

Decided 2026-11-19.
Built; `api/v1alpha1/authentication_types.go`, `internal/authnconfig` and Workloads hold it.

## Decision

- **`spec.apiServer.authentication.jwt` is a copy of kube-apiserver's `apiserver.config.k8s.io/v1` `JWTAuthenticator`**, field for field, with Lodia's bounds and the CEL rules a schema can see ([field reference](../api/controlplane.md#spec)).
  A test holds every JSON path of upstream's type, in the `k8s.io/apiserver` the operator links, to a path of the copy of the same kind through the same pointers, and finds none more; a filled object of either type survives a strict decode into the other and back.
  A bump of `k8s.io/apiserver` that changes the type fails it, and the pull request that bumps adds the field with its bounds and the first minor that knows it ([rule](../../.claude/rules/upstream-types.md)).
- **Workloads renders the list into the `AuthenticationConfiguration` it already writes**, `<cp>-authentication-config`, beside its own anonymous section, converted to upstream's type by a case-sensitive strict decode of its JSON, as kube-apiserver decodes the file ([authentication.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/pkg/kubeapiserver/options/authentication.go#L873-L915)).
- **Every issuer is reached through konnectivity**: Workloads sets `egressSelectorType: cluster` on each authenticator, and the schema admits `cluster` or nothing, defaults to it, and refuses `StructuredAuthenticationConfigurationEgressSelector: false`.
  kube-apiserver then sends every request for that issuer, discovery and keys, through the dialer of the egress selection `cluster` ([oidc.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiserver/plugin/pkg/authenticator/token/oidc/oidc.go#L317-L340)), which Lodia's egress configuration sends through konnectivity-server's socket to the agents on the tenant's nodes ([api-exposure](api-exposure.md)).
- **Every authenticator ends with a user validation rule of Lodia's**: `!user.username.startsWith('system:') && !user.username.startsWith('lodia:') && !user.groups.exists(g, g.startsWith('system:') || g.startsWith('lodia:'))`, with the message `system: and lodia: names are reserved`.
  An authenticator whose spec holds that expression keeps its own rule, since kube-apiserver refuses an expression twice ([validation.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiserver/pkg/apis/apiserver/validation/validation.go#L546-L566)).
- **Workloads writes a file only once kube-apiserver of the minor the operator links would load it**, for every tenant: the minor of its `k8s.io/apiserver`, 1.37 today, which a test holds to the newest minor Lodia runs.
  It decodes the bytes it renders as kube-apiserver decodes them and runs upstream's validation with upstream's default compiler, as kube-apiserver does, with the issuer Workloads renders for ServiceAccounts as the one no authenticator may name ([authentication.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/pkg/kubeapiserver/options/authentication.go#L602-L604)).
  That validation reads the feature gates of the process it runs in, the operator's, not the API server's `featureGates` ([validation.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiserver/pkg/apis/apiserver/validation/validation.go#L64)), so the schema refuses the egress selector's gate turned off.
  A file that fails is not written: the ConfigMap stays as it is while this `ControlPlane` controls it and it passes the same validation, and is otherwise written without JWT authenticators, so that no replica, of a rollout or one that replaces a lost one, reads a file it refuses; every other object of the pass is applied, and `APIServerReady` is False, `AuthenticationInvalid`, with that minor and the first three errors, unless it is False already, `ProgressDeadlineExceeded` without a ready replica or `Unavailable`, which then carries the refusal after its message ([reasons](../api/controlplane.md#conditions-and-their-reasons)); a held pass and a wait, which roll nothing and count the Deployments for the generation the condition names, count with the refusal that condition carries, which the pass that wrote it found for what it rendered.
  The outcome is kept for each `ControlPlane` with the bytes and issuer it was found for, so a pass that changes nothing compiles nothing.
- **A JWT edit reloads, and only the anonymous section restarts the API server.**
  The API server's restart hash reads the authentication configuration rendered without JWT authenticators, which are the bytes it read before this decision, so the release that brings it rolls no tenant and an edit of `jwt` rolls nothing.
  kube-apiserver watches the file and polls it every minute, and swaps in new bytes once they decode, validate and every issuer in them answers, within a minute; it refuses a changed anonymous section ([authentication.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/pkg/kubeapiserver/options/authentication.go#L760-L815), [config.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/pkg/kubeapiserver/authenticator/config.go#L314-L337)), so an operator revision that changes that section rolls the API server.

## Why

- **Upstream's type is the file its users already write.**
  A copy keeps `go.lodia.io/lodia/api` on `k8s.io/api` and `k8s.io/apimachinery` alone, so an importer keeps its own client-go ([releases-and-support](releases-and-support.md)); importing `k8s.io/apiserver` would add it and `k8s.io/component-base` to the API module, and controller-gen would build the schema from upstream's markers, which carry no bounds.
  A generator would need the same bounds chosen by hand and would add a field silently with a dependency bump; the test makes the bump decide.
- **The platform takes the whole `jwt` section as the tenant's administrator wrote it** ([who-is-trusted](who-is-trusted.md)): the URLs kube-apiserver fetches, the CA it trusts for them, the expressions it evaluates on every token.
  Through `cluster` the API server reaches an issuer only from the tenant's own nodes, where that administrator runs pods already, and never the management network, a node's metadata service or another tenant's Services.
- **A claim mapped with an empty prefix, or any expression, can yield any name**, `system:masters` and `lodia:user:alice` among them.
  The reserved names keep a token from being a node, a ServiceAccount or a member of `system:masters`, and from holding the bindings of a person's short-lived kubeconfig ([short-lived-kubeconfigs](short-lived-kubeconfigs.md)), so the two credential systems stay apart.
  kube-apiserver adds `system:authenticated` after the rule, so the rule does not refuse every token.
- **kube-apiserver takes a JWT edit without a restart**, so a roll for each would cost every replica for nothing.
- **kube-apiserver stops at start on a file it cannot decode or validate**, and refuses the same file at a reload and keeps the authenticators it has ([authentication.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/pkg/kubeapiserver/options/authentication.go#L787-L806)).
  Written, such a file would stop the next replica that starts, in a rollout or after a node is lost.
  No CRD rule compiles CEL or parses PEM, and no admission webhook may ([no-admission-webhooks](no-admission-webhooks.md)), so the check runs before the write, seconds after the edit, and leaves the cluster on its last valid authenticators.
- **`Ready` True at a generation means every change of it taken up** ([field reference](../api/controlplane.md#conditions-and-their-reasons)), which a configuration not written is not; `Refused` already makes `APIServerReady` False for a render the management API server refuses while the tenant's API server serves.

## Rejected

- **A flag and a file of the tenant's**, through `extraArgs`: no tenant can put a file into the API server's pod, and the schema refuses `authentication-config` and every `oidc-*` flag.
- **A reference to a ConfigMap of the tenant's**: without admission webhooks ([no-admission-webhooks](no-admission-webhooks.md)) nothing would check it before a reconcile, and the operator's cache holds only labelled objects ([independent-controllers](independent-controllers.md)).
- **A dialect of Lodia's own**: a third shape for one file, with fields upstream has and it lacks.
- **A roll on every edit**: kube-apiserver needs none.
- **Validation for each minor that reads the file**: upstream's compiler takes every library of its own release ([compile.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiserver/pkg/authentication/cel/compile.go#L75)), so an older minor would need a mirror of that compiler and of its cel-go, or its own validation built and run beside the operator.
- **A condition outside `Ready` for an invalid configuration**: `Ready` would claim an edit taken up that was not.
- **A direct dial from the API server's pod**: it reaches the management network; one limited to public networks is a loosening for later, with the egress policy that would enforce it.

What the neighbours do is in [analysis](../analysis.md).

## Consequences

- Before a tenant's first node joins, no agent is connected: a token's login fails and an edit does not load, while the admin kubeconfig and `KubeconfigRequest`s work ([short-lived-kubeconfigs](short-lived-kubeconfigs.md)).
- An issuer that does not answer holds back every edit of the file while it stays in it, and kube-apiserver logs `failed to update authentication config`.
- A platform that wants OIDC administrators binds a group of its own inside the tenant; `lodia:cluster-admins` stays the certificates' group.
- A tenant of an older minor whose configuration only a newer minor accepts, a CEL function or syntax its kube-apiserver lacks, as `includes`, which 1.37 adds ([base.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiserver/pkg/cel/environment/base.go#L182-L187)), gets the file: its API server keeps its last configuration on a reload and does not start on it.
  Whoever writes the configuration answers for it, by the owner's decision of 2026-11-19.
  A configuration that only an older minor accepts, as `duration()` of an int, which the cel-go of 1.35 and 1.36 declares and that of 1.37 does not ([v0.26.0](https://github.com/google/cel-go/blob/v0.26.0/common/stdlib/standard.go#L608-L609)), is refused for every tenant.
- `Ready` at a generation says that the file is written and valid, not that every replica has loaded it (`todo.md`): an issuer that does not answer holds the load back, which kube-apiserver's metrics and log tell ([operations](../operations.md#logging-in-with-an-oidc-provider)).

## Code

- `api/v1alpha1/authentication_types.go` — the copy; `test/api/authentication_test.go` holds each CEL rule, the default egress, the feature gate, and the URLs and empty strings it accepts as kube-apiserver does.
- `internal/authnconfig` — `Upstream`, the conversion; `drift_test.go` holds the shape and the round trips. `Validate`, kube-apiserver's load of the file; `validate_test.go` holds the minor of the linked `k8s.io/apiserver` to the newest Lodia runs, `includes`, an unknown field, a CA, the ServiceAccount issuer and the bounds of the message.
- `internal/controller/workloads/workloads.go` — `validated`, `validate`, `authenticated` and `carried`, whose order among the availability reasons and the refusal a False count carries `authentication_internal_test.go` holds; `authentication_test.go` holds an edit that reloads, an invalid one never written beside an edit that rolls out, and, after a refusal, the file without JWT authenticators in place of one a new replica would refuse, under a ServiceAccount issuer moved onto an issuer's URL by the spec or by the load balancer's address, a predecessor's orphan and a hand edit, and the file of this `ControlPlane` kept without a roll, each passed by `Validate` with that replica's ServiceAccount issuer and, at envtest's minor, loaded by its kube-apiserver, and held passes and waits that count no refused configuration as available, at its generation and at a later one, also when it was refused while the API server had no ready replica, with `Ready` False and no grant from Lifecycle, and count a served file as served once the issuer moves under them, with `Ready` True and the grant; `apiserver_test.go` holds a real kube-apiserver of envtest's minor that loads and reloads the file Workloads renders, refuses the reserved names, and stops on an unknown field and on its ServiceAccount issuer as a JWT issuer, as `Validate` refuses them; `authentication_internal_test.go` also holds which file a refused pass keeps and what its message says, and the validation dropped with its `ControlPlane`.
- `internal/controller/workloads/render.go` — `jwtAuthenticators` and `configMaps`; `authentication_internal_test.go` holds the egress and the reserved names once on every authenticator, the restart hash of before for any `jwt` and its change with the anonymous section, and the golden `testdata/render/authentication.yaml`.
