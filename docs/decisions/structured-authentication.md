# A tenant's JWT authenticators are upstream's `JWTAuthenticator`, reached through the tenant's nodes and reloaded without a restart

Decided 2026-11-19.
Built; `api/v1alpha1/authentication_types.go`, `internal/authnconfig` and Workloads hold it, with one workaround, the [compiler of a minor](#workaround-the-compiler-of-a-minor).

## Decision

- **`spec.apiServer.authentication.jwt` is a copy of kube-apiserver's `apiserver.config.k8s.io/v1` `JWTAuthenticator`**, field for field, with Lodia's bounds and the CEL rules a schema can see ([field reference](../api/controlplane.md#spec)).
  A test holds every JSON path of upstream's type, in the `k8s.io/apiserver` the operator links, to a path of the copy of the same kind, and finds none more; a filled object of either type survives a strict decode into the other and back.
  A bump of `k8s.io/apiserver` that changes the type fails it, and the pull request that bumps adds the field with its bounds and the first minor that knows it ([rule](../../.claude/rules/upstream-types.md)).
- **Workloads renders the list into the `AuthenticationConfiguration` it already writes**, `<cp>-authentication-config`, beside its own anonymous section, converted to upstream's type by a case-sensitive strict decode of its JSON, as kube-apiserver decodes the file ([authentication.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/pkg/kubeapiserver/options/authentication.go#L873-L915)).
- **Every issuer is reached through konnectivity**: Workloads sets `egressSelectorType: cluster` on each authenticator, and the schema admits `cluster` alone, defaults to it, and refuses `StructuredAuthenticationConfigurationEgressSelector: false`.
  kube-apiserver then sends every request for that issuer, discovery and keys, through the dialer of the egress selection `cluster` ([oidc.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiserver/plugin/pkg/authenticator/token/oidc/oidc.go#L317-L340)), which Lodia's egress configuration sends through konnectivity-server's socket to the agents on the tenant's nodes ([api-exposure](api-exposure.md)).
- **Every authenticator ends with a user validation rule of Lodia's**: `!user.username.startsWith('system:') && !user.username.startsWith('lodia:') && !user.groups.exists(g, g.startsWith('system:') || g.startsWith('lodia:'))`, with the message `system: and lodia: names are reserved`.
  An authenticator whose spec holds that expression keeps its own rule, since kube-apiserver refuses an expression twice ([validation.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiserver/pkg/apis/apiserver/validation/validation.go#L546-L566)).
- **Workloads writes a file only once upstream's validation passes it for each minor that reads it.**
  It decodes the bytes it renders as kube-apiserver decodes them, and runs upstream's validation for each minor that reads the file: those of `status.components.apiServer.version` and `status.target.version`, each with the CEL libraries and the CEL parser of that minor, with the issuer Workloads renders for ServiceAccounts as the one no authenticator may name ([authentication.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/pkg/kubeapiserver/options/authentication.go#L602-L604)), and under the API server's `featureGates`.
  Upstream's validation reads the gates of the process it runs in ([validation.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiserver/pkg/apis/apiserver/validation/validation.go#L64)), so Workloads refuses the `egressSelectorType` of each authenticator itself, as upstream does ([L230-L232](https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiserver/pkg/apis/apiserver/validation/validation.go#L230-L232)), when they turn `StructuredAuthenticationConfigurationEgressSelector` off, as a `ControlPlane` stored before the schema refused that may.
  A test holds it to kube-apiserver's own check, built from each minor's `k8s.io/apiserver` and so its own cel-go, on a corpus of expressions: they agree on each, except where Workloads refuses what a minor accepts, `duration()` of an int at 1.35 and 1.36, which cel-go v0.28.1 removed ([cel-go#1300](https://github.com/google/cel-go/pull/1300)) and 1.37 refuses too; a file Workloads passes and a minor refuses fails it.
  A file that fails is not written: the ConfigMap stays as it is, or is created without JWT authenticators while it is missing, every other object of the pass is applied, and `APIServerReady` is False, `AuthenticationInvalid`, with the minor and the first three errors, unless it is False already, `ProgressDeadlineExceeded` without a ready replica or `Unavailable` ([reasons](../api/controlplane.md#conditions-and-their-reasons)).
  The outcome is kept for each `ControlPlane` with the bytes, minors and issuer it was found for, so a pass that changes nothing compiles nothing.
- **The CEL libraries of a minor are its `NewExpressions` environment**: `environment.MustBaseEnvSet(minor)` of the `k8s.io/apiserver` the operator links, with the `claims` and `user` variables as upstream declares them.
  kube-apiserver compiles in its `StoredExpressions` environment, every library its release knows ([compile.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiserver/pkg/authentication/cel/compile.go#L75)), and 1.37 adds the lists library v1 ([base.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiserver/pkg/cel/environment/base.go#L182-L187)), so an expression with `includes` starts 1.37 and stops 1.35.
- **The CEL parser of a minor is that of the cel-go it links**: 1.35 and 1.36 link cel-go v0.26.0 ([v1.35.10](https://github.com/kubernetes/kubernetes/blob/v1.35.10/go.mod), [v1.36.6](https://github.com/kubernetes/kubernetes/blob/v1.36.6/go.mod)) and 1.37 v0.29.2 ([v1.37.2](https://github.com/kubernetes/kubernetes/blob/v1.37.2/go.mod)), as the operator does, and cel-go parses backtick-escaped identifiers, `` claims.`custom-tenant` ``, from v0.28.0 on ([cel-go#1295](https://github.com/google/cel-go/pull/1295)).
  Workloads parses each expression for 1.35 and 1.36 also without them, as v0.26.0 does.
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
- **Validation at the newest minor alone**, upstream's compiler as it is: an expression with a function of 1.37 would pass for a 1.35 tenant, whose API server would then refuse the file at its next start.
- **A condition outside `Ready` for an invalid configuration**: `Ready` would claim an edit taken up that was not.
- **A direct dial from the API server's pod**: it reaches the management network; one limited to public networks is a loosening for later, with the egress policy that would enforce it.

What the neighbours do is in [analysis](../analysis.md).

## Consequences

- Before a tenant's first node joins, no agent is connected: a token's login fails and an edit does not load, while the admin kubeconfig and `KubeconfigRequest`s work ([short-lived-kubeconfigs](short-lived-kubeconfigs.md)).
- An issuer that does not answer holds back every edit of the file while it stays in it, and kube-apiserver logs `failed to update authentication config`.
- A platform that wants OIDC administrators binds a group of its own inside the tenant; `lodia:cluster-admins` stays the certificates' group.
- During a minor step both minors read the file, so an expression of the newer minor is refused until the API server reports it, and then written.
  A file valid at one minor and refused by the next is what a rehearsal of the step would find; nothing checks the next minor before its step starts.
- `Ready` at a generation says that the file is written and valid, not that every replica has loaded it (`todo.md`): an issuer that does not answer holds the load back, which kube-apiserver's metrics and log tell ([operations](../operations.md#logging-in-with-an-oidc-provider)).

## Workaround: the compiler of a minor

`internal/authnconfig/compiler.go` mirrors the unexported compiler of `k8s.io/apiserver` and the two functions that build its environments ([compile.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiserver/pkg/authentication/cel/compile.go#L34-L161)), since upstream builds a compiler only over every library of its own release, and the parser of cel-go v0.26.0 for 1.35 and 1.36.
`TestValidateAgreesWithTheKubeAPIServerOfEveryMinorOnTheCorpus`, in `make test`, builds `test/kubeauthn`, kube-apiserver's decode and validation of the file, once for each minor Lodia runs against that minor's `k8s.io/apiserver` (`test/kubeauthn/<minor>.mod`), and holds `Validate` to it on every expression of `internal/authnconfig/testdata/corpus.txt`, each claim expression as a username, a claim rule and an extra value and each user expression as a user rule.
The corpus has a case for each change in cel-go's release notes from v0.26.0 to the version the operator links that touches parsing, checking or the standard library; a bump of cel-go or `k8s.io/apiserver` reruns the test, and a minor added to the table needs its reference.
It goes, with its `@todo`, once `k8s.io/apiserver` builds a compiler of a given version.

Upstream issue: (filed by the coordinator)
Notion task: (filed by the coordinator)

## Code

- `api/v1alpha1/authentication_types.go` — the copy; `test/api/authentication_test.go` holds each CEL rule, the default egress and the feature gate.
- `internal/authnconfig` — `Upstream`, the conversion; `drift_test.go` holds the shape and the round trips. `Validate` and `compiler.go`, the validation of each minor; `kubeapiserver_test.go` holds it to each minor's kube-apiserver, built from `test/kubeauthn`, on the corpus, and `validate_test.go` holds `includes` from 1.37, an unknown field, a CA, the ServiceAccount issuer and the bounds of the message.
- `internal/controller/workloads/workloads.go` — `validated`, `validate`, `readers` and `authenticated`, whose order among the availability reasons `authentication_internal_test.go` holds; `authentication_test.go` holds an edit that reloads, an invalid one never written beside an edit that rolls out, a 1.35 tenant that gets no expression of 1.37, a minor step that gets one only once the API server reports 1.37, a JWT issuer refused while it is the ServiceAccount issuer and written once it is not, and a `ControlPlane` stored with the egress selector's gate off that gets no JWT authenticator; `authentication_internal_test.go` also holds the validation dropped with its `ControlPlane`.
- `internal/controller/workloads/render.go` — `jwtAuthenticators` and `configMaps`; `authentication_internal_test.go` holds the egress and the reserved names once on every authenticator, the restart hash of before for any `jwt` and its change with the anonymous section, and the golden `testdata/render/authentication.yaml`.
