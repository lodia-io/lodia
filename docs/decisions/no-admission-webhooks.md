# Lodia runs no admission webhooks: validation and defaults live in the CRD schema

Decided 2026-10-25. The schema rules and defaults are
built; the conditions and the tenant write lock come with their controllers.

## Decision

- Every validation of a Lodia object is a CEL rule
  (`x-kubernetes-validations`) or an OpenAPI constraint in the CRD schema:
  formats, enums, unions ("exactly one of `dedicated` and `shared`"),
  immutability (`self == oldSelf`), and transitions such as "the Kubernetes
  minor never decreases".
- Defaults are schema defaults.
- What a schema cannot check (a `DatastoreClass` that does not exist, a
  Gateway that lacks a listener) is reported as a condition on the object
  with a reason, not rejected at admission.
- Inside a tenant cluster, a write lock (for migration or maintenance) is a
  `ValidatingAdmissionPolicy` the Tenant controller applies, not a webhook
  that calls back into the operator.
- `v1alpha1` is the only served and stored version; a later version is added
  only with a schema that converts with `strategy: None`, or with a conversion
  webhook as a deliberate, separately decided exception.

## Why

- A webhook with `failurePolicy: Fail` turns an operator outage into an API
  outage for its objects: nobody can change a cluster while the operator is
  down, which is exactly when they need to. `Ignore` is not better: it
  silently skips validation.
- CEL in the schema is evaluated by the API server itself, versioned with the
  CRD and testable with envtest; transition rules and ratcheting cover the
  cases that used to need a webhook
  ([Validation rules](https://kubernetes.io/docs/tasks/extend-kubernetes/custom-resources/custom-resource-definitions/#validation-rules)).
- An operator without webhooks needs no serving certificate, no
  `MutatingWebhookConfiguration` and no special install order; the Helm
  chart stays plain.

## Rejected

- Validating and mutating webhooks for "richer" checks — the richer checks
  are cross-object lookups, and a condition explains them better than an
  admission error.
- A webhook only for defaulting — schema defaults cover it.

## Consequences

- Some invalid states are accepted and then reported: `Ready=False`,
  reason `DatastoreClassNotFound`. Documentation and the CLI show conditions
  first.
- The CEL cost budget bounds rule complexity; rules are kept per field.

## Code

- `api/v1alpha1/*_types.go` — every rule and default as a
  `+kubebuilder:validation` or `+kubebuilder:default` marker;
  `api/crds/` is generated from them.
- `test/api/validation_test.go` — the envtest table of accepted and
  rejected manifests.
