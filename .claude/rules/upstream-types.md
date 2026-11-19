---
paths:
  - "api/v1alpha1/authentication_types.go"
  - "internal/authnconfig/**"
---

# Upstream's types copied into the API

- `api/v1alpha1/authentication_types.go` is kube-apiserver's `JWTAuthenticator` field for field, and changes only with `internal/authnconfig/drift_test.go` passing ([decision](../../docs/decisions/structured-authentication.md)).
- A bump of `k8s.io/apiserver` that changes the type adds the field to the copy with its bounds, and the schema refuses it while `spec.version`'s minor is older than the first that knows it.
