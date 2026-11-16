---
paths:
  - "api/**"
  - "internal/**"
---

# Where shared names live

- A name, a key or a value that more than one controller shares has one shape and one writer, in one of two homes ([decision](../../docs/decisions/independent-controllers.md)).
- `api/v1alpha1` holds what an importer may rely on: the types, the conditions and their reasons, the labels and annotations, the Secrets of a tenant namespace with their keys, the API server's Service, and the objects a platform handles.
- `internal/contract` holds what only Lodia's controllers share; a name moves to `api/v1alpha1` only when an importer needs it, and from then on it is API.
- A name only its writer uses stays in the writer's package.
