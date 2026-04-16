# TODO

Deferred follow-ups that are worth keeping visible after the Phase 01 adapter
baseline close-out.

## Revisit Queue

| Item | Why It Matters | When To Revisit | Trigger To Pull Forward |
| --- | --- | --- | --- |
| Broader Firebird error-shape cataloging and more DDL-oriented failure coverage | Serverpod integration, schema tooling, and migration flows will need cleaner classification of metadata-lock, dependency, permission, duplicate-object, and invalid-DDL failures than the Phase 01 adapter baseline needed. | Revisit in early Phase 02, right after the first end-to-end Serverpod runtime-dialect slice is green and before we start serious migration or schema-management work. | Pull this forward immediately if the first Serverpod-facing integration starts masking DDL failures behind generic adapter errors. |
| Array support | Firebird arrays are not part of the current baseline, and they add type-mapping, binding, result-decoding, and schema-tooling complexity that should be justified by a real Serverpod-facing use case. | Revisit in Phase 03, after the first Serverpod model, migration, and query pipeline is stable enough to prove whether arrays are actually needed. | Pull this forward only if a concrete application model or user story requires native Firebird arrays before the Phase 03 backlog is reached. |

## Revisit Notes

- The error-shape catalog should be driven by real Firebird DDL and schema
  workflows, not by hypothetical lists copied from other drivers.
- Array support should be evaluated against actual Serverpod model semantics,
  migration output, and query behavior so we do not add a low-value type
  surface too early.
