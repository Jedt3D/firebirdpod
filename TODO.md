# TODO

This file keeps the deliberate follow-up work visible after the Phase 04
close-out. These are not hidden defects. They are the next set of decisions we
chose not to force into the finished module-and-admin phase.

## Revisit Queue

| Item | Why It Matters | When To Revisit | Trigger To Pull Forward |
| --- | --- | --- | --- |
| Auth indexed-text compatibility revisit | The current `serverpod_auth_idp` policy keeps some indexed auth text on bounded ASCII-backed storage to stay inside the shared Firebird test key budget. That is workable, but it is still a compatibility tradeoff rather than the final design. | Early Phase 05 or the first module-expansion pass after performance work is stable. | Pull this forward if a real auth flow needs a wider or non-ASCII indexed key that the current policy cannot support cleanly. |
| Broader Firebird error-shape cataloging | Phase 01 and the later integration slices already map many common runtime failures, but DDL, metadata-lock, permission, and service-manager failure shapes can still be classified more precisely. | During the next round of schema, admin, or release hardening work. | Pull this forward if migration or admin failures start surfacing as generic exceptions where the caller clearly needs finer-grained behavior. |
| Async cancel control plane | Statement timeout is the current production control baseline. The low-level cancel seam exists, but it is not yet a trustworthy end-user cancellation story. | Phase 05 or the first release-hardening pass that focuses on operational control. | Pull this forward if timeout-only control proves too blunt for real application workflows. |
| Array support | Firebird arrays are still outside the supported baseline. They would add type-mapping, binding, result-decoding, and schema-tooling work that should be justified by a real Serverpod-facing need. | After the Phase 05 and Phase 06 baselines are stable enough to evaluate feature expansion calmly. | Pull this forward only if a concrete model or user story needs native arrays. |
| Admin hardening and permissions model | The service-manager seam now covers validation, statistics, backup, restore, sweep, and shutdown or online control, but the safety model is still mostly test-harness driven. | Phase 06 hardening and release. | Pull this forward if admin endpoints or deployment tooling need stronger privilege separation before then. |
| Isolated live databases for more parallelism | The live suite is stable with serial execution, but several files still share one Firebird database. Isolated databases per file would give cleaner parallel execution and reduce test-coupling pressure. | Phase 06 or the next test-harness cleanup window. | Pull this forward if serial live execution becomes a significant productivity bottleneck. |

## Notes

- The compatibility and error-shape items should be driven by real Firebird
  behavior we observe in tests or product flows, not by abstract completeness.
- The admin hardening item is about safety boundaries and deployment reality,
  not about whether the underlying service-manager calls work. They already do.
- The array item stays deliberately late because the current project goal is a
  strong Firebird-native Serverpod backend, not maximum Firebird surface area.
