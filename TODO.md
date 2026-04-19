# TODO

This file keeps the deliberate follow-up work visible after the completed
Phase 04 close-out and the current Phase 05 baseline through Slice `05L`.
These are not hidden defects. They are the next decisions we chose not to
force into the current observability and benchmark baseline.

## Revisit Queue

| Item | Why It Matters | When To Revisit | Trigger To Pull Forward |
| --- | --- | --- | --- |
| Auth indexed-text compatibility revisit | The current `serverpod_auth_idp` policy keeps some indexed auth text on bounded ASCII-backed storage to stay inside the shared Firebird test key budget. That is workable, but it is still a compatibility tradeoff rather than the final design. | After Phase 05 closes or during the first broader module-expansion pass that needs auth changes. | Pull this forward if a real auth flow needs a wider or non-ASCII indexed key that the current policy cannot support cleanly. |
| Broader Firebird error-shape cataloging | Phase 01 and the later integration slices already map many common runtime failures, but DDL, metadata-lock, permission, and service-manager failure shapes can still be classified more precisely. | During the next round of schema, admin, or release hardening work. | Pull this forward if migration or admin failures start surfacing as generic exceptions where the caller clearly needs finer-grained behavior. |
| Async cancel control plane | Phase 05 now proves the current boundary more explicitly: same-isolate `raise` reports `nothing to cancel`, while `abort` kills the client connection but is still only a low-level operator hook. That is useful observability, but it is not yet a trustworthy end-user cancellation story. | Late Phase 05 close-out or the first release-hardening window that focuses on operational control. | Pull this forward if timeout-only control proves too blunt for real application workflows, or if product requirements need real mid-flight cancel from UI, RPC, or admin tooling. |
| Hosted CI benchmark adoption and long-run calibration | The repo now has both an `employee.fdb` smoke policy and a broader calibrated candidate policy that selects one scenario per supported benchmark database. What is still unresolved is which of those policies should become hosted checks, how much hosted-machine drift to tolerate over time, and which scenarios should eventually graduate from smoke or candidate status into real release gates. | Late Phase 05 or the first release-engineering pass after the current repo-local policies have seen real use. | Pull this forward if the employee smoke gate and the multi-database candidate policy both stay stable long enough that we want a real hosted regression contract instead of a repo-only signal. |
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
