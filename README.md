# firebirdpod

`firebirdpod` is the implementation repository for a Firebird-native Serverpod
backend. This is where the runtime adapter, Serverpod integration, schema
tooling, module support, admin operations, and the real test evidence all land.

## Status At A Glance

- Phase 01 is complete: Firebird adapter, direct `fbclient` transport,
  transaction policy, timeout handling, and lifecycle coverage are in place.
- Phase 02 is complete through Slice `02E`: Firebird-backed Serverpod runtime
  registration, raw execution, generated reads and writes, relation loading,
  and a minimal real `Serverpod` proof all work.
- Phase 03 is complete through Slice `03D`: Firebird-native schema generation,
  migration execution and locking, schema drift analysis, and sample-database
  validation are in place.
- Phase 04 is complete through Slice `04F`: `serverpod_auth_core`,
  `serverpod_auth_idp`, and Firebird service-manager admin operations are all
  proven live.
- Phase 05 is underway through Slice `05H`: slices `05A` through `05H` are
  complete. Typed Firebird monitoring helpers, API-backed query-plan
  inspection, timeout diagnostics, read-consistency validation, cancellation
  capability diagnostics, benchmark baselines, and a repo-local benchmark
  snapshot comparison workflow are in place for live attachment, transaction,
  statement, optimizer-plan, timeout-budget, transaction-visibility,
  cancel-path, and query-class timing diagnostics across the shared
  `employee.fdb` proof fixture plus the converted `chinook` and `northwind`
  samples.
- The next major Phase 05 frontier is turning that local evidence into a more
  repeatable regression and tuning workflow, plus a small set of follow-up
  compatibility items tracked in [TODO.md](TODO.md).

## What Already Works

### Firebird Runtime

- Firebird query compilation from named or positional application parameters
  into Firebird-ready `?` bindings.
- Prepared SQL templates that parse once, bind many times, and reject mixed
  placeholder styles.
- An owned direct `fbclient` transport for attachments, statements,
  transactions, savepoints, generated IDs, timeout control, and retained-state
  reset for pooled reuse.
- Structured Firebird exception mapping and scalar, date, blob, and newer
  Firebird 5 value decoding.
- A small `fbdb` prototype path that still proves the seam shape separately.

### Serverpod Integration

- Firebird Serverpod config parsing, dialect registration, provider creation,
  pool-manager wiring, and transaction bridging.
- Raw execution through `query(...)`, `execute(...)`, `simpleQuery(...)`, and
  `simpleExecute(...)`.
- Generated read support for `find(...)`, `findFirstRow(...)`, `findById(...)`,
  `count(...)`, pagination, and `FOR UPDATE WITH LOCK`.
- Generated write support for insert, update, delete, ordered selection before
  mutation, multi-row atomicity, and `lockRows(...)`.
- Relation loading with object includes, list includes, relation-aware filters,
  relation-aware sorting, and per-parent list pagination.
- A minimal Firebird-backed Serverpod app proof against `employee.fdb`.

### Schema And Migration Tooling

- Firebird-native schema-definition SQL generation from `DatabaseDefinition`.
- Firebird-native migration SQL generation from `DatabaseMigration`.
- Firebird migration execution with batch-aware simple SQL support, a committed
  lock-table bootstrap, and one-migration-at-a-time locking.
- Firebird-owned schema introspection and drift reporting through
  `FirebirdServerpodDatabaseAnalyzer`.
- Converted and curated sample-database validation, including zero-gap gating
  for the curated native fixtures.

### Modules And Admin

- Live Firebird module proof for `serverpod_auth_core`.
- Live Firebird module proof for `serverpod_auth_idp`, including the current
  indexed-text compatibility policy for auth-style lookup keys.
- Firebird service-manager support for server version, validation, statistics,
  backup, restore, sweep, shutdown, and online operations.
- Safe live admin coverage that uses restored temporary databases for
  destructive service-manager proofs instead of taking the shared source
  fixture offline.

### Performance And Observability

- Typed Firebird monitoring reads over `MON$ATTACHMENTS`,
  `MON$TRANSACTIONS`, and `MON$STATEMENTS`.
- Snapshot helpers for current-attachment and external-attachment monitoring
  views.
- Monitoring-statistics snapshots and delta helpers for attachment,
  transaction, and statement-level Firebird I/O, record, memory, and table
  counters.
- One-shot legacy and detailed query-plan inspection through the direct
  `fbclient` statement API.
- Timeout diagnostics for connection defaults, prepared-statement overrides,
  and observed timeout-classified executions.
- Transaction read-consistency state capture and live visibility proofs for
  `READ COMMITTED READ CONSISTENCY` versus `SNAPSHOT`.
- Cancellation diagnostics that capture low-level `cancelCurrentOperation()`
  request outcomes and post-request connection usability.
- Live cancellation proofs that show the current same-isolate `raise` path
  reports `nothing to cancel`, while `abort` invalidates the client
  connection and clears monitored statements on the worker attachment.
- A repeatable benchmark runner for the shared `employee.fdb` proof fixture
  plus converted `chinook` and `northwind` query classes, with warmup passes,
  measured iterations, duration statistics, and captured explained plans.
- A runnable benchmark report tool that prints markdown-ready baselines for the
  supported sample datasets.
- Committed benchmark snapshots for `employee.fdb` plus the converted
  `chinook` and `northwind` fixtures, with comparison policies that gate row
  shape, column shape, timing regressions through ratio-plus-delta
  thresholds, and optional plan drift.
- A repo-local benchmark workflow guide under `benchmarks/` that describes how
  to compare a fresh run, inspect drift, and refresh an accepted baseline.
- Live monitoring proofs on temporary restored databases so attachment and
  transaction counts stay isolated and deterministic on this machine.

## Current Boundaries

- Pool-level Serverpod `runtimeParametersBuilder` is still intentionally
  unsupported on the Firebird path.
- `ignoreConflicts: true` is still rejected until there is a Firebird-native
  policy for it.
- Auth indexed email and rate-limit nonce storage still use an ASCII-backed
  indexed policy to stay inside the shared test database key budget.
- `cancelCurrentOperation()` now has typed diagnostics, but the current direct
  adapter still does not claim true user-facing mid-flight async cancellation.
  The live proof says the same-isolate `raise` path reports `nothing to
  cancel`; a real async control plane is still a later feature.
- Benchmark comparison is currently a repo-local workflow on the converted
  fixtures. It does not yet run as automated CI gating, and the default local
  timing budgets are intentionally looser than a strict 10% threshold because
  these shared converted fixtures show repeatable jitter on this machine even
  when row shape and plans stay stable. The current policy therefore combines
  ratio checks with small absolute-drift floors instead of pretending every
  few-millisecond wobble is a real regression.

## Test Commands

- Fast default verification:
  - `dart test`
- Full live direct verification:
  - `FIREBIRDPOD_RUN_FBCLIENT_DIRECT=1 dart test -j 1`
- `fbdb` prototype verification:
  - `FIREBIRDPOD_RUN_FBDB_PROTOTYPE=1 dart test test/firebird_fbdb_prototype_integration_test.dart`

The live suite still runs most reliably with `-j 1`. Most live tests share one
Firebird database, and some of them still perform setup or teardown DDL.

Destructive admin flows do not operate directly on the shared source fixture.
Backup, restore, sweep, shutdown, and online proofs use a temporary restored
copy instead.

## Repository Guide

- `lib/`
  - production Firebird runtime, Serverpod integration, schema tooling, and
    admin code
- `test/`
  - unit and integration coverage for runtime, modules, admin, and
    observability slices
- `tool/`
  - runnable reports and one-off developer utilities
- `tools/`
  - fixture builders and support tooling
- `fixtures/native/`
  - curated native fixture blueprints and notes
- [TODO.md](TODO.md)
  - follow-up work that we intentionally did not force into the current phase

## Useful Entry Points

- Runnable proof example:
  - `/Users/worajedt/GitHub/FireDart/firebirdpod/example/serverpod_employee_proof.dart`
- Sample-database validation report tool:
  - `/Users/worajedt/GitHub/FireDart/firebirdpod/tool/firebird_serverpod_sample_database_report.dart`
- Benchmark baseline report tool:
  - `/Users/worajedt/GitHub/FireDart/firebirdpod/tool/firebird_benchmark_report.dart`
- Benchmark workflow and stored baselines:
  - `/Users/worajedt/GitHub/FireDart/firebirdpod/benchmarks/`
- Main project docs workspace:
  - `/Users/worajedt/GitHub/FireDart/docs/serverpod-firebird`
- Phase roadmap:
  - `/Users/worajedt/GitHub/FireDart/docs/serverpod-firebird/planning/phase-roadmap.md`

## Fixtures And References

- Raw Firebird fixtures live outside this repository under:
  - `/Users/worajedt/GitHub/FireDart/databases/firebird`
- Reference repositories:
  - `/Users/worajedt/GitHub/FireDart/firedart`
  - `/Users/worajedt/GitHub/FireDart/postgresql-dart`
  - `/Users/worajedt/GitHub/FireDart/serverpod`

These reference repositories are for study only. New implementation work
belongs in `firebirdpod`.
