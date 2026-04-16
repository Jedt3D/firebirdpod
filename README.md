# firebirdpod

Primary implementation repository for the Firebird-native Serverpod backend.

## Current Repository Focus

- Firebird connectivity and runtime integration for Serverpod
- Firebird-native schema and migration support
- Fixture and native-schema work for repeatable testing

## Current Implementation Slices

- Dart package bootstrap for new backend-facing code in:
  - `lib/`
  - `test/`
- Firebird query compilation primitives that:
  - accept named or positional application parameters
  - rewrite them into Firebird-ready `?` placeholders
  - preserve binding order by actual placeholder appearance
  - protect quoted strings, quoted identifiers, and SQL comments
- Prepared SQL templates that:
  - parse once and bind many times
  - reject mixed named and positional placeholder styles
  - fail fast on parameter-style mismatch
- Runtime seam primitives that model:
  - endpoint attach
  - connection lifecycle
  - explicit transaction lifecycle
  - prepared statement lifecycle
  - native-client delegation behind a small interface
  - pool-safe connection reset before reuse
- First owned low-level direct `fbclient` transport that proves:
  - DPB-based attachment
  - default transaction lifecycle
  - explicit transaction begin, commit, rollback, and close
  - explicit transaction savepoints
  - explicit transaction isolation-level mapping
  - transaction-scoped runtime parameters through Firebird context variables
  - retained-transaction recreation for pooled reuse
  - prepared statement execution without the high-level `FbDb` transport
  - generated-id extraction from `INSERT ... RETURNING`
  - structured Firebird exception mapping
  - connection-level and statement-level timeout control
  - attachment cancellation entry points
  - scalar, date, blob, and richer Firebird 5 type decoding
  - dedicated retained and explicit write-contract coverage for insert, update, delete, and constraint failures
- Live prototype transport using the local `fbdb` package that proves:
  - real `fbclient` attachment
  - prepared statement execution through the seam
  - cursor and non-cursor example paths
- Curated native fixture tooling and verification in:
  - `tools/`
  - `tests/`

## Why The Query Compiler Lands First

Firebird prepared statements bind positional `?` placeholders. Serverpod-style
application SQL often starts from named parameters like `@tenantId` or indexed
parameters like `$1`. The compiler slice gives us one deterministic place to
normalize those inputs before we add actual `fbclient` statement execution.

The runtime slice now builds directly on top of that compiler contract, so the
future `fbclient` implementation only needs to provide:

- attach
- begin transaction
- prepare statement
- execute bound values
- commit or rollback when needed
- close statement and connection

The current `fbdb` prototype proved the seam shape first. The repository now
also contains an owned direct `fbclient` slice that executes through the
Firebird OO API inside `firebirdpod` itself. We still reuse the low-level Dart
wrapper types from the local `firedart` repository, but the statement lifecycle,
parameter encoding, and row decoding logic are now owned here.

The current direct slice already covers:

- text and binary blobs
- exact numeric generated IDs through `RETURNING`
- `INT128` and scaled 128-bit numerics
- `DECFLOAT(16)` and `DECFLOAT(34)`
- `TIME WITH TIME ZONE`
- `TIMESTAMP WITH TIME ZONE`
- structured timeout-aware error reporting
- a dedicated write-contract integration suite for ordinary DML behavior
- a monitoring-backed stress suite for repeated serial lifecycle validation
- repeated pooled-reset stress validation on one worker attachment
- an explicit-transaction capability suite for savepoints, isolation, and runtime parameters
- a pooled-reset suite for retained-state cleanup before reuse

## Timeout And Cancellation Policy

- Statement timeout is the production control baseline for the current direct
  adapter.
- `cancelCurrentOperation()` remains available as a low-level seam, but it is
  not the current guarantee for user-driven mid-flight cancellation.
- True async cancel for the direct adapter will require a later control-plane
  feature, not just another `Future`-returning Dart method.

## Transaction Use Policy

- The retained Firebird transaction is an internal auto-transaction path for
  standalone operations.
- The explicit transaction path is the future basis for the Serverpod
  `Transaction` contract.
- Anything that needs savepoints, row locks, custom isolation, runtime
  parameters, or multi-step atomic work must use the explicit path.
- Retained transaction state must not be allowed to become request-scoped
  application state when pooled connections are reused.
- `resetForReuse()` is the adapter primitive that future pooled integration
  should call before an attachment becomes idle for the next request.

## Current Direct-Test Policy

- Fast default verification:
  - `dart test`
- Full live direct verification:
  - `FIREBIRDPOD_RUN_FBCLIENT_DIRECT=1 dart test -j 1`

The live direct suite currently runs most reliably with `-j 1`, because the
integration files still share one Firebird database and some of them perform
DDL during setup or teardown.

The current evidence from the stress slice says serial live execution is clean;
isolated databases per file remain an optional future harness upgrade if we
decide we need safe parallel live execution.

The explicit-transaction capability tests also follow this serial shared-schema
pattern on this machine because fresh throwaway database creation is currently
rejected by the local Firebird environment.

The pooled-reset integration test follows the same policy and uses dedicated
tables inside the shared Firebird test database.

## Fixture Work

- Raw Firebird fixtures live outside this repository under:
  - `/Users/worajedt/GitHub/FireDart/databases/firebird`
- Native refactoring blueprints and curated fixture design live here under:
  - `fixtures/native/`
- The executable curated-fixture builder lives at:
  - `tools/build_native_fixtures.py`
- The executable curated-fixture verifier lives at:
  - `tools/verify_native_fixtures.py`
- The repo-level contract tests live at:
  - `tests/test_native_fixture_contract.py`

## Reference Repositories

- `/Users/worajedt/GitHub/FireDart/firedart`
- `/Users/worajedt/GitHub/FireDart/postgresql-dart`
- `/Users/worajedt/GitHub/FireDart/serverpod`

These are study references only. New implementation work belongs in this repository.
