# Native Fixture Package

This folder defines the curated Firebird-native direction for the example databases used by `firebirdpod`.

## Purpose

- Keep raw converted fixtures for regression and compatibility testing
- Define Firebird-native variants for better schema design, Serverpod integration, and operational realism

## Layout

- `common/`
  - shared domains and design rules
- `car_database/`
  - native schema charter and blueprint
- `chinook/`
  - native schema charter and blueprint
- `northwind/`
  - native schema charter and blueprint
- `sakila_master/`
  - native schema charter and blueprint
- `../tools/build_native_fixtures.py`
  - executable builder that creates the curated databases from the raw Firebird fixtures

## Core Rules

- Use sequence-backed primary keys with `NEXT VALUE FOR`
- Avoid space-containing and mixed-case identifiers in curated schemas
- Standardize UTF-8 text handling
- Move repeated scalar definitions into domains
- Prefer Firebird views and PSQL procedures for reusable business workflows
- Add audit columns and deterministic trigger naming where useful

## Output Target

The current curated output set is:

- `/Users/worajedt/GitHub/FireDart/databases/firebird_native/car_database_native.fdb`
- `/Users/worajedt/GitHub/FireDart/databases/firebird_native/chinook_native.fdb`
- `/Users/worajedt/GitHub/FireDart/databases/firebird_native/northwind_native.fdb`
- `/Users/worajedt/GitHub/FireDart/databases/firebird_native/sakila_master_native.fdb`

## Build Command

Use the builder from the repository root or any working directory:

```bash
python3 /Users/worajedt/GitHub/FireDart/firebirdpod/tools/build_native_fixtures.py --overwrite
```

For single-database rebuilds:

```bash
python3 /Users/worajedt/GitHub/FireDart/firebirdpod/tools/build_native_fixtures.py --overwrite --database northwind
```

## Verify Command

Validate the current curated databases without rebuilding:

```bash
python3 /Users/worajedt/GitHub/FireDart/firebirdpod/tools/verify_native_fixtures.py --database all
```

Rebuild and verify one fixture:

```bash
python3 /Users/worajedt/GitHub/FireDart/firebirdpod/tools/verify_native_fixtures.py --database sakila_master --rebuild
```

The files in this directory now act as the design source for the generated databases, while the builder script is the executable path that turns those rules into real Firebird fixtures.
