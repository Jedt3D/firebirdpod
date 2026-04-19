# Benchmark Workflow

This directory holds the recorded benchmark baselines that the completed Phase
05 slices use for local observability and performance checks.

As of Slice `05L`, the stored baselines cover the shared `employee.fdb` proof
fixture plus the converted `chinook` and `northwind` sample databases.

## Current Baselines

- `baselines/employee.json`
- `baselines/converted/chinook.json`
- `baselines/converted/northwind.json`

## Compare A Fresh Run

Use one database at a time:

```bash
dart run tool/firebird_benchmark_report.dart \
  --database=employee \
  --warmup=1 \
  --iterations=5 \
  --compare-snapshot=benchmarks/baselines/employee.json
```

If the comparison fails, the tool exits non-zero.

## Run The Local Gate

Use the dedicated gate tool when you want one pass or fail answer across the
stored snapshots:

```bash
dart run tool/firebird_benchmark_gate.dart --database=all
```

The gate runs the selected databases against their committed snapshots and
exits non-zero if any target regresses.

## Run The CI Smoke Gate

Use the CI-oriented tool when you want the repo-owned policy profile and a
machine-readable summary:

```bash
dart run tool/firebird_benchmark_ci_gate.dart \
  --policy=benchmarks/policies/ci_smoke.json \
  --json
```

The current smoke policy is intentionally conservative. It gates the shared
`employee.fdb` fixture first, because that is the lowest-jitter target on this
machine and the cleanest starting point for future hosted automation.

There is also a broader candidate profile when you want one calibrated
scenario per supported benchmark database without forcing the full benchmark
catalog into the first CI-shaped pass:

```bash
dart run tool/firebird_benchmark_ci_gate.dart \
  --policy=benchmarks/policies/ci_candidate_multi_database.json \
  --json
```

That candidate policy currently covers:

- `employee`: `employee_directory_join`
- `chinook`: `track_catalog_join`
- `northwind`: `order_rollup`

## Investigate A Failing Gate

Use the tuning report when you want the next-step guidance instead of just a
pass or fail answer:

```bash
dart run tool/firebird_benchmark_tuning_report.dart \
  --database=chinook \
  --warmup=1 \
  --iterations=5
```

The tuning report groups failures by the kind of drift you hit: result-shape
changes, plan drift, or timing-only regressions. The fuller workflow is in
[TUNING.md](TUNING.md).

The current default comparison policy is tuned for this local Firebird harness:

- median regression budget: `1.20x`
- `p90` regression budget: `1.25x`
- median absolute drift floor: `5 ms`
- `p90` absolute drift floor: `10 ms`
- plan drift is reported by default and can be promoted to a failure with
  `--fail-on-plan-change`

The report tool also defaults to `5` measured iterations. That keeps `p90`
useful on this machine; with only `3` samples, `p90` is too close to the
single slowest measurement to be a stable gate.

## Refresh A Baseline

Only refresh after we understand and accept the behavior change:

```bash
dart run tool/firebird_benchmark_report.dart \
  --database=northwind \
  --warmup=1 \
  --iterations=5 \
  --write-snapshot=benchmarks/baselines/converted/northwind.json
```

## Tuning Loop

1. Capture the failing comparison report.
2. Check whether row shape or plan drift changed before treating it as a pure
   timing regression.
3. Review the explained plan and the relevant `MON$` monitoring view or
   monitoring-statistics snapshot for the query class.
4. Change one thing at a time: SQL shape, index strategy, statistics, or
   Firebird cache or config.
5. Re-run the same benchmark command and only refresh the snapshot after the
   new result is understood and intentionally accepted.

Hosted CI adoption is still a later decision. For now this directory supports
five repo-owned workflows: snapshot comparison, a one-command local gate, a
tuning report for investigation, a conservative CI smoke gate, and a broader
multi-database candidate gate. The candidate policy is useful evidence, but it
is still calibrated for this local harness rather than treated as a release
contract across every benchmark target.
