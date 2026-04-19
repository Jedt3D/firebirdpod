# Benchmark Workflow

This directory holds the recorded benchmark baselines that the completed Phase
05 slices use for local observability and performance checks.

As of Slice `05H`, the stored baselines cover the shared `employee.fdb` proof
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

Automated CI gating is still a later decision. For now this directory supports
a repo-local comparison loop, not a machine-enforced release gate.
