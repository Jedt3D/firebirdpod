# Benchmark Tuning Playbook

This playbook is the practical follow-up to the local benchmark workflows. The
gate answers “did anything drift?” This page is about what to do next.

## Start With The Gate

Run the stored snapshots first:

```bash
dart run tool/firebird_benchmark_gate.dart --database=all
```

If the gate passes, stop unless you are doing a broader tuning review.

If you are investigating the CI-shaped benchmark profiles instead, start with
one of these commands:

```bash
dart run tool/firebird_benchmark_ci_gate.dart \
  --policy=benchmarks/policies/ci_smoke.json
```

```bash
dart run tool/firebird_benchmark_ci_gate.dart \
  --policy=benchmarks/policies/ci_candidate_multi_database.json
```

If the gate fails, move to the tuning report for the specific database you care
about:

```bash
dart run tool/firebird_benchmark_tuning_report.dart \
  --database=northwind \
  --warmup=1 \
  --iterations=5
```

That report does not try to “fix” the database for you. It tells you which kind
of drift happened, and which investigation should happen first.

## Read Failures In This Order

### 1. Result Shape First

If row count or column shape changed, treat that as a semantic change before
you treat it as a timing problem.

Typical causes:

- fixture drift
- changed predicates or joins
- changed projection or aliasing
- a snapshot that no longer belongs to the current scenario definition

Do not retune budgets yet. First make sure the query is still measuring the
same thing.

### 2. Plan Drift Second

If timing regressed and the explained plan also changed, start with the plan.

Typical checks:

- compare the stored plan and current plan line by line
- review index availability and predicate shape
- review Firebird index statistics or selectivity changes

Until the plan drift is understood, it is too early to blame cache noise or
machine jitter.

### 3. Timing-Only Regressions Third

If timing regressed but the plan stayed stable, the next questions are broader:

- did cache pressure change?
- did I/O activity change?
- was there concurrent load on the same instance?
- is the slowdown stable across reruns?

That is the point where the `MON$` monitoring helpers matter. Use them to
compare attachment, statement, I/O, and table counters around the same query
class instead of guessing.

## Refresh A Snapshot Last

Only refresh a snapshot after all of these are true:

1. The query still measures the same thing.
2. Any plan drift is understood and accepted.
3. The new timing is either an intentional change or the calibrated new normal.

If you refresh too early, you lose the evidence that would have told you what
actually changed.

## Useful Commands

Compare one database against its stored snapshot:

```bash
dart run tool/firebird_benchmark_report.dart \
  --database=employee \
  --warmup=1 \
  --iterations=5 \
  --compare-snapshot=benchmarks/baselines/employee.json
```

Show the scenario-by-scenario investigation report:

```bash
dart run tool/firebird_benchmark_tuning_report.dart --database=employee
```

Show the tuning report including currently passing scenarios:

```bash
dart run tool/firebird_benchmark_tuning_report.dart \
  --database=employee \
  --show-passing
```

## What This Playbook Does Not Claim

This is still a repo-local workflow. The project now has CI-shaped smoke and
candidate policies, but they are still calibration tools rather than hosted
release gates, and they do not pretend that every few milliseconds of drift is
meaningful on this machine.

The point is to make investigation repeatable and teachable, not to turn local
benchmark noise into false certainty.
