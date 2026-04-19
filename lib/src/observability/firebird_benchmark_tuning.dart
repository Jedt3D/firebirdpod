import 'package:meta/meta.dart';

import 'firebird_benchmark_gate.dart';
import 'firebird_benchmark_snapshots.dart';

enum FirebirdBenchmarkTuningSeverity { info, warning, critical }

/// High-level investigation guidance for one compared benchmark run.
@immutable
class FirebirdBenchmarkTuningReport {
  const FirebirdBenchmarkTuningReport({
    required this.databaseLabel,
    required this.databaseMatches,
    required this.sharedNextSteps,
    required this.scenarios,
  });

  final String databaseLabel;
  final bool databaseMatches;
  final List<String> sharedNextSteps;
  final List<FirebirdBenchmarkTuningScenarioAdvice> scenarios;

  bool get requiresAction =>
      !databaseMatches || scenarios.any((scenario) => scenario.requiresAction);

  List<FirebirdBenchmarkTuningScenarioAdvice> get interestingScenarios =>
      scenarios
          .where(
            (scenario) => scenario.requiresAction || scenario.verdict != 'pass',
          )
          .toList(growable: false);
}

/// Investigation guidance for one benchmark scenario.
@immutable
class FirebirdBenchmarkTuningScenarioAdvice {
  const FirebirdBenchmarkTuningScenarioAdvice({
    required this.name,
    required this.verdict,
    required this.severity,
    required this.headline,
    required this.summary,
    required this.nextSteps,
    required this.requiresAction,
    required this.planMatches,
    required this.rowCountMatches,
    required this.columnsMatch,
    required this.medianBaseline,
    required this.medianCurrent,
    required this.medianDelta,
    required this.medianRatio,
    required this.p90Baseline,
    required this.p90Current,
    required this.p90Delta,
    required this.p90Ratio,
  });

  final String name;
  final String verdict;
  final FirebirdBenchmarkTuningSeverity severity;
  final String headline;
  final String summary;
  final List<String> nextSteps;
  final bool requiresAction;
  final bool planMatches;
  final bool rowCountMatches;
  final bool columnsMatch;
  final Duration? medianBaseline;
  final Duration? medianCurrent;
  final Duration? medianDelta;
  final double? medianRatio;
  final Duration? p90Baseline;
  final Duration? p90Current;
  final Duration? p90Delta;
  final double? p90Ratio;
}

/// Turns benchmark comparison results into a concrete tuning workflow.
class FirebirdBenchmarkTuningAdvisor {
  const FirebirdBenchmarkTuningAdvisor();

  FirebirdBenchmarkTuningReport analyzeGateResult(
    FirebirdBenchmarkGateTargetResult result,
  ) {
    return analyzeComparison(
      result.comparison,
      databaseLabel: result.target.name,
    );
  }

  FirebirdBenchmarkTuningReport analyzeComparison(
    FirebirdBenchmarkComparison comparison, {
    String? databaseLabel,
  }) {
    final scenarios = comparison.scenarios
        .map(_analyzeScenario)
        .toList(growable: false);

    return FirebirdBenchmarkTuningReport(
      databaseLabel: databaseLabel ?? comparison.suite.databaseLabel,
      databaseMatches: comparison.databaseMatches,
      sharedNextSteps: _sharedNextSteps(comparison, scenarios),
      scenarios: scenarios,
    );
  }

  FirebirdBenchmarkTuningScenarioAdvice _analyzeScenario(
    FirebirdBenchmarkScenarioComparison scenario,
  ) {
    if (scenario.missingInCurrent || scenario.unexpectedInCurrent) {
      return FirebirdBenchmarkTuningScenarioAdvice(
        name: scenario.name,
        verdict: scenario.verdict,
        severity: FirebirdBenchmarkTuningSeverity.critical,
        headline: 'Scenario catalog drift',
        summary:
            'The live run and the stored snapshot do not agree on whether this scenario exists. '
            'Treat that as a workflow mismatch before reading any timing numbers.',
        nextSteps: const <String>[
          'Confirm that the selected database and snapshot file belong to the same benchmark target.',
          'Check whether the scenario list changed intentionally in code.',
          'Only refresh the snapshot after the catalog change is understood and accepted.',
        ],
        requiresAction: true,
        planMatches: scenario.planMatches,
        rowCountMatches: scenario.rowCountMatches,
        columnsMatch: scenario.columnsMatch,
        medianBaseline: scenario.snapshotScenario?.median,
        medianCurrent: scenario.currentResult?.statistics.median,
        medianDelta: scenario.medianDelta,
        medianRatio: scenario.medianRatio,
        p90Baseline: scenario.snapshotScenario?.p90,
        p90Current: scenario.currentResult?.statistics.p90,
        p90Delta: scenario.p90Delta,
        p90Ratio: scenario.p90Ratio,
      );
    }

    if (!scenario.rowCountMatches) {
      return FirebirdBenchmarkTuningScenarioAdvice(
        name: scenario.name,
        verdict: scenario.verdict,
        severity: FirebirdBenchmarkTuningSeverity.critical,
        headline: 'Row-count drift changes query meaning',
        summary:
            'The scenario now returns a different number of rows than the stored baseline. '
            'That usually means data shape, predicates, or join behavior changed, so fix meaning first and timing second.',
        nextSteps: const <String>[
          'Verify that the benchmark fixture and the scenario parameters still match the stored baseline.',
          'Inspect query predicates, joins, and limiting clauses before treating this as a performance regression.',
          'Refresh the snapshot only after the changed result shape is intentional.',
        ],
        requiresAction: true,
        planMatches: scenario.planMatches,
        rowCountMatches: scenario.rowCountMatches,
        columnsMatch: scenario.columnsMatch,
        medianBaseline: scenario.snapshotScenario?.median,
        medianCurrent: scenario.currentResult?.statistics.median,
        medianDelta: scenario.medianDelta,
        medianRatio: scenario.medianRatio,
        p90Baseline: scenario.snapshotScenario?.p90,
        p90Current: scenario.currentResult?.statistics.p90,
        p90Delta: scenario.p90Delta,
        p90Ratio: scenario.p90Ratio,
      );
    }

    if (!scenario.columnsMatch) {
      return FirebirdBenchmarkTuningScenarioAdvice(
        name: scenario.name,
        verdict: scenario.verdict,
        severity: FirebirdBenchmarkTuningSeverity.critical,
        headline: 'Column shape drift changes the contract',
        summary:
            'The live run returns a different column set than the stored baseline. '
            'Treat that as a query-shape change, not just a timing change.',
        nextSteps: const <String>[
          'Inspect the SELECT list, aliases, and serializer or materialization path.',
          'Confirm that the snapshot belongs to the same logical scenario version.',
          'Only refresh the snapshot after the column contract change is deliberate.',
        ],
        requiresAction: true,
        planMatches: scenario.planMatches,
        rowCountMatches: scenario.rowCountMatches,
        columnsMatch: scenario.columnsMatch,
        medianBaseline: scenario.snapshotScenario?.median,
        medianCurrent: scenario.currentResult?.statistics.median,
        medianDelta: scenario.medianDelta,
        medianRatio: scenario.medianRatio,
        p90Baseline: scenario.snapshotScenario?.p90,
        p90Current: scenario.currentResult?.statistics.p90,
        p90Delta: scenario.p90Delta,
        p90Ratio: scenario.p90Ratio,
      );
    }

    if (scenario.medianRegressed || scenario.p90Regressed) {
      if (!scenario.planMatches) {
        return FirebirdBenchmarkTuningScenarioAdvice(
          name: scenario.name,
          verdict: scenario.verdict,
          severity: FirebirdBenchmarkTuningSeverity.critical,
          headline: 'Timing regressed and the plan changed',
          summary:
              'The scenario is slower than budget and the explained plan drifted. '
              'Start with plan shape and index selection before chasing cache or runtime noise.',
          nextSteps: const <String>[
            'Compare the current explained plan with the stored snapshot and identify the first structural difference.',
            'Review index availability, selectivity statistics, and predicate shape before retuning the benchmark budget.',
            'After the plan is understood, rerun the scenario and then decide whether the snapshot should move.',
          ],
          requiresAction: true,
          planMatches: scenario.planMatches,
          rowCountMatches: scenario.rowCountMatches,
          columnsMatch: scenario.columnsMatch,
          medianBaseline: scenario.snapshotScenario?.median,
          medianCurrent: scenario.currentResult?.statistics.median,
          medianDelta: scenario.medianDelta,
          medianRatio: scenario.medianRatio,
          p90Baseline: scenario.snapshotScenario?.p90,
          p90Current: scenario.currentResult?.statistics.p90,
          p90Delta: scenario.p90Delta,
          p90Ratio: scenario.p90Ratio,
        );
      }

      return FirebirdBenchmarkTuningScenarioAdvice(
        name: scenario.name,
        verdict: scenario.verdict,
        severity: FirebirdBenchmarkTuningSeverity.warning,
        headline: 'Timing regressed without plan drift',
        summary:
            'The query stayed on the same plan but still ran slower than budget. '
            'That pushes the investigation toward cache state, I/O pressure, concurrency, or broader runtime conditions.',
        nextSteps: const <String>[
          'Capture MON\$ monitoring snapshots for the same query class and compare I/O, record, and table counters.',
          'Check page-cache pressure, temp-space activity, and concurrent work on the same Firebird instance.',
          'Rerun the same benchmark command to confirm that the slowdown is persistent before accepting a new baseline.',
        ],
        requiresAction: true,
        planMatches: scenario.planMatches,
        rowCountMatches: scenario.rowCountMatches,
        columnsMatch: scenario.columnsMatch,
        medianBaseline: scenario.snapshotScenario?.median,
        medianCurrent: scenario.currentResult?.statistics.median,
        medianDelta: scenario.medianDelta,
        medianRatio: scenario.medianRatio,
        p90Baseline: scenario.snapshotScenario?.p90,
        p90Current: scenario.currentResult?.statistics.p90,
        p90Delta: scenario.p90Delta,
        p90Ratio: scenario.p90Ratio,
      );
    }

    if (!scenario.planMatches) {
      return FirebirdBenchmarkTuningScenarioAdvice(
        name: scenario.name,
        verdict: scenario.verdict,
        severity: FirebirdBenchmarkTuningSeverity.info,
        headline: 'Plan drift without a budget failure',
        summary:
            'The plan changed but the scenario still stayed inside the current timing budget. '
            'That is worth reviewing, but it is not an urgent regression by itself.',
        nextSteps: const <String>[
          'Compare the stored and current plans and decide whether the drift is expected.',
          'If plan stability matters more than timing wiggle for this query class, consider enabling fail-on-plan-change for the snapshot.',
          'Refresh the snapshot only after the new plan is intentionally accepted.',
        ],
        requiresAction: false,
        planMatches: scenario.planMatches,
        rowCountMatches: scenario.rowCountMatches,
        columnsMatch: scenario.columnsMatch,
        medianBaseline: scenario.snapshotScenario?.median,
        medianCurrent: scenario.currentResult?.statistics.median,
        medianDelta: scenario.medianDelta,
        medianRatio: scenario.medianRatio,
        p90Baseline: scenario.snapshotScenario?.p90,
        p90Current: scenario.currentResult?.statistics.p90,
        p90Delta: scenario.p90Delta,
        p90Ratio: scenario.p90Ratio,
      );
    }

    return FirebirdBenchmarkTuningScenarioAdvice(
      name: scenario.name,
      verdict: scenario.verdict,
      severity: FirebirdBenchmarkTuningSeverity.info,
      headline: 'Within the current budget',
      summary:
          'This scenario still matches the stored result shape and timing budget. '
          'No extra investigation is needed unless you are doing a broader tuning review.',
      nextSteps: const <String>[
        'Keep the current snapshot unless you intentionally changed the scenario or query shape.',
      ],
      requiresAction: false,
      planMatches: scenario.planMatches,
      rowCountMatches: scenario.rowCountMatches,
      columnsMatch: scenario.columnsMatch,
      medianBaseline: scenario.snapshotScenario?.median,
      medianCurrent: scenario.currentResult?.statistics.median,
      medianDelta: scenario.medianDelta,
      medianRatio: scenario.medianRatio,
      p90Baseline: scenario.snapshotScenario?.p90,
      p90Current: scenario.currentResult?.statistics.p90,
      p90Delta: scenario.p90Delta,
      p90Ratio: scenario.p90Ratio,
    );
  }

  List<String> _sharedNextSteps(
    FirebirdBenchmarkComparison comparison,
    List<FirebirdBenchmarkTuningScenarioAdvice> scenarios,
  ) {
    final steps = <String>[];

    if (!comparison.databaseMatches) {
      steps.add(
        'Confirm that the selected benchmark database and the snapshot file describe the same target before interpreting any drift.',
      );
    }

    final hasShapeDrift = scenarios.any(
      (scenario) => !scenario.rowCountMatches || !scenario.columnsMatch,
    );
    if (hasShapeDrift) {
      steps.add(
        'Treat row-count or column-shape drift as a semantic change first; do not retune timing budgets until the result shape is understood.',
      );
    }

    final hasPlanRegression = scenarios.any(
      (scenario) => scenario.requiresAction && !scenario.planMatches,
    );
    if (hasPlanRegression) {
      steps.add(
        'When timing regresses with plan drift, compare the explained plans first and review index or statistics changes before looking at cache noise.',
      );
    }

    final hasTimingOnlyRegression = scenarios.any(
      (scenario) =>
          scenario.requiresAction &&
          scenario.planMatches &&
          scenario.rowCountMatches &&
          scenario.columnsMatch,
    );
    if (hasTimingOnlyRegression) {
      steps.add(
        'When timing regresses without plan drift, use MON\$ monitoring snapshots and instance-level cache or load checks to separate real regression from harness jitter.',
      );
    }

    if (steps.isEmpty) {
      steps.add(
        'The current run stayed inside the stored budgets. Keep the snapshots as they are unless you intentionally changed the benchmark scenarios.',
      );
    }

    return List<String>.unmodifiable(steps);
  }
}
