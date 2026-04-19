import 'package:firebirdpod/firebirdpod.dart';
import 'package:test/test.dart';

void main() {
  group('Firebird benchmark tuning advisor', () {
    const advisor = FirebirdBenchmarkTuningAdvisor();

    test('treats row-count drift as a semantic issue before timing', () {
      final baseline = FirebirdBenchmarkSnapshot.fromSuiteResult(
        _buildSuite(rowCount: 50),
      );
      final changedRows = _buildSuite(rowCount: 60);

      final report = advisor.analyzeComparison(baseline.compare(changedRows));
      final scenario = report.scenarios.single;

      expect(report.requiresAction, isTrue);
      expect(scenario.headline, 'Row-count drift changes query meaning');
      expect(scenario.requiresAction, isTrue);
      expect(scenario.severity, FirebirdBenchmarkTuningSeverity.critical);
    });

    test('treats plan-changing regressions as plan-first work', () {
      final baseline = FirebirdBenchmarkSnapshot.fromSuiteResult(
        _buildSuite(
          median: const Duration(milliseconds: 10),
          p90: const Duration(milliseconds: 12),
          plan: 'PLAN (TRACKS INDEX (RDB\$PRIMARY10))',
        ),
        defaultBudget: const FirebirdBenchmarkBudget(
          maxMedianRegressionRatio: 1.10,
          maxP90RegressionRatio: 1.10,
          minMedianRegressionDelta: Duration.zero,
          minP90RegressionDelta: Duration.zero,
        ),
      );
      final slowerChangedPlan = _buildSuite(
        median: const Duration(milliseconds: 15),
        p90: const Duration(milliseconds: 18),
        plan: 'PLAN (TRACKS NATURAL)',
      );

      final report = advisor.analyzeComparison(
        baseline.compare(slowerChangedPlan),
      );
      final scenario = report.scenarios.single;

      expect(scenario.headline, 'Timing regressed and the plan changed');
      expect(
        scenario.nextSteps.first,
        contains('Compare the current explained plan'),
      );
      expect(scenario.severity, FirebirdBenchmarkTuningSeverity.critical);
    });

    test(
      'pushes plan-stable regressions toward monitoring and cache checks',
      () {
        final baseline = FirebirdBenchmarkSnapshot.fromSuiteResult(
          _buildSuite(
            median: const Duration(milliseconds: 10),
            p90: const Duration(milliseconds: 12),
          ),
          defaultBudget: const FirebirdBenchmarkBudget(
            maxMedianRegressionRatio: 1.10,
            maxP90RegressionRatio: 1.10,
            minMedianRegressionDelta: Duration.zero,
            minP90RegressionDelta: Duration.zero,
          ),
        );
        final slowerSamePlan = _buildSuite(
          median: const Duration(milliseconds: 14),
          p90: const Duration(milliseconds: 16),
        );

        final report = advisor.analyzeComparison(
          baseline.compare(slowerSamePlan),
        );
        final scenario = report.scenarios.single;

        expect(scenario.headline, 'Timing regressed without plan drift');
        expect(
          scenario.nextSteps.first,
          contains('Capture MON\$ monitoring snapshots'),
        );
        expect(scenario.severity, FirebirdBenchmarkTuningSeverity.warning);
      },
    );

    test('treats plan drift without regression as informational', () {
      final baseline = FirebirdBenchmarkSnapshot.fromSuiteResult(
        _buildSuite(plan: 'PLAN (TRACKS INDEX (RDB\$PRIMARY10))'),
      );
      final changedPlan = _buildSuite(plan: 'PLAN (TRACKS NATURAL)');

      final report = advisor.analyzeComparison(baseline.compare(changedPlan));
      final scenario = report.scenarios.single;

      expect(report.requiresAction, isFalse);
      expect(scenario.headline, 'Plan drift without a budget failure');
      expect(scenario.requiresAction, isFalse);
      expect(scenario.severity, FirebirdBenchmarkTuningSeverity.info);
      expect(report.interestingScenarios, hasLength(1));
    });
  });
}

FirebirdBenchmarkSuiteResult _buildSuite({
  Duration median = const Duration(milliseconds: 10),
  Duration p90 = const Duration(milliseconds: 12),
  String plan = 'Select Expression -> Table "tracks" Full Scan',
  int rowCount = 50,
}) {
  final statistics = FirebirdBenchmarkStatistics.fromSamples([
    const Duration(milliseconds: 8),
    median,
    p90,
  ]);
  final result = FirebirdBenchmarkScenarioResult(
    scenario: const FirebirdBenchmarkScenario(
      database: 'chinook',
      name: 'track_catalog_join',
      description: 'Catalog join from tracks through albums to artists.',
      query: 'select * from "tracks"',
      tags: <String>['join'],
    ),
    options: const FirebirdBenchmarkOptions(
      warmupIterations: 1,
      measuredIterations: 3,
    ),
    statistics: statistics,
    plan: FirebirdQueryPlan(
      sourceSql: 'select * from "tracks"',
      normalizedSql: 'select * from "tracks"',
      parameterStyle: FirebirdParameterStyle.none,
      parameterCount: 0,
      detailed: true,
      plan: plan,
    ),
    rowCount: rowCount,
    affectedRows: 0,
    columns: const <String>['TrackId', 'Name'],
  );

  return FirebirdBenchmarkSuiteResult(
    databaseLabel: 'chinook',
    options: const FirebirdBenchmarkOptions(
      warmupIterations: 1,
      measuredIterations: 3,
    ),
    startedAt: DateTime.parse('2026-04-19T00:00:00Z'),
    finishedAt: DateTime.parse('2026-04-19T00:00:01Z'),
    results: <FirebirdBenchmarkScenarioResult>[result],
  );
}
