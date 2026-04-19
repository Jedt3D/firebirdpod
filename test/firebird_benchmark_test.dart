import 'package:firebirdpod/firebirdpod.dart';
import 'package:test/test.dart';

void main() {
  group('Firebird benchmarks', () {
    test('computes coherent duration statistics from measured samples', () {
      final statistics = FirebirdBenchmarkStatistics.fromSamples([
        const Duration(milliseconds: 9),
        const Duration(milliseconds: 3),
        const Duration(milliseconds: 6),
        const Duration(milliseconds: 12),
      ]);

      expect(statistics.samples, hasLength(4));
      expect(statistics.minimum, const Duration(milliseconds: 3));
      expect(statistics.maximum, const Duration(milliseconds: 12));
      expect(
        statistics.mean,
        const Duration(milliseconds: 7, microseconds: 500),
      );
      expect(
        statistics.median,
        const Duration(milliseconds: 7, microseconds: 500),
      );
      expect(statistics.p90, const Duration(milliseconds: 12));
      expect(statistics.total, const Duration(milliseconds: 30));
    });

    test('builds named parameters for parameterized scenarios', () {
      const scenario = FirebirdBenchmarkScenario(
        database: 'chinook',
        name: 'example',
        description: 'example',
        query: 'select * from "tracks" where "TrackId" = @trackId',
        namedParameters: <String, Object?>{'trackId': 10},
      );

      final parameters = scenario.parameters;

      expect(parameters, isNotNull);
      expect(
        (parameters! as FirebirdNamedParameters).parameters,
        <String, Object?>{'trackId': 10},
      );
    });

    test('round-trips benchmark snapshots and compares within budget', () {
      final suite = _buildSuite(
        median: const Duration(milliseconds: 10),
        p90: const Duration(milliseconds: 12),
      );
      final snapshot = FirebirdBenchmarkSnapshot.fromSuiteResult(
        suite,
        defaultBudget: const FirebirdBenchmarkBudget(
          maxMedianRegressionRatio: 1.10,
          maxP90RegressionRatio: 1.10,
          minMedianRegressionDelta: Duration.zero,
          minP90RegressionDelta: Duration.zero,
        ),
      );
      final roundTrip = FirebirdBenchmarkSnapshot.fromJson(snapshot.toJson());
      final comparison = roundTrip.compare(suite);

      expect(roundTrip.databaseLabel, 'chinook');
      expect(roundTrip.scenarios, hasLength(1));
      expect(comparison.passed, isTrue);
      expect(comparison.scenarios.single.verdict, 'pass');
      expect(comparison.scenarios.single.medianRatio, 1.0);
      expect(comparison.scenarios.single.p90Ratio, 1.0);
    });

    test('flags benchmark regressions beyond policy budgets', () {
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
      final slowerSuite = _buildSuite(
        median: const Duration(milliseconds: 13),
        p90: const Duration(milliseconds: 15),
      );
      final comparison = baseline.compare(slowerSuite);

      expect(comparison.passed, isFalse);
      expect(comparison.scenarios.single.medianRegressed, isTrue);
      expect(comparison.scenarios.single.p90Regressed, isTrue);
      expect(comparison.scenarios.single.verdict, 'regressed');
    });

    test('allows small absolute drift on fast scenarios', () {
      final baseline = FirebirdBenchmarkSnapshot.fromSuiteResult(
        _buildSuite(
          median: const Duration(milliseconds: 6),
          p90: const Duration(milliseconds: 7),
        ),
        defaultBudget: const FirebirdBenchmarkBudget(
          maxMedianRegressionRatio: 1.20,
          maxP90RegressionRatio: 1.25,
          minMedianRegressionDelta: Duration(milliseconds: 5),
          minP90RegressionDelta: Duration(milliseconds: 10),
        ),
      );
      final slightlySlowerSuite = _buildSuite(
        median: const Duration(milliseconds: 8),
        p90: const Duration(milliseconds: 11),
      );
      final comparison = baseline.compare(slightlySlowerSuite);

      expect(comparison.passed, isTrue);
      expect(comparison.scenarios.single.medianRatio, closeTo(1.143, 0.001));
      expect(comparison.scenarios.single.p90Ratio, closeTo(1.375, 0.001));
      expect(
        comparison.scenarios.single.medianDelta,
        const Duration(milliseconds: 1),
      );
      expect(
        comparison.scenarios.single.p90Delta,
        const Duration(milliseconds: 3),
      );
      expect(comparison.scenarios.single.verdict, 'pass');
    });

    test('can fail on plan drift when the budget requests it', () {
      final baseline = FirebirdBenchmarkSnapshot.fromSuiteResult(
        _buildSuite(
          median: const Duration(milliseconds: 10),
          p90: const Duration(milliseconds: 12),
          plan: 'PLAN (TRACKS INDEX (RDB\$PRIMARY10))',
        ),
        defaultBudget: const FirebirdBenchmarkBudget(failOnPlanChange: true),
      );
      final changedPlanSuite = _buildSuite(
        median: const Duration(milliseconds: 10),
        p90: const Duration(milliseconds: 12),
        plan: 'PLAN (TRACKS NATURAL)',
      );
      final comparison = baseline.compare(changedPlanSuite);

      expect(comparison.passed, isFalse);
      expect(comparison.scenarios.single.planMatches, isFalse);
      expect(comparison.scenarios.single.verdict, 'plan-changed');
    });
  });
}

FirebirdBenchmarkSuiteResult _buildSuite({
  required Duration median,
  required Duration p90,
  String plan = 'Select Expression -> Table "tracks" Full Scan',
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
    rowCount: 50,
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
