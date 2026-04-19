import 'package:firebirdpod/firebirdpod.dart';
import 'package:test/test.dart';

void main() {
  group('Firebird benchmark catalog', () {
    test('resolves the default targets in stable order', () {
      final targets = firebirdResolveBenchmarkTargets(
        null,
        environment: const <String, String>{},
      );

      expect(targets.map((target) => target.name), <String>[
        'employee',
        'chinook',
        'northwind',
      ]);
      expect(targets.first.snapshotPath, 'benchmarks/baselines/employee.json');
      expect(
        targets[1].snapshotPath,
        'benchmarks/baselines/converted/chinook.json',
      );
    });

    test('preserves the default order for a selected subset', () {
      final targets = firebirdResolveBenchmarkTargets(const <String>{
        'northwind',
        'employee',
      }, environment: const <String, String>{});

      expect(targets.map((target) => target.name), <String>[
        'employee',
        'northwind',
      ]);
    });

    test('rejects unknown targets clearly', () {
      expect(
        () => firebirdResolveBenchmarkTargets(const <String>{
          'unknown',
        }, environment: const <String, String>{}),
        throwsA(
          isA<ArgumentError>().having(
            (error) => error.message,
            'message',
            contains('Unknown benchmark databases'),
          ),
        ),
      );
    });
  });

  group('Firebird benchmark gate', () {
    test('summarizes failed targets and scenarios', () {
      final baseline = FirebirdBenchmarkSnapshot.fromSuiteResult(
        _buildSuite(
          databaseLabel: 'employee',
          database: 'employee',
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

      final passingSuite = _buildSuite(
        databaseLabel: 'employee',
        database: 'employee',
        median: const Duration(milliseconds: 10),
        p90: const Duration(milliseconds: 12),
      );
      final failingSuite = _buildSuite(
        databaseLabel: 'chinook',
        database: 'chinook',
        median: const Duration(milliseconds: 13),
        p90: const Duration(milliseconds: 15),
      );

      final summary = FirebirdBenchmarkGateSummary(
        results: <FirebirdBenchmarkGateTargetResult>[
          FirebirdBenchmarkGateTargetResult(
            target: const FirebirdBenchmarkTarget(
              name: 'employee',
              databasePath: '/tmp/employee.fdb',
              snapshotPath: 'benchmarks/baselines/employee.json',
            ),
            snapshot: baseline,
            suite: passingSuite,
            comparison: baseline.compare(passingSuite),
          ),
          FirebirdBenchmarkGateTargetResult(
            target: const FirebirdBenchmarkTarget(
              name: 'chinook',
              databasePath: '/tmp/chinook.fdb',
              snapshotPath: 'benchmarks/baselines/converted/chinook.json',
            ),
            snapshot: baseline,
            suite: failingSuite,
            comparison: baseline.compare(failingSuite),
          ),
        ],
      );

      expect(summary.passed, isFalse);
      expect(summary.failedTargetCount, 1);
      expect(summary.failingScenarioCount, 1);
    });
  });
}

FirebirdBenchmarkSuiteResult _buildSuite({
  required String databaseLabel,
  required String database,
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
    scenario: FirebirdBenchmarkScenario(
      database: database,
      name: 'track_catalog_join',
      description: 'Catalog join from tracks through albums to artists.',
      query: 'select * from "tracks"',
      tags: const <String>['join'],
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
    databaseLabel: databaseLabel,
    options: const FirebirdBenchmarkOptions(
      warmupIterations: 1,
      measuredIterations: 3,
    ),
    startedAt: DateTime.parse('2026-04-19T00:00:00Z'),
    finishedAt: DateTime.parse('2026-04-19T00:00:01Z'),
    results: <FirebirdBenchmarkScenarioResult>[result],
  );
}
