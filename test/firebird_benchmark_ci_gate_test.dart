import 'package:firebirdpod/firebirdpod.dart';
import 'package:test/test.dart';

void main() {
  group('Firebird benchmark gate policy', () {
    test(
      'parses a CI smoke policy and resolves its target budget override',
      () {
        final policy = FirebirdBenchmarkGatePolicy.fromJson(<String, Object?>{
          'formatVersion': 1,
          'name': 'ci-smoke',
          'description': 'smoke',
          'warmupIterations': 1,
          'measuredIterations': 3,
          'targets': <Object?>[
            <String, Object?>{
              'name': 'employee',
              'budgetOverride': <String, Object?>{
                'maxMedianRegressionRatio': 1.20,
                'maxP90RegressionRatio': 1.25,
                'minMedianRegressionDeltaMicroseconds': 3000,
                'minP90RegressionDeltaMicroseconds': 5000,
              },
            },
          ],
        });

        final targets = policy.resolveTargets(
          environment: const <String, String>{},
        );

        expect(policy.name, 'ci-smoke');
        expect(policy.options.measuredIterations, 3);
        expect(targets, hasLength(1));
        expect(targets.single.name, 'employee');
        expect(targets.single.budgetOverride, isNotNull);
        expect(
          targets.single.budgetOverride!.minMedianRegressionDelta,
          const Duration(milliseconds: 3),
        );
      },
    );

    test('keeps calibrated scenario selections on resolved targets', () {
      final policy = FirebirdBenchmarkGatePolicy.fromJson(<String, Object?>{
        'formatVersion': 1,
        'name': 'ci-candidate',
        'description': 'candidate',
        'warmupIterations': 1,
        'measuredIterations': 3,
        'targets': <Object?>[
          <String, Object?>{
            'name': 'chinook',
            'scenarioNames': <Object?>['track_catalog_join'],
          },
        ],
      });

      final targets = policy.resolveTargets(
        environment: const <String, String>{},
      );

      expect(targets.single.name, 'chinook');
      expect(targets.single.scenarioNames, <String>['track_catalog_join']);
    });

    test('comparison json exposes verdicts and pass state', () {
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
      final slower = _buildSuite(
        median: const Duration(milliseconds: 15),
        p90: const Duration(milliseconds: 18),
      );
      final comparison = baseline.compare(slower);
      final json = comparison.toJson();

      expect(json['passed'], false);
      expect(json['databaseMatches'], true);
      expect((json['scenarios'] as List<Object?>).single, isA<Map>());
      expect(
        ((json['scenarios'] as List<Object?>).single
            as Map<Object?, Object?>)['verdict'],
        'regressed',
      );
    });

    test('snapshot and scenario helpers filter selected scenario names', () {
      final snapshot = FirebirdBenchmarkSnapshot.fromSuiteResult(
        FirebirdBenchmarkSuiteResult(
          databaseLabel: 'chinook',
          options: const FirebirdBenchmarkOptions(
            warmupIterations: 1,
            measuredIterations: 3,
          ),
          startedAt: DateTime.parse('2026-04-19T00:00:00Z'),
          finishedAt: DateTime.parse('2026-04-19T00:00:01Z'),
          results: <FirebirdBenchmarkScenarioResult>[
            _scenarioResult(
              name: 'track_catalog_join',
              query: 'select * from "tracks"',
            ),
            _scenarioResult(
              name: 'invoice_customer_rollup',
              query: 'select * from "invoices"',
            ),
          ],
        ),
      );

      final filteredSnapshot = snapshot.selectScenarios(const <String>[
        'track_catalog_join',
      ]);
      final filteredScenarios = firebirdResolveBenchmarkScenariosForDatabase(
        'chinook',
        selectedScenarioNames: const <String>['track_catalog_join'],
      );

      expect(
        filteredSnapshot.scenarios.map((scenario) => scenario.name),
        <String>['track_catalog_join'],
      );
      expect(filteredScenarios.map((scenario) => scenario.name), <String>[
        'track_catalog_join',
      ]);
    });
  });
}

FirebirdBenchmarkSuiteResult _buildSuite({
  required Duration median,
  required Duration p90,
}) {
  final statistics = FirebirdBenchmarkStatistics.fromSamples([
    const Duration(milliseconds: 8),
    median,
    p90,
  ]);
  final result = FirebirdBenchmarkScenarioResult(
    scenario: const FirebirdBenchmarkScenario(
      database: 'employee',
      name: 'department_salary_rollup',
      description: 'Rollup over employee and department tables.',
      query: 'select * from "department"',
      tags: <String>['aggregate'],
    ),
    options: const FirebirdBenchmarkOptions(
      warmupIterations: 1,
      measuredIterations: 3,
    ),
    statistics: statistics,
    plan: const FirebirdQueryPlan(
      sourceSql: 'select * from "department"',
      normalizedSql: 'select * from "department"',
      parameterStyle: FirebirdParameterStyle.none,
      parameterCount: 0,
      detailed: true,
      plan: 'PLAN (DEPARTMENT NATURAL)',
    ),
    rowCount: 14,
    affectedRows: 0,
    columns: const <String>['dept_no', 'department'],
  );

  return FirebirdBenchmarkSuiteResult(
    databaseLabel: 'employee',
    options: const FirebirdBenchmarkOptions(
      warmupIterations: 1,
      measuredIterations: 3,
    ),
    startedAt: DateTime.parse('2026-04-19T00:00:00Z'),
    finishedAt: DateTime.parse('2026-04-19T00:00:01Z'),
    results: <FirebirdBenchmarkScenarioResult>[result],
  );
}

FirebirdBenchmarkScenarioResult _scenarioResult({
  required String name,
  required String query,
}) {
  return FirebirdBenchmarkScenarioResult(
    scenario: FirebirdBenchmarkScenario(
      database: 'chinook',
      name: name,
      description: name,
      query: query,
    ),
    options: const FirebirdBenchmarkOptions(
      warmupIterations: 1,
      measuredIterations: 3,
    ),
    statistics: FirebirdBenchmarkStatistics.fromSamples(const <Duration>[
      Duration(milliseconds: 8),
      Duration(milliseconds: 10),
      Duration(milliseconds: 12),
    ]),
    plan: FirebirdQueryPlan(
      sourceSql: query,
      normalizedSql: query,
      parameterStyle: FirebirdParameterStyle.none,
      parameterCount: 0,
      detailed: true,
      plan: 'PLAN (TRACKS NATURAL)',
    ),
    rowCount: 10,
    affectedRows: 0,
    columns: const <String>['id'],
  );
}
