import 'package:firebirdpod/firebirdpod.dart';
import 'package:test/test.dart';

import 'firebird_test_support.dart';

void main() {
  group('Firebird benchmarks', () {
    test(
      'captures coherent benchmark baselines for employee query classes',
      () async {
        if (!shouldRunDirectIntegrationTests()) {
          markTestSkipped(
            'set FIREBIRDPOD_RUN_FBCLIENT_DIRECT=1 to run live Firebird benchmark tests',
          );
          return;
        }

        final connection = await buildDirectEndpoint(
          database: firebirdTestDatabasePath(),
        ).connect();
        addTearDown(connection.close);

        final suite = await connection.benchmarks.runScenarios(
          firebirdEmployeeBenchmarkScenarios,
          options: const FirebirdBenchmarkOptions(
            warmupIterations: 1,
            measuredIterations: 2,
          ),
          databaseLabel: 'employee',
        );

        _expectCoherentSuite(suite, database: 'employee');
      },
    );

    test(
      'captures coherent benchmark baselines for chinook query classes',
      () async {
        if (!shouldRunDirectIntegrationTests()) {
          markTestSkipped(
            'set FIREBIRDPOD_RUN_FBCLIENT_DIRECT=1 to run live Firebird benchmark tests',
          );
          return;
        }

        final connection = await buildDirectEndpoint(
          database: firebirdChinookDatabasePath(),
        ).connect();
        addTearDown(connection.close);

        final suite = await connection.benchmarks.runScenarios(
          firebirdChinookBenchmarkScenarios,
          options: const FirebirdBenchmarkOptions(
            warmupIterations: 1,
            measuredIterations: 2,
          ),
          databaseLabel: 'chinook',
        );

        _expectCoherentSuite(suite, database: 'chinook');
      },
    );

    test(
      'captures coherent benchmark baselines for northwind query classes',
      () async {
        if (!shouldRunDirectIntegrationTests()) {
          markTestSkipped(
            'set FIREBIRDPOD_RUN_FBCLIENT_DIRECT=1 to run live Firebird benchmark tests',
          );
          return;
        }

        final connection = await buildDirectEndpoint(
          database: firebirdNorthwindDatabasePath(),
        ).connect();
        addTearDown(connection.close);

        final suite = await connection.benchmarks.runScenarios(
          firebirdNorthwindBenchmarkScenarios,
          options: const FirebirdBenchmarkOptions(
            warmupIterations: 1,
            measuredIterations: 2,
          ),
          databaseLabel: 'northwind',
        );

        _expectCoherentSuite(suite, database: 'northwind');
      },
    );
  });
}

void _expectCoherentSuite(
  FirebirdBenchmarkSuiteResult suite, {
  required String database,
}) {
  expect(suite.databaseLabel, database);
  expect(suite.results, isNotEmpty);
  expect(suite.elapsed, greaterThanOrEqualTo(Duration.zero));

  for (final result in suite.results) {
    expect(result.scenario.database, database);
    expect(result.statistics.samples, hasLength(2));
    expect(result.statistics.minimum, greaterThan(Duration.zero));
    expect(
      result.statistics.maximum,
      greaterThanOrEqualTo(result.statistics.minimum),
    );
    expect(
      result.statistics.mean,
      greaterThanOrEqualTo(result.statistics.minimum),
    );
    expect(
      result.statistics.median,
      greaterThanOrEqualTo(result.statistics.minimum),
    );
    expect(
      result.statistics.p90,
      greaterThanOrEqualTo(result.statistics.median),
    );
    expect(
      result.statistics.total,
      greaterThanOrEqualTo(result.statistics.maximum),
    );
    expect(result.plan.detailed, isTrue);
    expect(result.plan.plan, isNotEmpty);
    expect(result.plan.lines, isNotEmpty);
    expect(result.rowCount, greaterThan(0));
    expect(result.columns, isNotEmpty);
  }

  final snapshot = FirebirdBenchmarkSnapshot.fromSuiteResult(suite);
  final comparison = snapshot.compare(suite);

  expect(comparison.passed, isTrue);
  expect(comparison.databaseMatches, isTrue);
  expect(
    comparison.scenarios.every((scenario) => scenario.verdict == 'pass'),
    isTrue,
  );
}
