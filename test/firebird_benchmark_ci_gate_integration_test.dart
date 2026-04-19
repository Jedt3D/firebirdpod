import 'package:firebirdpod/firebirdpod.dart';
import 'package:test/test.dart';

import 'firebird_test_support.dart';

void main() {
  group('Firebird benchmark CI gate', () {
    test(
      'passes the calibrated CI smoke policy on the shared employee fixture',
      () async {
        if (!shouldRunDirectIntegrationTests()) {
          print(
            'set FIREBIRDPOD_RUN_FBCLIENT_DIRECT=1 to run live Firebird benchmark CI gate tests',
          );
          return;
        }

        final policy = await FirebirdBenchmarkGatePolicy.load(
          'benchmarks/policies/ci_smoke.json',
        );
        final runner = FirebirdBenchmarkGateRunner(
          connect: (target) =>
              buildDirectEndpoint(database: target.databasePath).connect(),
        );
        final summary = await runner.runTargets(
          policy.resolveTargets(),
          options: policy.options,
        );

        expect(summary.results, hasLength(1));
        expect(summary.results.single.target.name, 'employee');
        expect(summary.passed, isTrue);
        expect(summary.failedTargetCount, 0);
      },
    );

    test(
      'passes the broader calibrated CI candidate policy across all three benchmark databases',
      () async {
        if (!shouldRunDirectIntegrationTests()) {
          print(
            'set FIREBIRDPOD_RUN_FBCLIENT_DIRECT=1 to run live Firebird benchmark CI gate tests',
          );
          return;
        }

        final policy = await FirebirdBenchmarkGatePolicy.load(
          'benchmarks/policies/ci_candidate_multi_database.json',
        );
        final runner = FirebirdBenchmarkGateRunner(
          connect: (target) =>
              buildDirectEndpoint(database: target.databasePath).connect(),
        );
        final summary = await runner.runTargets(
          policy.resolveTargets(),
          options: policy.options,
        );

        expect(summary.results, hasLength(3));
        expect(summary.results.map((result) => result.target.name), <String>[
          'employee',
          'chinook',
          'northwind',
        ]);
        expect(summary.passed, isTrue);
        expect(summary.failedTargetCount, 0);
      },
    );
  });
}
