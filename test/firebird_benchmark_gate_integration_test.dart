import 'package:firebirdpod/firebirdpod.dart';
import 'package:test/test.dart';

import 'firebird_test_support.dart';

void main() {
  group('Firebird benchmark gate', () {
    test(
      'passes against the committed snapshots for all benchmark targets',
      () async {
        if (!shouldRunDirectIntegrationTests()) {
          print(
            'set FIREBIRDPOD_RUN_FBCLIENT_DIRECT=1 to run live Firebird benchmark gate tests',
          );
          return;
        }

        final runner = FirebirdBenchmarkGateRunner(
          connect: (target) =>
              buildDirectEndpoint(database: target.databasePath).connect(),
        );
        final summary = await runner.runTargets(
          firebirdResolveBenchmarkTargets(null),
          options: const FirebirdBenchmarkOptions(
            warmupIterations: 1,
            measuredIterations: 5,
          ),
        );

        expect(summary.results, hasLength(3));
        expect(summary.passed, isTrue);
        expect(summary.failedTargetCount, 0);
        expect(summary.failingScenarioCount, 0);
      },
    );
  });
}
