import 'package:firebirdpod/firebirdpod.dart';
import 'package:test/test.dart';

import 'firebird_test_support.dart';

void main() {
  group('Firebird timeout diagnostics', () {
    test('captures connection and statement timeout state', () async {
      if (!shouldRunDirectIntegrationTests()) {
        markTestSkipped(
          'set FIREBIRDPOD_RUN_FBCLIENT_DIRECT=1 to run live Firebird timeout tests',
        );
        return;
      }

      final connection = await buildDirectEndpoint(
        database: firebirdTestDatabasePath(),
      ).connect();
      addTearDown(connection.close);

      await connection.setStatementTimeout(const Duration(milliseconds: 250));
      final connectionState = await connection.timeoutDiagnostics
          .captureConnectionState();

      final statement = await connection.prepare(
        'select current_connection as CONN_ID from rdb\$database',
      );
      addTearDown(statement.close);

      await statement.setTimeout(const Duration(milliseconds: 90));
      final statementState = await connection.timeoutDiagnostics
          .captureStatementState(statement);

      expect(
        connectionState.configuredTimeout,
        const Duration(milliseconds: 250),
      );
      expect(connectionState.systemContextMilliseconds, 250);
      expect(
        statementState.configuredTimeout,
        const Duration(milliseconds: 90),
      );
      expect(statementState.parameterStyle, FirebirdParameterStyle.none);
      expect(statementState.parameterCount, 0);
      expect(
        statementState.normalizedSql,
        'select current_connection as CONN_ID from rdb\$database',
      );
    });

    test(
      'observes timeout-classified execution within the configured budget window',
      () async {
        if (!shouldRunDirectIntegrationTests()) {
          markTestSkipped(
            'set FIREBIRDPOD_RUN_FBCLIENT_DIRECT=1 to run live Firebird timeout tests',
          );
          return;
        }

        final connection = await buildDirectEndpoint(
          database: firebirdTestDatabasePath(),
        ).connect();
        addTearDown(connection.close);

        final requestedTimeout = const Duration(milliseconds: 20);
        final observation = await connection.timeoutDiagnostics
            .observeExecution('''
          select count(*)
          from rdb\$types a
          cross join rdb\$types b
          cross join rdb\$types c
          cross join rdb\$types d
          cross join rdb\$types e
          ''', timeout: requestedTimeout);

        expect(observation.succeeded, isFalse);
        expect(observation.timedOut, isTrue);
        expect(observation.connectionTimeout, isNull);
        expect(observation.connectionSystemContextMilliseconds, 0);
        expect(observation.requestedStatementTimeout, requestedTimeout);
        expect(observation.statementTimeout, requestedTimeout);
        expect(observation.parameterStyle, FirebirdParameterStyle.none);
        expect(observation.parameterCount, 0);
        expect(observation.error, isA<FirebirdDatabaseException>());
        expect(
          observation.elapsed,
          lessThanOrEqualTo(requestedTimeout + const Duration(seconds: 1)),
        );
      },
    );
  });
}
