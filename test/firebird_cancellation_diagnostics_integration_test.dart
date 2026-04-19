import 'package:firebirdpod/firebirdpod.dart';
import 'package:test/test.dart';

import 'firebird_test_support.dart';

void main() {
  group('Firebird cancellation diagnostics', () {
    test(
      'raise reports nothing to cancel on the current same-isolate control path',
      () async {
        if (!shouldRunDirectIntegrationTests()) {
          markTestSkipped(
            'set FIREBIRDPOD_RUN_FBCLIENT_DIRECT=1 to run live Firebird cancellation tests',
          );
          return;
        }

        final database = await FirebirdRestoredDatabase.create();
        addTearDown(database.dispose);

        final monitorConnection = await buildDirectEndpoint(
          database: database.databasePath,
        ).connect();
        addTearDown(monitorConnection.close);

        final workerConnection = await buildDirectEndpoint(
          database: database.databasePath,
        ).connect();
        addTearDown(() async {
          try {
            await workerConnection.close();
          } catch (_) {}
        });

        final workerState = await workerConnection.cancellationDiagnostics
            .captureConnectionState();
        final beforeSnapshot = await _eventually(
          () => monitorConnection.monitoring.captureAttachmentSnapshot(
            workerState.attachmentId,
          ),
          (snapshot) => snapshot.attachmentCount == 1,
        );

        final observed = await workerConnection.cancellationDiagnostics
            .observeRequest();

        final reuseResult = await workerConnection.execute(
          'select current_connection as CONN_ID from rdb\$database',
        );
        final afterSnapshot = await _eventually(
          () => monitorConnection.monitoring.captureAttachmentSnapshot(
            workerState.attachmentId,
          ),
          (snapshot) => snapshot.attachmentCount == 1,
        );

        expect(beforeSnapshot.attachmentCount, 1);
        expect(observed.attachmentId, workerState.attachmentId);
        expect(observed.requestedMode, FirebirdCancelMode.raise);
        expect(observed.requestAccepted, isFalse);
        expect(observed.reportedNothingToCancel, isTrue);
        expect(observed.cancelled, isFalse);
        expect(observed.timedOut, isFalse);
        expect(observed.requestError?.errorCodes, contains(335544933));
        expect(observed.connectionUsableAfterObservation, isTrue);
        expect(observed.probedAttachmentId, workerState.attachmentId);
        expect(observed.connectionProbeError, isNull);
        expect(reuseResult.singleRow?['CONN_ID'], workerState.attachmentId);
        expect(afterSnapshot.attachmentCount, 1);
        expect(afterSnapshot.transactionCount, greaterThanOrEqualTo(1));
      },
    );

    test(
      'abort invalidates the attachment and clears monitored statements',
      () async {
        if (!shouldRunDirectIntegrationTests()) {
          markTestSkipped(
            'set FIREBIRDPOD_RUN_FBCLIENT_DIRECT=1 to run live Firebird cancellation tests',
          );
          return;
        }

        final database = await FirebirdRestoredDatabase.create();
        addTearDown(database.dispose);

        final monitorConnection = await buildDirectEndpoint(
          database: database.databasePath,
        ).connect();
        addTearDown(monitorConnection.close);

        final workerConnection = await buildDirectEndpoint(
          database: database.databasePath,
        ).connect();
        addTearDown(() async {
          try {
            await workerConnection.close();
          } catch (_) {}
        });

        final workerState = await workerConnection.cancellationDiagnostics
            .captureConnectionState();
        final beforeSnapshot = await _eventually(
          () => monitorConnection.monitoring.captureAttachmentSnapshot(
            workerState.attachmentId,
          ),
          (snapshot) => snapshot.attachmentCount == 1,
        );

        final observed = await workerConnection.cancellationDiagnostics
            .observeRequest(mode: FirebirdCancelMode.abort);
        final afterSnapshot = await _eventually(
          () => monitorConnection.monitoring.captureAttachmentSnapshot(
            workerState.attachmentId,
          ),
          (snapshot) => snapshot.statementCount == 0,
        );

        expect(beforeSnapshot.attachmentCount, 1);
        expect(observed.attachmentId, workerState.attachmentId);
        expect(observed.requestedMode, FirebirdCancelMode.abort);
        expect(observed.requestAccepted, isTrue);
        expect(observed.cancelled, isFalse);
        expect(observed.timedOut, isFalse);
        expect(observed.likelyForcedAbort, isTrue);
        expect(observed.connectionUsableAfterObservation, isFalse);
        expect(observed.connectionProbeError, isA<FirebirdDatabaseException>());
        expect(observed.connectionProbeError?.operation, 'prepare');
        expect(afterSnapshot.attachmentCount, inInclusiveRange(0, 1));
        expect(afterSnapshot.transactionCount, inInclusiveRange(0, 1));
        expect(afterSnapshot.statementCount, 0);
      },
    );
  });
}

Future<T> _eventually<T>(
  Future<T> Function() read,
  bool Function(T value) isDone,
) async {
  late T lastValue;
  for (var attempt = 0; attempt < 200; attempt++) {
    lastValue = await read();
    if (isDone(lastValue)) {
      return lastValue;
    }
    await Future<void>.delayed(const Duration(milliseconds: 50));
  }
  return lastValue;
}
