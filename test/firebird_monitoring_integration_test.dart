import 'package:firebirdpod/firebirdpod.dart';
import 'package:test/test.dart';

import 'firebird_test_support.dart';

void main() {
  group('Firebird monitoring', () {
    test(
      'captures current and external attachment snapshots on a restored database',
      () async {
        if (!shouldRunDirectIntegrationTests()) {
          markTestSkipped(
            'set FIREBIRDPOD_RUN_FBCLIENT_DIRECT=1 to run live Firebird monitoring tests',
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
        addTearDown(workerConnection.close);

        final currentSnapshot = await monitorConnection.monitoring
            .captureCurrentAttachmentSnapshot();
        final workerAttachmentId = await workerConnection.monitoring
            .currentAttachmentId();

        expect(
          currentSnapshot.attachmentId,
          await monitorConnection.monitoring.currentAttachmentId(),
        );
        expect(currentSnapshot.attachmentCount, 1);
        expect(currentSnapshot.transactionCount, 1);
        expect(currentSnapshot.statementCount, greaterThan(0));
        expect(
          currentSnapshot.attachments.single.user,
          firebirdTestUser().toUpperCase(),
        );
        expect(
          currentSnapshot.statements
              .map((statement) => statement.sqlText)
              .whereType<String>(),
          contains(contains('from mon\$statements')),
        );

        final externalSnapshot = await _eventually(
          () => monitorConnection.monitoring.captureExternalSnapshot(),
          (snapshot) => snapshot.attachmentIds.contains(workerAttachmentId),
        );
        final externalUserAttachmentIds = externalSnapshot.attachments
            .where((attachment) => attachment.isSystem != true)
            .map((attachment) => attachment.id)
            .toList(growable: false);

        final workerSnapshot = await _eventually(
          () => monitorConnection.monitoring.captureAttachmentSnapshot(
            workerAttachmentId,
          ),
          (snapshot) =>
              snapshot.transactionCount == 1 && snapshot.statementCount == 0,
        );

        expect(externalUserAttachmentIds, [workerAttachmentId]);
        expect(
          externalSnapshot.attachments.any(
            (attachment) => attachment.id == workerAttachmentId,
          ),
          isTrue,
        );
        expect(workerSnapshot.attachmentId, workerAttachmentId);
        expect(workerSnapshot.attachmentCount, 1);
        expect(workerSnapshot.transactionCount, 1);
        expect(workerSnapshot.statementCount, 0);
        expect(
          workerSnapshot.attachments.single.user,
          firebirdTestUser().toUpperCase(),
        );
      },
    );

    test(
      'captures open statement and explicit transaction metrics for a worker attachment',
      () async {
        if (!shouldRunDirectIntegrationTests()) {
          markTestSkipped(
            'set FIREBIRDPOD_RUN_FBCLIENT_DIRECT=1 to run live Firebird monitoring tests',
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
        addTearDown(workerConnection.close);

        final workerAttachmentId = await workerConnection.monitoring
            .currentAttachmentId();
        final retainedStatement = await workerConnection.prepare(
          'select current_connection as CONN_ID from rdb\$database',
        );
        addTearDown(retainedStatement.close);

        final transaction = await workerConnection.beginTransaction();
        addTearDown(transaction.close);

        final transactionStatement = await transaction.prepare(
          'select current_connection as TX_CONN_ID from rdb\$database',
        );
        addTearDown(transactionStatement.close);

        final snapshot = await _eventually(
          () => monitorConnection.monitoring.captureAttachmentSnapshot(
            workerAttachmentId,
          ),
          (value) => value.transactionCount == 2 && value.statementCount == 2,
        );

        expect(snapshot.attachmentId, workerAttachmentId);
        expect(snapshot.attachmentCount, 1);
        expect(snapshot.transactionCount, 2);
        expect(snapshot.statementCount, 2);
        expect(
          snapshot.transactions.every(
            (transaction) => transaction.attachmentId == workerAttachmentId,
          ),
          isTrue,
        );
        expect(
          snapshot.transactions.any(
            (transaction) => transaction.isolationMode != null,
          ),
          isTrue,
        );
        expect(
          snapshot.statements
              .map((statement) => statement.sqlText)
              .whereType<String>(),
          containsAll([
            contains('current_connection as CONN_ID'),
            contains('current_connection as TX_CONN_ID'),
          ]),
        );
      },
    );
  });
}

Future<T> _eventually<T>(
  Future<T> Function() read,
  bool Function(T value) isDone,
) async {
  late T lastValue;
  for (var attempt = 0; attempt < 40; attempt++) {
    lastValue = await read();
    if (isDone(lastValue)) {
      return lastValue;
    }
    await Future<void>.delayed(const Duration(milliseconds: 50));
  }
  return lastValue;
}
