import 'package:firebirdpod/firebirdpod.dart';
import 'package:test/test.dart';

import 'firebird_test_support.dart';

const _tableName = 'FIREBIRDPOD_READ_CONSISTENCY_ITEM';

void main() {
  group('Firebird read consistency', () {
    FirebirdRestoredDatabase? database;

    setUpAll(() async {
      if (!shouldRunDirectIntegrationTests()) {
        return;
      }

      database = await FirebirdRestoredDatabase.create();

      final connection = await _openConnection(database!);
      try {
        await _dropTableIfExists(connection, _tableName);
        await connection.execute('''
          create table $_tableName (
            ID integer not null primary key,
            NAME varchar(50) not null
          )
        ''');
      } finally {
        await connection.close();
      }
    });

    tearDownAll(() async {
      if (database == null) {
        return;
      }

      try {
        final connection = await _openConnection(database!);
        try {
          await _dropTableIfExists(connection, _tableName);
        } finally {
          await connection.close();
        }
      } finally {
        await database!.dispose();
      }
    });

    setUp(() async {
      if (database == null) {
        return;
      }

      final connection = await _openConnection(database!);
      try {
        await connection.execute('delete from $_tableName');
        await connection.execute(
          '''
          insert into $_tableName (ID, NAME)
          values (\$1, \$2)
          ''',
          parameters: FirebirdStatementParameters.positional([1, 'before']),
        );
      } finally {
        await connection.close();
      }
    });

    test(
      'read committed read consistency sees a new committed version on the next statement',
      () async {
        if (!shouldRunDirectIntegrationTests()) {
          markTestSkipped(
            'set FIREBIRDPOD_RUN_FBCLIENT_DIRECT=1 to run live Firebird read-consistency tests',
          );
          return;
        }

        final reader = await _openConnection(database!);
        addTearDown(reader.close);
        final writer = await _openConnection(database!);
        addTearDown(writer.close);

        final transaction = await reader.beginTransaction(
          settings: const FirebirdTransactionSettings(
            isolationLevel: FirebirdTransactionIsolationLevel.readCommitted,
          ),
        );
        addTearDown(transaction.close);

        final state = await transaction.readConsistency.captureState();
        final firstValue = await _readName(transaction);

        await _writeName(writer, 'after');

        final secondValue = await _readName(transaction);

        expect(state.isolationLevelName, 'READ COMMITTED');
        expect(state.monitorIsolationMode, 4);
        expect(
          state.monitorIsolationModeName,
          'READ COMMITTED READ CONSISTENCY',
        );
        expect(state.usesReadConsistency, isTrue);
        expect(firstValue, 'before');
        expect(secondValue, 'after');
      },
    );

    test(
      'repeatable read keeps its snapshot across statements after a concurrent commit',
      () async {
        if (!shouldRunDirectIntegrationTests()) {
          markTestSkipped(
            'set FIREBIRDPOD_RUN_FBCLIENT_DIRECT=1 to run live Firebird read-consistency tests',
          );
          return;
        }

        final reader = await _openConnection(database!);
        addTearDown(reader.close);
        final writer = await _openConnection(database!);
        addTearDown(writer.close);

        final transaction = await reader.beginTransaction(
          settings: const FirebirdTransactionSettings(
            isolationLevel: FirebirdTransactionIsolationLevel.repeatableRead,
          ),
        );
        addTearDown(transaction.close);

        final state = await transaction.readConsistency.captureState();
        final firstValue = await _readName(transaction);

        await _writeName(writer, 'after');

        final secondValue = await _readName(transaction);
        final committedValue = await _readNameOutsideTransaction(writer);

        expect(state.isolationLevelName, 'SNAPSHOT');
        expect(state.monitorIsolationMode, 1);
        expect(state.monitorIsolationModeName, 'SNAPSHOT');
        expect(state.usesReadConsistency, isFalse);
        expect(firstValue, 'before');
        expect(secondValue, 'before');
        expect(committedValue, 'after');
      },
    );
  });
}

Future<FirebirdConnection> _openConnection(
  FirebirdRestoredDatabase database,
) async {
  return buildDirectEndpoint(database: database.databasePath).connect();
}

Future<void> _dropTableIfExists(
  FirebirdConnection connection,
  String tableName,
) async {
  try {
    await connection.execute('drop table $tableName');
  } catch (_) {}
}

Future<String?> _readName(FirebirdTransaction transaction) async {
  final result = await transaction.execute('''
    select NAME
    from $_tableName
    where ID = 1
    ''');
  return result.singleRow?['NAME'] as String?;
}

Future<String?> _readNameOutsideTransaction(
  FirebirdConnection connection,
) async {
  final result = await connection.execute('''
    select NAME
    from $_tableName
    where ID = 1
    ''');
  return result.singleRow?['NAME'] as String?;
}

Future<void> _writeName(FirebirdConnection connection, String value) async {
  final result = await connection.execute('''
    update $_tableName
    set NAME = \$1
    where ID = 1
    ''', parameters: FirebirdStatementParameters.positional([value]));
  expect(result.affectedRows, 1);
}
