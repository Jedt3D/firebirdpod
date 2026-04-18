import 'dart:io';

import 'package:fbdb/fbclient.dart' as fbclient;
import 'package:fbdb/fbdb.dart' as fbdb;
import 'package:firebirdpod/firebirdpod.dart';
import 'package:test/test.dart';

import 'firebird_test_support.dart';

void main() {
  group('Firebird service manager', () {
    test('can query the Firebird server version', () async {
      if (!shouldRunDirectIntegrationTests()) {
        markTestSkipped(
          'set FIREBIRDPOD_RUN_FBCLIENT_DIRECT=1 to run live Firebird admin tests',
        );
        return;
      }

      final connection = await _attachServiceManager();
      addTearDown(connection.close);

      final version = await connection.queryServerVersion();

      expect(version, contains('Firebird'));
    });

    test('can collect database statistics for the shared fixture', () async {
      if (!shouldRunDirectIntegrationTests()) {
        markTestSkipped(
          'set FIREBIRDPOD_RUN_FBCLIENT_DIRECT=1 to run live Firebird admin tests',
        );
        return;
      }

      final connection = await _attachServiceManager();
      addTearDown(connection.close);

      final report = await connection.getDatabaseStatistics();
      final output = report.lines.join('\n');

      expect(report.database, firebirdTestDatabasePath());
      expect(report.lines, isNotEmpty);
      expect(
        output,
        anyOf(
          contains('Database header page information'),
          contains('Database file sequence'),
          contains('Checksum'),
        ),
      );
    });

    test('can validate the shared fixture without service errors', () async {
      if (!shouldRunDirectIntegrationTests()) {
        markTestSkipped(
          'set FIREBIRDPOD_RUN_FBCLIENT_DIRECT=1 to run live Firebird admin tests',
        );
        return;
      }

      final connection = await _attachServiceManager();
      addTearDown(connection.close);

      final report = await connection.validateDatabase();
      final normalizedOutput = report.lines.join('\n').toLowerCase();

      expect(report.database, firebirdTestDatabasePath());
      expect(normalizedOutput, isNot(contains('error')));
      expect(normalizedOutput, isNot(contains('corrupt')));
    });

    test('can backup and restore the shared fixture', () async {
      if (!shouldRunDirectIntegrationTests()) {
        markTestSkipped(
          'set FIREBIRDPOD_RUN_FBCLIENT_DIRECT=1 to run live Firebird admin tests',
        );
        return;
      }

      final tempDirectory = await _createAdminTempDirectory();
      final backupFile = '${tempDirectory.path}/employee_roundtrip.fbk';
      final restoredDatabasePath =
          '${tempDirectory.path}/employee_roundtrip.fdb';

      addTearDown(
        () => _disposeRestoreArtifacts(
          tempDirectory,
          restoredDatabasePath: restoredDatabasePath,
        ),
      );

      final connection = await _attachServiceManager();
      addTearDown(connection.close);

      final backupReport = await connection.backupDatabase(
        backupFile: backupFile,
      );
      final backupOutput = backupReport.lines.join('\n').toLowerCase();

      expect(backupReport.database, firebirdTestDatabasePath());
      expect(backupReport.backupFile, backupFile);
      expect(await File(backupFile).exists(), isTrue);
      expect(await File(backupFile).length(), greaterThan(0));
      expect(backupOutput, isNot(contains('error')));

      final restoreReport = await connection.restoreDatabase(
        backupFile: backupFile,
        database: restoredDatabasePath,
      );
      final restoreOutput = restoreReport.lines.join('\n').toLowerCase();

      expect(restoreReport.backupFile, backupFile);
      expect(restoreReport.database, restoredDatabasePath);
      expect(await File(restoredDatabasePath).exists(), isTrue);
      expect(restoreOutput, isNot(contains('error')));

      final restoredConnection = await buildDirectEndpoint(
        database: restoredDatabasePath,
      ).connect();
      addTearDown(restoredConnection.close);

      final result = await restoredConnection.execute('''
        select count(*) as total_rows
        from employee
        ''');

      expect(_readIntValue(result.singleRow?['TOTAL_ROWS']), greaterThan(0));
    });

    test('can sweep and cycle shutdown state on a restored database', () async {
      if (!shouldRunDirectIntegrationTests()) {
        markTestSkipped(
          'set FIREBIRDPOD_RUN_FBCLIENT_DIRECT=1 to run live Firebird admin tests',
        );
        return;
      }

      final tempDirectory = await _createAdminTempDirectory();
      final backupFile = '${tempDirectory.path}/employee_admin_cycle.fbk';
      final restoredDatabasePath =
          '${tempDirectory.path}/employee_admin_cycle.fdb';

      addTearDown(
        () => _disposeRestoreArtifacts(
          tempDirectory,
          restoredDatabasePath: restoredDatabasePath,
        ),
      );

      final serviceManager = await _attachServiceManager();
      addTearDown(serviceManager.close);

      await serviceManager.backupDatabase(backupFile: backupFile);
      await serviceManager.restoreDatabase(
        backupFile: backupFile,
        database: restoredDatabasePath,
      );

      final sweepReport = await serviceManager.sweepDatabase(
        database: restoredDatabasePath,
      );
      final sweepOutput = sweepReport.lines.join('\n').toLowerCase();

      expect(sweepReport.database, restoredDatabasePath);
      expect(sweepOutput, isNot(contains('error')));
      expect(sweepOutput, isNot(contains('corrupt')));

      final beforeShutdownConnection = await buildDirectEndpoint(
        database: restoredDatabasePath,
      ).connect();
      try {
        final result = await beforeShutdownConnection.execute('''
          select count(*) as total_rows
          from employee
          ''');
        expect(_readIntValue(result.singleRow?['TOTAL_ROWS']), greaterThan(0));
      } finally {
        await beforeShutdownConnection.close();
      }

      await serviceManager.shutdownDatabase(
        database: restoredDatabasePath,
        options: const FirebirdDatabaseShutdownOptions(
          mode: FirebirdDatabaseShutdownMode.full,
          method: FirebirdDatabaseShutdownMethod.force,
          timeoutSeconds: 0,
        ),
      );

      final shutdownException = await _captureDbException(
        () => buildDirectEndpoint(database: restoredDatabasePath).connect(),
      );
      expect(
        shutdownException.errorCodes,
        anyOf(
          contains(fbclient.FbErrorCodes.isc_shutdown),
          contains(fbclient.FbErrorCodes.isc_att_shutdown),
        ),
      );

      final onlineReport = await serviceManager.bringDatabaseOnline(
        database: restoredDatabasePath,
      );
      expect(onlineReport.database, restoredDatabasePath);

      final afterOnlineConnection = await buildDirectEndpoint(
        database: restoredDatabasePath,
      ).connect();
      try {
        final result = await afterOnlineConnection.execute('''
          select count(*) as total_rows
          from employee
          ''');
        expect(_readIntValue(result.singleRow?['TOTAL_ROWS']), greaterThan(0));
      } finally {
        await afterOnlineConnection.close();
      }
    });
  });
}

Future<FirebirdServiceManagerConnection> _attachServiceManager() {
  return FirebirdServiceManager(
    fbClientLibraryPath: firebirdClientLibraryPath(),
  ).attach(
    FirebirdConnectionOptions(
      host: 'localhost',
      port: 3050,
      database: firebirdTestDatabasePath(),
      user: firebirdTestUser(),
      password: firebirdTestPassword(),
      charset: 'UTF8',
    ),
  );
}

Future<Directory> _createAdminTempDirectory() async {
  final baseDirectory = Directory(
    '${File(firebirdTestDatabasePath()).parent.path}/.firebirdpod_tmp',
  );
  await baseDirectory.create(recursive: true);
  return baseDirectory.createTemp('firebirdpod_admin_');
}

Future<void> _disposeRestoreArtifacts(
  Directory directory, {
  required String restoredDatabasePath,
}) async {
  try {
    if (await File(restoredDatabasePath).exists()) {
      final database = await fbdb.FbDb.attach(
        database: restoredDatabasePath,
        user: firebirdTestUser(),
        password: firebirdTestPassword(),
        options: fbdb.FbOptions(libFbClient: firebirdClientLibraryPath()),
      );
      try {
        await database.dropDatabase();
      } finally {
        try {
          await database.detach();
        } catch (_) {}
      }
    }
  } catch (_) {
    if (await File(restoredDatabasePath).exists()) {
      try {
        await File(restoredDatabasePath).delete();
      } catch (_) {}
    }
  } finally {
    if (await directory.exists()) {
      try {
        await directory.delete(recursive: true);
      } catch (_) {}
    }
  }
}

int _readIntValue(Object? value) {
  if (value is int) return value;
  if (value is BigInt) return value.toInt();
  throw StateError('Unexpected integer value type: ${value.runtimeType}');
}

Future<FirebirdDatabaseException> _captureDbException(
  Future<Object?> Function() action,
) async {
  try {
    await action();
  } on FirebirdDatabaseException catch (error) {
    return error;
  }

  fail('Expected FirebirdDatabaseException to be thrown.');
}
