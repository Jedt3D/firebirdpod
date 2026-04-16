import 'dart:io';

import 'package:fbdb/fbdb.dart' as fbdb;
import 'package:firebirdpod/firebirdpod.dart';

const _defaultFbClientLibraryPath =
    '/Library/Frameworks/Firebird.framework/Versions/A/Resources/lib/libfbclient.dylib';

bool shouldRunDirectIntegrationTests() {
  return Platform.environment['FIREBIRDPOD_RUN_FBCLIENT_DIRECT'] == '1';
}

bool shouldRunDirectStressTests() {
  return Platform.environment['FIREBIRDPOD_RUN_FBCLIENT_STRESS'] == '1';
}

int firebirdStressIterations() {
  return _readInt(Platform.environment['FIREBIRDPOD_STRESS_ITERATIONS']) ?? 50;
}

String firebirdClientLibraryPath() {
  return Platform.environment['FIREBIRDPOD_FBCLIENT_LIB'] ??
      _defaultFbClientLibraryPath;
}

String firebirdTestUser() {
  return Platform.environment['FIREBIRDPOD_TEST_USER'] ?? 'sysdba';
}

String firebirdTestPassword() {
  return Platform.environment['FIREBIRDPOD_TEST_PASSWORD'] ?? 'masterkey';
}

FirebirdEndpoint buildDirectEndpoint({
  required String database,
  Duration? statementTimeout,
}) {
  return FirebirdEndpoint(
    client: FirebirdFbClientNativeClient(
      fbClientLibraryPath: firebirdClientLibraryPath(),
    ),
    options: FirebirdConnectionOptions(
      host: Platform.environment['FIREBIRDPOD_TEST_HOST'] ?? 'localhost',
      port: _readInt(Platform.environment['FIREBIRDPOD_TEST_PORT']),
      database: database,
      user: firebirdTestUser(),
      password: firebirdTestPassword(),
      statementTimeout: statementTimeout,
    ),
  );
}

String firebirdTestDatabasePath() {
  return Platform.environment['FIREBIRDPOD_TEST_DATABASE'] ??
      '/Users/worajedt/GitHub/FireDart/databases/firebird/employee.fdb';
}

int? _readInt(String? value) {
  if (value == null || value.isEmpty) return null;
  return int.parse(value);
}

class FirebirdTempDatabase {
  FirebirdTempDatabase._({
    required this.directory,
    required this.databasePath,
  });

  final Directory directory;
  final String databasePath;

  static Future<FirebirdTempDatabase> create({
    required List<String> schemaStatements,
  }) async {
    final baseDirectory = Directory(
      '${File(firebirdTestDatabasePath()).parent.path}/.firebirdpod_tmp',
    );
    await baseDirectory.create(recursive: true);

    final directory = await baseDirectory.createTemp(
      'firebirdpod_phase01_',
    );
    final databasePath = '${directory.path}/phase01.fdb';

    final database = await fbdb.FbDb.createDatabase(
      database: databasePath,
      user: firebirdTestUser(),
      password: firebirdTestPassword(),
      options: fbdb.FbOptions(
        libFbClient: firebirdClientLibraryPath(),
        dbCharset: 'UTF8',
      ),
    );

    try {
      for (final sql in schemaStatements) {
        await database.execute(sql: sql);
      }
    } finally {
      await database.detach();
    }

    return FirebirdTempDatabase._(
      directory: directory,
      databasePath: databasePath,
    );
  }

  Future<void> dispose() async {
    try {
      final database = await fbdb.FbDb.attach(
        database: databasePath,
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
    } catch (_) {
      if (await File(databasePath).exists()) {
        try {
          await File(databasePath).delete();
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
}
