import 'dart:typed_data';

import 'package:firebirdpod/firebirdpod.dart';
import 'package:serverpod_auth_core_server/serverpod_auth_core_server.dart'
    as auth;
import 'package:serverpod_database/serverpod_database.dart';
import 'package:test/test.dart';

import 'firebird_serverpod_module_test_support.dart';
import 'firebird_test_support.dart';

void main() {
  group('Firebird serverpod_auth_core module', () {
    test(
      'schema round-trips and persists auth rows with generated UUID ids',
      () async {
        if (!shouldRunDirectIntegrationTests()) {
          markTestSkipped(
            'set FIREBIRDPOD_RUN_FBCLIENT_DIRECT=1 to run live Firebird module tests',
          );
          return;
        }

        registerFirebirdServerpodDialect();

        final poolManager = FirebirdServerpodPoolManager(
          auth.Protocol(),
          null,
          FirebirdServerpodDatabaseConfig(
            host: 'localhost',
            port: 3050,
            user: firebirdTestUser(),
            password: firebirdTestPassword(),
            name: firebirdTestDatabasePath(),
            charset: 'UTF8',
            fbClientLibraryPath: firebirdClientLibraryPath(),
          ),
        )..start();
        addTearDown(poolManager.stop);

        late Database database;
        final session = _TestSession(() => database);
        database = DatabaseConstructor.create(
          session: session,
          poolManager: poolManager,
        );
        final targetTables = auth.Protocol().getTargetTableDefinitions();
        await cleanupModuleArtifacts(session, targetTables);
        addTearDown(() => cleanupModuleArtifacts(session, targetTables));

        final definition = DatabaseDefinition(
          moduleName: auth.Protocol().getModuleName(),
          tables: auth.Protocol().getTargetTableDefinitions(),
          installedModules: const [],
          migrationApiVersion: 1,
        );
        final definitionSql = const FirebirdServerpodSqlGenerator()
            .generateDatabaseDefinitionSql(
              definition,
              installedModules: const [],
            );

        await session.db.unsafeSimpleExecute(definitionSql);

        expect(await MigrationManager.verifyDatabaseIntegrity(session), isTrue);

        final authUser = await session.db.insertRow<auth.AuthUser>(
          auth.AuthUser(scopeNames: {'profile'}),
        );
        expect(authUser.id, isNotNull);

        final refreshToken = await session.db.insertRow<auth.RefreshToken>(
          auth.RefreshToken(
            authUserId: authUser.id!,
            scopeNames: {'profile'},
            method: 'email',
            fixedSecret: ByteData(16),
            rotatingSecretHash: 'argon2id\$test',
          ),
        );
        expect(refreshToken.id, isNotNull);

        final fetchedRefreshToken = await session.db
            .findById<auth.RefreshToken>(
              refreshToken.id!,
              include: auth.RefreshToken.include(
                authUser: auth.AuthUser.include(),
              ),
            );

        expect(fetchedRefreshToken, isNotNull);
        expect(fetchedRefreshToken!.authUser?.id, authUser.id);
        expect(fetchedRefreshToken.scopeNames, {'profile'});
      },
    );
  });
}

class _TestSession implements DatabaseSession {
  _TestSession(this._database);

  final Database Function() _database;

  @override
  Database get db => _database();

  @override
  Transaction? get transaction => null;

  @override
  LogQueryFunction? get logQuery => null;

  @override
  LogWarningFunction? get logWarning => null;
}
