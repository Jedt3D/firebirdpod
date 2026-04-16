import 'package:firebirdpod/firebirdpod.dart';
import 'package:serverpod_database/serverpod_database.dart';
import 'package:serverpod_shared/serverpod_shared.dart';
import 'package:test/test.dart';
import 'package:yaml/yaml.dart';

class _TestSerializationManager extends SerializationManagerServer {
  @override
  String getModuleName() => 'test';

  @override
  Table? getTableForType(Type t) => null;

  @override
  List<TableDefinition> getTargetTableDefinitions() => const [];
}

void main() {
  group('Serverpod Firebird registration', () {
    test('registers DatabaseConfig and DatabaseProvider hooks', () {
      registerFirebirdServerpodDialect();

      final config = DatabaseConfig(
        host: '127.0.0.1',
        port: 3050,
        user: 'sysdba',
        password: 'masterkey',
        name: '/tmp/example.fdb',
        dialect: DatabaseDialect.firebird,
      );

      expect(config, isA<FirebirdServerpodDatabaseConfig>());
      expect(config.dialect, DatabaseDialect.firebird);

      final provider = DatabaseProvider.forDialect(DatabaseDialect.firebird);
      expect(provider, isA<FirebirdServerpodDatabaseProvider>());
    });

    test(
      'loads Firebird database config through ServerpodConfig and creates the pool manager',
      () {
        registerFirebirdServerpodDialect();

        const configSource = '''
apiServer:
  port: 8080
  publicHost: localhost
  publicPort: 8080
  publicScheme: http
database:
  dialect: firebird
  host: 127.0.0.1
  port: 3050
  name: /tmp/serverpod-firebird.fdb
  user: sysdba
  charset: UTF8
  role: app_user
  fbClientLibraryPath: /opt/firebird/lib/libfbclient.dylib
  statementTimeoutInSeconds: 7
  maxConnectionCount: 12
''';

        final serverpodConfig = ServerpodConfig.loadFromMap(
          'development',
          null,
          const {
            'serviceSecret': 'LONG_PASSWORD_THAT_IS_REQUIRED',
            'database': 'masterkey',
          },
          loadYaml(configSource),
        );

        final databaseConfig =
            serverpodConfig.database as FirebirdServerpodDatabaseConfig;
        expect(databaseConfig.dialect, DatabaseDialect.firebird);
        expect(databaseConfig.host, '127.0.0.1');
        expect(databaseConfig.port, 3050);
        expect(databaseConfig.name, '/tmp/serverpod-firebird.fdb');
        expect(databaseConfig.user, 'sysdba');
        expect(databaseConfig.password, 'masterkey');
        expect(databaseConfig.charset, 'UTF8');
        expect(databaseConfig.role, 'app_user');
        expect(
          databaseConfig.fbClientLibraryPath,
          '/opt/firebird/lib/libfbclient.dylib',
        );
        expect(
          databaseConfig.defaultStatementTimeout,
          const Duration(seconds: 7),
        );
        expect(databaseConfig.maxConnectionCount, 12);

        final provider = DatabaseProvider.forDialect(DatabaseDialect.firebird);
        final poolManager = provider.createPoolManager(
          _TestSerializationManager(),
          null,
          databaseConfig,
        );

        expect(poolManager, isA<FirebirdServerpodPoolManager>());
        expect(poolManager.dialect, DatabaseDialect.firebird);
        expect(
          (poolManager as FirebirdServerpodPoolManager).config,
          same(databaseConfig),
        );
      },
    );

    test(
      'rejects pool-level runtime parameters until Firebird has a native mapping',
      () {
        registerFirebirdServerpodDialect();

        final provider = DatabaseProvider.forDialect(DatabaseDialect.firebird);

        expect(
          () => provider.createPoolManager(
            _TestSerializationManager(),
            (params) => [params.searchPaths(['app'])],
            FirebirdServerpodDatabaseConfig(
              host: '127.0.0.1',
              port: 3050,
              user: 'sysdba',
              password: 'masterkey',
              name: '/tmp/serverpod-firebird.fdb',
            ),
          ),
          throwsA(
            isA<UnsupportedError>().having(
              (error) => error.message,
              'message',
              contains('pool-level Serverpod runtime parameters'),
            ),
          ),
        );
      },
    );
  });
}
