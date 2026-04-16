import 'package:serverpod_database/serverpod_database.dart';
import 'package:serverpod_shared/serverpod_shared.dart';

import 'firebird_serverpod_config.dart';
import 'firebird_serverpod_provider.dart';

bool _isFirebirdServerpodDialectRegistered = false;

/// Registers Firebird as an externally implemented Serverpod dialect in the
/// local development workspace.
void registerFirebirdServerpodDialect() {
  if (_isFirebirdServerpodDialectRegistered) return;

  DatabaseConfig.registerDialect(
    dialect: DatabaseDialect.firebird,
    factory: ({
      required String host,
      required int port,
      required String user,
      required String password,
      required String name,
      required bool requireSsl,
      required bool isUnixSocket,
      required List<String>? searchPaths,
      required int? maxConnectionCount,
    }) {
      return FirebirdServerpodDatabaseConfig(
        host: host,
        port: port,
        user: user,
        password: password,
        name: name,
        maxConnectionCount: maxConnectionCount,
      );
    },
    parser: FirebirdServerpodDatabaseConfig.fromServerpodConfig,
  );

  DatabaseProvider.registerDialect(
    DatabaseDialect.firebird,
    () => const FirebirdServerpodDatabaseProvider(),
  );

  _isFirebirdServerpodDialectRegistered = true;
}
