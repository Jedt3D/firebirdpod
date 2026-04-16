import 'package:serverpod_database/serverpod_database.dart';
import 'package:serverpod_shared/serverpod_shared.dart';

import 'firebird_serverpod_analyzer.dart';
import 'firebird_serverpod_config.dart';
import 'firebird_serverpod_connection.dart';
import 'firebird_serverpod_migration_runner.dart';
import 'firebird_serverpod_pool_manager.dart';

/// First Firebird database provider for Serverpod integration work.
class FirebirdServerpodDatabaseProvider implements DatabaseProvider {
  const FirebirdServerpodDatabaseProvider();

  @override
  DatabaseDefinitionRestrictions get definitionRestrictions =>
      const DatabaseDefinitionRestrictions(
        supportedIndexTypes: ['btree'],
      );

  @override
  FirebirdServerpodPoolManager createPoolManager(
    SerializationManagerServer serializationManager,
    RuntimeParametersListBuilder? runtimeParametersBuilder,
    covariant DatabaseConfig config,
  ) {
    if (config is! FirebirdServerpodDatabaseConfig) {
      throw ArgumentError.value(
        config,
        'config',
        'Expected FirebirdServerpodDatabaseConfig for Firebird dialect.',
      );
    }

    if (runtimeParametersBuilder != null) {
      throw UnsupportedError(
        'Firebird does not support pool-level Serverpod runtime parameters '
        'yet. Use transaction.setRuntimeParameters(...) on explicit '
        'transactions until a Firebird-native pool-level mapping exists.',
      );
    }

    return FirebirdServerpodPoolManager(
      serializationManager,
      runtimeParametersBuilder,
      config,
    );
  }

  @override
  FirebirdServerpodDatabaseConnection createConnection(
    covariant FirebirdServerpodPoolManager poolManager,
  ) {
    return FirebirdServerpodDatabaseConnection(poolManager);
  }

  @override
  FirebirdServerpodMigrationRunner createMigrationRunner({String? runMode}) {
    return FirebirdServerpodMigrationRunner(runMode: runMode);
  }

  @override
  FirebirdServerpodDatabaseAnalyzer createAnalyzer(Database database) {
    return FirebirdServerpodDatabaseAnalyzer(database: database);
  }
}
