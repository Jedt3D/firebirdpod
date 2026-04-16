import 'package:serverpod_database/serverpod_database.dart';

import '../runtime/fbclient/firebird_fbclient_native_client.dart';
import '../runtime/firebird_connection.dart';
import '../runtime/firebird_connection_options.dart';
import '../runtime/firebird_endpoint.dart';
import 'firebird_serverpod_config.dart';
import 'firebird_serverpod_value_encoder.dart';

/// First Firebird-backed Serverpod pool-manager scaffold.
class FirebirdServerpodPoolManager implements DatabasePoolManager {
  FirebirdServerpodPoolManager(
    this.serializationManager,
    this.runtimeParametersBuilder,
    this.config,
  );

  @override
  DatabaseDialect get dialect => DatabaseDialect.firebird;

  @override
  DateTime? lastDatabaseOperationTime;

  @override
  final SerializationManagerServer serializationManager;

  /// The runtime-parameter builder requested by Serverpod.
  final RuntimeParametersListBuilder? runtimeParametersBuilder;

  /// Firebird-specific Serverpod config.
  final FirebirdServerpodDatabaseConfig config;

  FirebirdEndpoint? _endpoint;

  /// The configured Firebird endpoint.
  FirebirdEndpoint get endpoint {
    final endpoint = _endpoint;
    if (endpoint == null) {
      throw StateError('Firebird pool manager has not been started.');
    }
    return endpoint;
  }

  @override
  FirebirdServerpodValueEncoder get encoder =>
      const FirebirdServerpodValueEncoder();

  @override
  void start() {
    _endpoint ??= FirebirdEndpoint(
      client: FirebirdFbClientNativeClient(
        fbClientLibraryPath: config.fbClientLibraryPath,
      ),
      options: FirebirdConnectionOptions(
        database: config.name,
        host: config.host,
        port: config.port,
        user: config.user,
        password: config.password,
        charset: config.charset,
        role: config.role,
        statementTimeout: config.defaultStatementTimeout,
      ),
    );
  }

  @override
  Future<void> stop() async {
    _endpoint = null;
  }

  @override
  Future<bool> testConnection() async {
    final connection = await connect();
    try {
      lastDatabaseOperationTime = DateTime.now();
      return true;
    } finally {
      await connection.close();
    }
  }

  /// Opens a Firebird attachment for the current pool-manager configuration.
  Future<FirebirdConnection> connect() async {
    start();
    return endpoint.connect();
  }
}
