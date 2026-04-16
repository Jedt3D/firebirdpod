import 'package:serverpod_shared/serverpod_shared.dart';

/// Firebird-specific Serverpod database config used by the external dialect
/// registration seam.
class FirebirdServerpodDatabaseConfig extends DatabaseConfig {
  FirebirdServerpodDatabaseConfig({
    required super.host,
    required super.port,
    required super.user,
    required super.password,
    required super.name,
    this.charset = 'UTF8',
    this.role,
    this.fbClientLibraryPath,
    this.defaultStatementTimeout,
    super.maxConnectionCount,
  }) : super.internal(
         requireSsl: false,
         isUnixSocket: false,
         searchPaths: null,
         dialect: DatabaseDialect.firebird,
       );

  /// Firebird attachment charset.
  final String charset;

  /// Optional Firebird SQL role.
  final String? role;

  /// Optional explicit path to the Firebird client library.
  final String? fbClientLibraryPath;

  /// Default connection statement timeout.
  final Duration? defaultStatementTimeout;

  factory FirebirdServerpodDatabaseConfig.fromServerpodConfig(
    Map<dynamic, dynamic> dbSetup,
    Map<dynamic, dynamic> passwords,
    String name,
  ) {
    final host = dbSetup[ServerpodEnv.databaseHost.configKey];
    final port = dbSetup[ServerpodEnv.databasePort.configKey];
    final databaseName = dbSetup[ServerpodEnv.databaseName.configKey];
    final user = dbSetup[ServerpodEnv.databaseUser.configKey];

    if (host == null || host is! String || host.trim().isEmpty) {
      throw ArgumentError(
        'Invalid Firebird database configuration for "$name": missing host.',
      );
    }

    if (port == null || port is! int) {
      throw ArgumentError(
        'Invalid Firebird database configuration for "$name": missing port.',
      );
    }

    if (databaseName == null ||
        databaseName is! String ||
        databaseName.trim().isEmpty) {
      throw ArgumentError(
        'Invalid Firebird database configuration for "$name": missing '
        'database name.',
      );
    }

    if (user == null || user is! String || user.trim().isEmpty) {
      throw ArgumentError(
        'Invalid Firebird database configuration for "$name": missing user.',
      );
    }

    final password = passwords[ServerpodPassword.databasePassword.configKey];
    if (password == null) {
      throw PasswordMissingException(
        ServerpodPassword.databasePassword.configKey,
      );
    }

    int? maxConnectionCount =
        dbSetup[ServerpodEnv.databaseMaxConnectionCount.configKey] ??
        DatabaseConfig.defaultMaxConnectionCount;

    if (maxConnectionCount != null && maxConnectionCount < 1) {
      maxConnectionCount = null;
    }

    final charset = _readOptionalString(dbSetup, 'charset') ?? 'UTF8';
    final role = _readOptionalString(dbSetup, 'role');
    final fbClientLibraryPath = _readOptionalString(
      dbSetup,
      'fbClientLibraryPath',
    );
    final statementTimeoutSeconds = dbSetup['statementTimeoutInSeconds'];

    return FirebirdServerpodDatabaseConfig(
      host: host,
      port: port,
      user: user,
      password: password,
      name: databaseName,
      charset: charset,
      role: role,
      fbClientLibraryPath: fbClientLibraryPath,
      defaultStatementTimeout: statementTimeoutSeconds is int &&
              statementTimeoutSeconds > 0
          ? Duration(seconds: statementTimeoutSeconds)
          : null,
      maxConnectionCount: maxConnectionCount,
    );
  }

  static String? _readOptionalString(
    Map<dynamic, dynamic> source,
    String key,
  ) {
    final value = source[key];
    if (value == null || value is! String) return null;
    final trimmed = value.trim();
    return trimmed.isEmpty ? null : trimmed;
  }

  @override
  String toString() {
    var str = '';
    str += 'database host: $host\n';
    str += 'database port: $port\n';
    str += 'database name: $name\n';
    str += 'database user: $user\n';
    str += 'database charset: $charset\n';
    str += 'database role: ${role ?? '(none)'}\n';
    str += 'database max connection count: $maxConnectionCount\n';
    str += 'database dialect: ${dialect.name}\n';
    return str;
  }
}
