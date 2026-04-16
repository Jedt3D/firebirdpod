import 'package:meta/meta.dart';

@immutable
class FirebirdConnectionOptions {
  const FirebirdConnectionOptions({
    required this.database,
    this.host,
    this.port,
    this.user = 'sysdba',
    this.password = 'masterkey',
    this.charset = 'UTF8',
    this.role,
    this.statementTimeout,
  });

  final String database;
  final String? host;
  final int? port;
  final String user;
  final String password;
  final String charset;
  final String? role;
  final Duration? statementTimeout;

  String get attachmentString {
    if (host == null || host!.isEmpty) return database;
    if (port == null) return '$host:$database';
    return '$host/$port:$database';
  }
}
