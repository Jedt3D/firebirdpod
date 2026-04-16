import 'package:fbdb/fbclient.dart' as fbclient;
import 'package:fbdb/fbdb.dart' as fbdb;

import 'firebird_database_exception.dart';

FirebirdDatabaseException mapFbStatusException({
  required fbclient.FbStatusException exception,
  required fbclient.IUtil util,
  required String operation,
}) {
  final message = util.formattedStatus(exception.status).trim();
  final statusVector = exception.status.errors.where((value) => value != 0).toList();
  return FirebirdDatabaseException(
    operation: operation,
    message: message.isEmpty ? 'Firebird $operation failed.' : message,
    sqlState: _extractSqlState(message),
    statusVector: statusVector,
    errorCodes: _extractErrorCodes(statusVector),
  );
}

FirebirdDatabaseException mapFbServerException({
  required fbdb.FbServerException exception,
  required String operation,
}) {
  final message = exception.message.trim();
  final statusVector = exception.errors.where((value) => value != 0).toList();
  return FirebirdDatabaseException(
    operation: operation,
    message: message.isEmpty ? 'Firebird $operation failed.' : message,
    sqlState: _extractSqlState(message),
    statusVector: statusVector,
    errorCodes: _extractErrorCodes(statusVector),
  );
}

String? _extractSqlState(String message) {
  final match = RegExp(r'SQLSTATE\s*=\s*([A-Z0-9]{5})').firstMatch(message);
  return match?.group(1);
}

List<int> _extractErrorCodes(List<int> statusVector) {
  final codes = <int>{};
  for (final value in statusVector) {
    if (value >= 335544321) {
      codes.add(value);
    }
  }
  return codes.toList(growable: false);
}
