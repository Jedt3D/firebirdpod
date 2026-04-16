import 'package:fbdb/fbclient.dart' as fbclient;
import 'package:meta/meta.dart';

@immutable
class FirebirdDatabaseException implements Exception {
  FirebirdDatabaseException({
    required this.operation,
    required this.message,
    this.sqlState,
    List<int> statusVector = const [],
    List<int> errorCodes = const [],
  }) : statusVector = List<int>.unmodifiable(statusVector),
       errorCodes = List<int>.unmodifiable(errorCodes);

  final String operation;
  final String message;
  final String? sqlState;
  final List<int> statusVector;
  final List<int> errorCodes;

  int? get primaryErrorCode => errorCodes.isEmpty ? null : errorCodes.first;

  bool hasErrorCode(int code) => errorCodes.contains(code);

  bool get isCancelled => hasErrorCode(fbclient.FbErrorCodes.isc_cancelled);

  bool get isTimeout =>
      hasErrorCode(fbclient.FbErrorCodes.isc_cfg_stmt_timeout) ||
      hasErrorCode(fbclient.FbErrorCodes.isc_att_stmt_timeout) ||
      hasErrorCode(fbclient.FbErrorCodes.isc_req_stmt_timeout);

  @override
  String toString() {
    final buffer = StringBuffer('FirebirdDatabaseException');
    buffer.write(' during $operation');
    if (sqlState case final state?) {
      buffer.write(' [SQLSTATE $state]');
    }
    if (primaryErrorCode case final code?) {
      buffer.write(' [code $code]');
    }
    buffer.write(': $message');
    return buffer.toString();
  }
}
