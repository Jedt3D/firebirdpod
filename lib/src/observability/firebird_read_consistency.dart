import 'package:meta/meta.dart';

import '../runtime/firebird_transaction.dart';

/// Reads transaction-level consistency state from Firebird system context and
/// monitoring tables.
class FirebirdReadConsistencyDiagnostics {
  const FirebirdReadConsistencyDiagnostics(this._transaction);

  final FirebirdTransaction _transaction;

  Future<FirebirdTransactionConsistencyState> captureState() async {
    final result = await _transaction.execute('''
      select
        current_connection as ATTACHMENT_ID,
        current_transaction as TRANSACTION_ID,
        rdb\$get_context('SYSTEM', 'ISOLATION_LEVEL') as ISOLATION_LEVEL,
        rdb\$get_context('SYSTEM', 'READ_ONLY') as READ_ONLY,
        tx.mon\$isolation_mode as MONITOR_ISOLATION_MODE,
        tx.mon\$lock_timeout as LOCK_TIMEOUT_SECONDS
      from mon\$transactions tx
      where tx.mon\$transaction_id = current_transaction
      ''');
    final row = result.singleRow;
    if (row == null) {
      throw StateError(
        'Firebird returned no transaction row for the current transaction.',
      );
    }

    return FirebirdTransactionConsistencyState(
      attachmentId: _requireInt(
        row['ATTACHMENT_ID'],
        fieldName: 'ATTACHMENT_ID',
      ),
      transactionId: _requireInt(
        row['TRANSACTION_ID'],
        fieldName: 'TRANSACTION_ID',
      ),
      isolationLevelName: _requireString(
        row['ISOLATION_LEVEL'],
        fieldName: 'ISOLATION_LEVEL',
      ),
      isReadOnly: _requireBool(row['READ_ONLY'], fieldName: 'READ_ONLY'),
      monitorIsolationMode: _asInt(row['MONITOR_ISOLATION_MODE']),
      lockTimeoutSeconds: _asInt(row['LOCK_TIMEOUT_SECONDS']),
    );
  }
}

extension FirebirdTransactionReadConsistency on FirebirdTransaction {
  FirebirdReadConsistencyDiagnostics get readConsistency =>
      FirebirdReadConsistencyDiagnostics(this);
}

@immutable
class FirebirdTransactionConsistencyState {
  const FirebirdTransactionConsistencyState({
    required this.attachmentId,
    required this.transactionId,
    required this.isolationLevelName,
    required this.isReadOnly,
    this.monitorIsolationMode,
    this.lockTimeoutSeconds,
  });

  final int attachmentId;
  final int transactionId;
  final String isolationLevelName;
  final bool isReadOnly;
  final int? monitorIsolationMode;
  final int? lockTimeoutSeconds;

  bool get usesReadConsistency => monitorIsolationMode == 4;

  String? get monitorIsolationModeName => switch (monitorIsolationMode) {
    0 => 'CONSISTENCY',
    1 => 'SNAPSHOT',
    2 => 'READ COMMITTED RECORD VERSION',
    3 => 'READ COMMITTED NO RECORD VERSION',
    4 => 'READ COMMITTED READ CONSISTENCY',
    _ => null,
  };
}

int _requireInt(Object? value, {required String fieldName}) {
  final intValue = _asInt(value);
  if (intValue == null) {
    throw StateError('Expected $fieldName to be an integer, got $value.');
  }
  return intValue;
}

int? _asInt(Object? value) {
  switch (value) {
    case null:
      return null;
    case int intValue:
      return intValue;
    case BigInt bigIntValue:
      return bigIntValue.toInt();
    case num numericValue:
      return numericValue.toInt();
    case String textValue:
      return int.tryParse(textValue.trim());
    default:
      return null;
  }
}

String _requireString(Object? value, {required String fieldName}) {
  final stringValue = value?.toString().trim();
  if (stringValue == null || stringValue.isEmpty) {
    throw StateError(
      'Expected $fieldName to be a non-empty string, got $value.',
    );
  }
  return stringValue;
}

bool _requireBool(Object? value, {required String fieldName}) {
  switch (value) {
    case bool boolValue:
      return boolValue;
    case num numericValue:
      return numericValue != 0;
    case String textValue:
      final normalized = textValue.trim().toUpperCase();
      if (normalized == 'TRUE') return true;
      if (normalized == 'FALSE') return false;
    default:
      break;
  }

  throw StateError('Expected $fieldName to be boolean-like, got $value.');
}
