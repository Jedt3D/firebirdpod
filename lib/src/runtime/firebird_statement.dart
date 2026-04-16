import '../sql/firebird_prepared_sql.dart';
import '../sql/firebird_statement_parameters.dart';
import 'firebird_execution_result.dart';
import 'firebird_native_client.dart';

class FirebirdStatement {
  FirebirdStatement.internal({
    required this.preparedSql,
    required FirebirdNativeStatement nativeStatement,
    required Future<void> Function(FirebirdStatement statement) onClose,
  }) : _nativeStatement = nativeStatement,
       _onClose = onClose;

  final FirebirdPreparedSql preparedSql;
  final FirebirdNativeStatement _nativeStatement;
  final Future<void> Function(FirebirdStatement statement) _onClose;

  bool _isClosed = false;

  bool get isClosed => _isClosed;

  Future<Duration?> getTimeout() async {
    _ensureOpen();
    return _nativeStatement.getTimeout();
  }

  Future<void> setTimeout(Duration? timeout) async {
    _ensureOpen();
    await _nativeStatement.setTimeout(timeout);
  }

  Future<FirebirdExecutionResult> execute(
    FirebirdStatementParameters? parameters, {
    Duration? timeout,
  }
  ) async {
    _ensureOpen();
    final compiled = preparedSql.bind(parameters);
    if (timeout != null) {
      await _nativeStatement.setTimeout(timeout);
    }
    return _nativeStatement.execute(compiled.values);
  }

  Future<void> close() async {
    if (_isClosed) return;
    _isClosed = true;
    await _nativeStatement.close();
    await _onClose(this);
  }

  void _ensureOpen() {
    if (_isClosed) {
      throw StateError('The Firebird statement is already closed.');
    }
  }
}
