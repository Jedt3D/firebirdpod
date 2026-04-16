import '../sql/firebird_query_compiler.dart';
import '../sql/firebird_statement_parameters.dart';
import 'firebird_execution_result.dart';
import 'firebird_native_client.dart';
import 'firebird_runtime_parameters.dart';
import 'firebird_savepoint.dart';
import 'firebird_statement.dart';
import 'firebird_transaction_settings.dart';

class FirebirdTransaction {
  FirebirdTransaction.internal({
    required FirebirdNativeTransaction nativeTransaction,
    required this.readOnly,
    required this.settings,
    required Future<void> Function(FirebirdTransaction transaction) onClose,
  }) : _nativeTransaction = nativeTransaction,
       _onClose = onClose;

  final FirebirdNativeTransaction _nativeTransaction;
  final Future<void> Function(FirebirdTransaction transaction) _onClose;
  final Set<FirebirdStatement> _openStatements = <FirebirdStatement>{};
  final Set<FirebirdSavepoint> _openSavepoints = <FirebirdSavepoint>{};

  final bool readOnly;
  final FirebirdTransactionSettings settings;
  final Map<String, Object> runtimeParameters = <String, Object>{};

  bool _isClosed = false;
  bool _isCommitted = false;
  bool _isRolledBack = false;
  int _savepointCounter = 0;

  bool get isClosed => _isClosed;
  bool get isCommitted => _isCommitted;
  bool get isRolledBack => _isRolledBack;

  Future<FirebirdStatement> prepare(String query) async {
    _ensureOpen();

    final preparedSql = parseFirebirdSql(query);
    final nativeStatement = await _nativeTransaction.prepareStatement(
      preparedSql.sql,
    );

    final statement = FirebirdStatement.internal(
      preparedSql: preparedSql,
      nativeStatement: nativeStatement,
      onClose: _releaseStatement,
    );
    _openStatements.add(statement);
    return statement;
  }

  Future<FirebirdExecutionResult> execute(
    String query, {
    FirebirdStatementParameters? parameters,
    Duration? timeout,
  }) async {
    final statement = await prepare(query);
    try {
      return await statement.execute(parameters, timeout: timeout);
    } finally {
      await statement.close();
    }
  }

  Future<FirebirdSavepoint> createSavepoint() async {
    _ensureOpen();

    final id = 'firebirdpod_sp_${++_savepointCounter}';
    final nativeSavepoint = await _nativeTransaction.createSavepoint(id);
    final savepoint = FirebirdSavepoint.internal(
      id: id,
      nativeSavepoint: nativeSavepoint,
      onRelease: _releaseSavepoint,
    );
    _openSavepoints.add(savepoint);
    return savepoint;
  }

  Future<void> setRuntimeParameters(
    FirebirdRuntimeParametersListBuilder builder,
  ) async {
    _ensureOpen();

    final parameters = builder(const FirebirdRuntimeParametersBuilder());
    for (final parameterGroup in parameters) {
      await parameterGroup.apply(_nativeTransaction);
      runtimeParameters.addAll(parameterGroup.options);
    }
  }

  Future<void> cancel() async {
    await rollback();
  }

  Future<void> commit() async {
    _ensureOpen();
    await _closeStatements();
    _openSavepoints.clear();
    await _nativeTransaction.commit();
    _isClosed = true;
    _isCommitted = true;
    await _onClose(this);
  }

  Future<void> rollback() async {
    if (_isClosed) return;
    await _closeStatements();
    _openSavepoints.clear();
    await _nativeTransaction.rollback();
    _isClosed = true;
    _isRolledBack = true;
    await _onClose(this);
  }

  Future<void> close() async {
    if (_isClosed) return;
    try {
      await rollback();
    } finally {
      await _nativeTransaction.close();
    }
  }

  Future<void> _closeStatements() async {
    for (final statement in _openStatements.toList()) {
      await statement.close();
    }
  }

  Future<void> _releaseStatement(FirebirdStatement statement) async {
    _openStatements.remove(statement);
  }

  Future<void> _releaseSavepoint(FirebirdSavepoint savepoint) async {
    _openSavepoints.remove(savepoint);
  }

  void _ensureOpen() {
    if (_isClosed) {
      throw StateError('The Firebird transaction is already closed.');
    }
  }
}
