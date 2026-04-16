import 'package:fbdb/fbdb.dart' as fbdb;

import '../firebird_cancel_mode.dart';
import '../firebird_connection_options.dart';
import '../firebird_error_mapper.dart';
import '../firebird_execution_result.dart';
import '../firebird_native_client.dart';
import '../firebird_transaction_settings.dart';

/// Prototype transport that adapts the local `fbdb` package to the
/// `FirebirdNativeClient` seam.
///
/// This is intentionally a narrow bridge for validating lifecycle design with a
/// real `fbclient`-backed attachment before we commit to a broader transport
/// implementation.
class FirebirdFbdbPrototypeClient implements FirebirdNativeClient {
  const FirebirdFbdbPrototypeClient({this.fbClientLibraryPath});

  final String? fbClientLibraryPath;

  @override
  Future<FirebirdNativeConnection> attach(
    FirebirdConnectionOptions options,
  ) async {
    try {
      final db = await fbdb.FbDb.attach(
        host: options.host,
        port: options.port,
        database: options.database,
        user: options.user,
        password: options.password,
        role: options.role,
        options: fbClientLibraryPath == null
            ? null
            : fbdb.FbOptions(libFbClient: fbClientLibraryPath),
      );
      return _FirebirdFbdbPrototypeConnection(db);
    } on fbdb.FbServerException catch (error) {
      throw mapFbServerException(exception: error, operation: 'attach');
    }
  }
}

class _FirebirdFbdbPrototypeConnection implements FirebirdNativeConnection {
  _FirebirdFbdbPrototypeConnection(this._db);

  final fbdb.FbDb _db;
  bool _isClosed = false;
  Duration? _statementTimeout;

  @override
  Future<void> cancelOperation(FirebirdCancelMode mode) async {
    throw UnsupportedError(
      'The fbdb prototype client does not support attachment cancellation.',
    );
  }

  @override
  Future<void> close() async {
    if (_isClosed) return;
    _isClosed = true;
    await _db.detach();
  }

  @override
  Future<FirebirdNativeStatement> prepareStatement(String sql) async {
    _ensureOpen();
    try {
      final query = _db.query();
      await query.prepare(sql: sql);
      return _FirebirdFbdbPrototypeStatement(
        sql: sql,
        query: query,
        timeout: _statementTimeout,
      );
    } on fbdb.FbServerException catch (error) {
      throw mapFbServerException(exception: error, operation: 'prepare');
    }
  }

  @override
  Future<FirebirdNativeTransaction> beginTransaction({
    bool readOnly = false,
    FirebirdTransactionSettings settings = const FirebirdTransactionSettings(),
  }) async {
    _ensureOpen();
    try {
      final transaction = await _db.newTransaction(
        flags: _mapTransactionFlags(
          readOnly: readOnly,
          settings: settings,
        ),
      );
      return _FirebirdFbdbPrototypeTransaction(_db, transaction);
    } on fbdb.FbServerException catch (error) {
      throw mapFbServerException(
        exception: error,
        operation: 'begin transaction',
      );
    }
  }

  @override
  Future<void> resetRetainedState() async {
    _ensureOpen();
  }

  @override
  Future<Duration?> getStatementTimeout() async => _statementTimeout;

  @override
  Future<void> setStatementTimeout(Duration? timeout) async {
    _statementTimeout = timeout;
  }

  void _ensureOpen() {
    if (_isClosed) {
      throw StateError('The fbdb prototype connection is already closed.');
    }
  }
}

class _FirebirdFbdbPrototypeStatement implements FirebirdNativeStatement {
  _FirebirdFbdbPrototypeStatement({
    required String sql,
    required fbdb.FbQuery query,
    fbdb.FbTransaction? transaction,
    Duration? timeout,
  }) : _query = query,
       _transaction = transaction,
       _timeout = timeout,
       _mode = _classify(sql);

  final fbdb.FbQuery _query;
  final fbdb.FbTransaction? _transaction;
  final _FirebirdFbdbExecutionMode _mode;
  Duration? _timeout;
  bool _isClosed = false;

  @override
  Future<void> close() async {
    if (_isClosed) return;
    _isClosed = true;
    await _query.close();
  }

  @override
  Future<Duration?> getTimeout() async => _timeout;

  @override
  Future<void> setTimeout(Duration? timeout) async {
    _timeout = timeout;
  }

  @override
  Future<FirebirdExecutionResult> execute(List<Object?> values) async {
    _ensureOpen();

    try {
      return switch (_mode) {
        _FirebirdFbdbExecutionMode.cursor => _executeCursor(values),
        _FirebirdFbdbExecutionMode.execute => _executeNonCursor(values),
      };
    } on fbdb.FbServerException catch (error) {
      throw mapFbServerException(exception: error, operation: 'execute');
    }
  }

  Future<FirebirdExecutionResult> _executeCursor(List<Object?> values) async {
    await _query.openPrepared(parameters: values, inTransaction: _transaction);
    final rows = await _query.fetchAllAsMaps();
    return FirebirdExecutionResult(
      rows: rows.map((row) => Map<String, Object?>.from(row)).toList(),
    );
  }

  Future<FirebirdExecutionResult> _executeNonCursor(
    List<Object?> values,
  ) async {
    await _query.executePrepared(
      parameters: values,
      inTransaction: _transaction,
    );
    final affectedRows = await _query.affectedRows();

    final output = await _safeOutputMap();
    return FirebirdExecutionResult(
      affectedRows: affectedRows,
      rows: output == null ? const [] : [output],
    );
  }

  Future<Map<String, Object?>?> _safeOutputMap() async {
    try {
      final output = await _query.getOutputAsMap();
      if (output.isEmpty) return null;
      return Map<String, Object?>.from(output);
    } catch (_) {
      return null;
    }
  }

  void _ensureOpen() {
    if (_isClosed) {
      throw StateError('The fbdb prototype statement is already closed.');
    }
  }
}

class _FirebirdFbdbPrototypeTransaction implements FirebirdNativeTransaction {
  _FirebirdFbdbPrototypeTransaction(this._db, this._transaction);

  final fbdb.FbDb _db;
  final fbdb.FbTransaction _transaction;
  bool _isClosed = false;

  @override
  Future<void> close() async {
    if (_isClosed) return;
    if (await _transaction.isActive()) {
      await _transaction.rollback();
    }
    _isClosed = true;
  }

  @override
  Future<void> commit() async {
    _ensureOpen();
    await _transaction.commit();
    _isClosed = true;
  }

  @override
  Future<FirebirdNativeSavepoint> createSavepoint(String id) async {
    _ensureOpen();
    final normalizedId = _requireSqlIdentifier(id, context: 'savepoint id');
    await _executeControlStatement('SAVEPOINT $normalizedId');
    return _FirebirdFbdbPrototypeSavepoint(
      transaction: this,
      id: normalizedId,
    );
  }

  @override
  Future<FirebirdNativeStatement> prepareStatement(String sql) async {
    _ensureOpen();
    try {
      final query = _db.query();
      await query.prepare(sql: sql, inTransaction: _transaction);
      return _FirebirdFbdbPrototypeStatement(
        sql: sql,
        query: query,
        transaction: _transaction,
      );
    } on fbdb.FbServerException catch (error) {
      throw mapFbServerException(exception: error, operation: 'prepare');
    }
  }

  @override
  Future<void> setTransactionContext(String key, Object value) async {
    _ensureOpen();
    final normalizedKey = key.trim();
    if (normalizedKey.isEmpty) {
      throw ArgumentError.value(
        key,
        'key',
        'Transaction context keys must not be empty.',
      );
    }

    await _executeControlStatement(
      '''
      select rdb\$set_context('USER_TRANSACTION', ?, ?) as APPLIED
      from rdb\$database
      ''',
      values: [normalizedKey, value.toString()],
    );
  }

  Future<void> _executeControlStatement(
    String sql, {
    List<Object?> values = const [],
  }) async {
    final statement = await prepareStatement(sql);
    try {
      await statement.execute(values);
    } finally {
      await statement.close();
    }
  }

  @override
  Future<void> rollback() async {
    if (_isClosed) return;
    await _transaction.rollback();
    _isClosed = true;
  }

  void _ensureOpen() {
    if (_isClosed) {
      throw StateError('The fbdb prototype transaction is already closed.');
    }
  }
}

class _FirebirdFbdbPrototypeSavepoint implements FirebirdNativeSavepoint {
  _FirebirdFbdbPrototypeSavepoint({
    required _FirebirdFbdbPrototypeTransaction transaction,
    required this.id,
  }) : _transaction = transaction;

  final _FirebirdFbdbPrototypeTransaction _transaction;

  @override
  final String id;

  bool _isClosed = false;

  @override
  Future<void> release() async {
    if (_isClosed) return;
    await _transaction._executeControlStatement('RELEASE SAVEPOINT $id ONLY');
    _isClosed = true;
  }

  @override
  Future<void> rollback() async {
    if (_isClosed) return;
    await _transaction._executeControlStatement('ROLLBACK TO SAVEPOINT $id');
  }
}

enum _FirebirdFbdbExecutionMode { cursor, execute }

_FirebirdFbdbExecutionMode _classify(String sql) {
  final normalized = sql.trimLeft().toLowerCase();
  if (normalized.startsWith('select ') || normalized.startsWith('with ')) {
    return _FirebirdFbdbExecutionMode.cursor;
  }
  return _FirebirdFbdbExecutionMode.execute;
}

Set<fbdb.FbTrFlag> _mapTransactionFlags({
  required bool readOnly,
  required FirebirdTransactionSettings settings,
}) {
  final flags = <fbdb.FbTrFlag>{
    fbdb.FbTrFlag.wait,
    readOnly ? fbdb.FbTrFlag.read : fbdb.FbTrFlag.write,
  };

  switch (settings.isolationLevel) {
    case FirebirdTransactionIsolationLevel.readUncommitted:
    case FirebirdTransactionIsolationLevel.readCommitted:
      flags.addAll(<fbdb.FbTrFlag>{
        fbdb.FbTrFlag.readCommitted,
        fbdb.FbTrFlag.noRecVersion,
      });
      break;
    case FirebirdTransactionIsolationLevel.repeatableRead:
      flags.add(fbdb.FbTrFlag.concurrency);
      break;
    case FirebirdTransactionIsolationLevel.serializable:
      flags.add(fbdb.FbTrFlag.consistency);
      break;
  }

  return flags;
}

String _requireSqlIdentifier(String value, {required String context}) {
  final normalized = value.trim();
  final pattern = RegExp(r'^[A-Za-z_][A-Za-z0-9_]*$');
  if (!pattern.hasMatch(normalized)) {
    throw ArgumentError.value(
      value,
      context,
      'Only simple SQL identifiers are supported here.',
    );
  }
  return normalized;
}
