import '../sql/firebird_query_compiler.dart';
import '../sql/firebird_statement_parameters.dart';
import 'firebird_cancel_mode.dart';
import 'firebird_execution_result.dart';
import 'firebird_native_client.dart';
import 'firebird_statement.dart';
import 'firebird_transaction.dart';
import 'firebird_transaction_settings.dart';

class FirebirdConnection {
  FirebirdConnection.internal(
    this._nativeConnection, {
    Duration? defaultStatementTimeout,
  }) : _defaultStatementTimeout = defaultStatementTimeout;

  final FirebirdNativeConnection _nativeConnection;
  final Duration? _defaultStatementTimeout;
  final Set<FirebirdStatement> _openStatements = <FirebirdStatement>{};
  final Set<FirebirdTransaction> _openTransactions = <FirebirdTransaction>{};

  bool _isClosed = false;

  bool get isClosed => _isClosed;

  Future<Duration?> getStatementTimeout() async {
    _ensureOpen();
    return _nativeConnection.getStatementTimeout();
  }

  Future<void> setStatementTimeout(Duration? timeout) async {
    _ensureOpen();
    await _nativeConnection.setStatementTimeout(timeout);
  }

  /// Requests cancellation through the native attachment.
  ///
  /// Applications should prefer statement timeouts for ordinary query budgets.
  /// True mid-flight async cancellation needs a separate control path beyond
  /// the calling isolate.
  Future<void> cancelCurrentOperation({
    FirebirdCancelMode mode = FirebirdCancelMode.raise,
  }) async {
    _ensureOpen();
    await _nativeConnection.cancelOperation(mode);
  }

  Future<FirebirdStatement> prepare(String query) async {
    _ensureOpen();

    final preparedSql = parseFirebirdSql(query);
    final nativeStatement = await _nativeConnection.prepareStatement(
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

  Future<FirebirdTransaction> beginTransaction({
    bool readOnly = false,
    FirebirdTransactionSettings settings = const FirebirdTransactionSettings(),
  }) async {
    _ensureOpen();

    final nativeTransaction = await _nativeConnection.beginTransaction(
      readOnly: readOnly,
      settings: settings,
    );
    final transaction = FirebirdTransaction.internal(
      nativeTransaction: nativeTransaction,
      readOnly: readOnly,
      settings: settings,
      onClose: _releaseTransaction,
    );
    _openTransactions.add(transaction);
    return transaction;
  }

  /// Restores this connection to a pool-safe baseline for the next request.
  ///
  /// This closes any still-open statements, rolls back and closes open explicit
  /// transactions, recreates the internal retained transaction, and reapplies
  /// the endpoint-configured default statement timeout.
  Future<void> resetForReuse() async {
    _ensureOpen();

    Object? firstError;
    StackTrace? firstStackTrace;

    void recordError(Object error, StackTrace stackTrace) {
      firstError ??= error;
      firstStackTrace ??= stackTrace;
    }

    for (final statement in _openStatements.toList()) {
      try {
        await statement.close();
      } catch (error, stackTrace) {
        recordError(error, stackTrace);
      }
    }

    for (final transaction in _openTransactions.toList()) {
      try {
        await transaction.close();
      } catch (error, stackTrace) {
        recordError(error, stackTrace);
      }
    }

    try {
      await _nativeConnection.resetRetainedState();
    } catch (error, stackTrace) {
      recordError(error, stackTrace);
    }

    try {
      await _nativeConnection.setStatementTimeout(_defaultStatementTimeout);
    } catch (error, stackTrace) {
      recordError(error, stackTrace);
    }

    if (firstError != null) {
      Error.throwWithStackTrace(firstError!, firstStackTrace!);
    }
  }

  Future<void> close() async {
    if (_isClosed) return;
    _isClosed = true;

    for (final statement in _openStatements.toList()) {
      await statement.close();
    }

    for (final transaction in _openTransactions.toList()) {
      await transaction.close();
    }

    await _nativeConnection.close();
  }

  Future<void> _releaseStatement(FirebirdStatement statement) async {
    _openStatements.remove(statement);
  }

  Future<void> _releaseTransaction(FirebirdTransaction transaction) async {
    _openTransactions.remove(transaction);
  }

  void _ensureOpen() {
    if (_isClosed) {
      throw StateError('The Firebird connection is already closed.');
    }
  }
}
