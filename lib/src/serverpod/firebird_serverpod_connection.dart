import 'package:serverpod_database/serverpod_database.dart';
import 'package:serverpod_shared/serverpod_shared.dart';

import '../runtime/firebird_execution_result.dart';
import '../runtime/firebird_transaction_settings.dart';
import '../sql/firebird_statement_parameters.dart';
import 'firebird_serverpod_database_result.dart';
import 'firebird_serverpod_pool_manager.dart';
import 'firebird_serverpod_select_builder.dart';
import 'firebird_serverpod_transaction.dart';

/// Serverpod database-connection scaffold backed by `firebirdpod`.
///
/// Slice 02A intentionally stops at dialect registration and provider wiring.
/// The raw query bridge and CRUD implementation land in Slice 02B and later.
class FirebirdServerpodDatabaseConnection
    extends DatabaseConnection<FirebirdServerpodPoolManager> {
  FirebirdServerpodDatabaseConnection(super.poolManager);

  @override
  Future<bool> testConnection() => poolManager.testConnection();

  Never _unsupported(String operation) {
    throw UnsupportedError(
      'Firebird Serverpod operation "$operation" is not implemented yet. '
      'This belongs to Phase 02 Slice 02B or later.',
    );
  }

  @override
  Future<List<T>> find<T extends TableRow>(
    DatabaseSession session, {
    Expression? where,
    int? limit,
    int? offset,
    Column? orderBy,
    List<Column>? orderByList,
    bool orderDescending = false,
    Include? include,
    Transaction? transaction,
    LockMode? lockMode,
    LockBehavior? lockBehavior,
  }) async {
    if (limit != null && limit <= 0) return <T>[];

    final table = _getTableOrAssert<T>(operation: 'find');
    final orderByCols = _resolveOrderBy(orderByList, orderBy, orderDescending);

    _ensureValueEncoderConfigured();
    _assertSlice02CReadShapeSupported(
      table: table,
      where: where,
      orderBy: orderByCols,
      include: include,
    );

    final query = FirebirdSelectQueryBuilder(table: table)
        .withSelectFields(table.columns)
        .withWhere(where)
        .withOrderBy(orderByCols)
        .withLimit(limit)
        .withOffset(offset)
        .withLockMode(lockMode, lockBehavior)
        .build();

    final result = await this.query(
      session,
      query,
      timeoutInSeconds: 60,
      transaction: transaction,
    );

    return _deserializePrefixedRows<T>(
      table,
      result.map((row) => row.toColumnMap()),
    );
  }

  @override
  Future<T?> findFirstRow<T extends TableRow>(
    DatabaseSession session, {
    Expression? where,
    int? offset,
    Column? orderBy,
    List<Column>? orderByList,
    bool orderDescending = false,
    Transaction? transaction,
    Include? include,
    LockMode? lockMode,
    LockBehavior? lockBehavior,
  }) async {
    final rows = await find<T>(
      session,
      where: where,
      offset: offset,
      orderBy: orderBy,
      orderByList: orderByList,
      orderDescending: orderDescending,
      include: include,
      transaction: transaction,
      limit: 1,
      lockMode: lockMode,
      lockBehavior: lockBehavior,
    );

    if (rows.isEmpty) return null;
    return rows.first;
  }

  @override
  Future<T?> findById<T extends TableRow>(
    DatabaseSession session,
    Object id, {
    Transaction? transaction,
    Include? include,
    LockMode? lockMode,
    LockBehavior? lockBehavior,
  }) async {
    final table = _getTableOrAssert<T>(operation: 'findById');
    return findFirstRow<T>(
      session,
      where: table.id.equals(id),
      transaction: transaction,
      include: include,
      lockMode: lockMode,
      lockBehavior: lockBehavior,
    );
  }

  @override
  Future<void> lockRows<T extends TableRow>(
    DatabaseSession session, {
    required Expression where,
    required LockMode lockMode,
    required Transaction transaction,
    LockBehavior lockBehavior = LockBehavior.wait,
  }) async => _unsupported('lockRows');

  @override
  Future<List<T>> insert<T extends TableRow>(
    DatabaseSession session,
    List<T> rows, {
    Transaction? transaction,
    bool ignoreConflicts = false,
  }) async => _unsupported('insert');

  @override
  Future<T> insertRow<T extends TableRow>(
    DatabaseSession session,
    T row, {
    Transaction? transaction,
  }) async => _unsupported('insertRow');

  @override
  Future<List<T>> update<T extends TableRow>(
    DatabaseSession session,
    List<T> rows, {
    List<Column>? columns,
    Transaction? transaction,
  }) async => _unsupported('update');

  @override
  Future<T> updateRow<T extends TableRow>(
    DatabaseSession session,
    T row, {
    List<Column>? columns,
    Transaction? transaction,
  }) async => _unsupported('updateRow');

  @override
  Future<T> updateById<T extends TableRow>(
    DatabaseSession session,
    Object id, {
    required List<ColumnValue> columnValues,
    Transaction? transaction,
  }) async => _unsupported('updateById');

  @override
  Future<List<T>> updateWhere<T extends TableRow>(
    DatabaseSession session, {
    required List<ColumnValue> columnValues,
    required Expression where,
    int? limit,
    int? offset,
    Column? orderBy,
    List<Column>? orderByList,
    bool orderDescending = false,
    Transaction? transaction,
  }) async => _unsupported('updateWhere');

  @override
  Future<List<T>> delete<T extends TableRow>(
    DatabaseSession session,
    List<T> rows, {
    Column? orderBy,
    List<Column>? orderByList,
    bool orderDescending = false,
    Transaction? transaction,
  }) async => _unsupported('delete');

  @override
  Future<T> deleteRow<T extends TableRow>(
    DatabaseSession session,
    T row, {
    Transaction? transaction,
  }) async => _unsupported('deleteRow');

  @override
  Future<List<T>> deleteWhere<T extends TableRow>(
    DatabaseSession session,
    Expression where, {
    Column? orderBy,
    List<Column>? orderByList,
    bool orderDescending = false,
    Transaction? transaction,
  }) async => _unsupported('deleteWhere');

  @override
  Future<int> count<T extends TableRow>(
    DatabaseSession session, {
    Expression? where,
    int? limit,
    Transaction? transaction,
  }) async {
    if (limit != null && limit <= 0) return 0;

    final table = _getTableOrAssert<T>(operation: 'count');

    _ensureValueEncoderConfigured();
    _assertSlice02CReadShapeSupported(
      table: table,
      where: where,
      orderBy: null,
      include: null,
    );

    final query = FirebirdCountQueryBuilder(table: table)
        .withCountAlias('C')
        .withWhere(where)
        .withLimit(limit)
        .build();

    final result = await this.query(
      session,
      query,
      timeoutInSeconds: 60,
      transaction: transaction,
    );

    if (result.isEmpty) return 0;
    final value = result.first.isEmpty ? null : result.first.first;
    if (value is int) return value;
    if (value is num) return value.toInt();
    return 0;
  }

  @override
  Future<DatabaseResult> simpleQuery(
    DatabaseSession session,
    String query, {
    int? timeoutInSeconds,
    Transaction? transaction,
  }) async {
    return this.query(
      session,
      query,
      timeoutInSeconds: timeoutInSeconds,
      transaction: transaction,
    );
  }

  @override
  Future<DatabaseResult> query(
    DatabaseSession session,
    String query, {
    int? timeoutInSeconds,
    Transaction? transaction,
    QueryParameters? parameters,
  }) async {
    final stopwatch = Stopwatch()..start();

    try {
      final result = FirebirdServerpodDatabaseResult(
        await _executeRaw(
          session,
          query,
          timeoutInSeconds: timeoutInSeconds,
          transaction: transaction,
          parameters: parameters,
        ),
      );
      _logQuery(
        session,
        query,
        stopwatch,
        numRowsAffected: result.affectedRowCount,
      );
      return result;
    } catch (error, stackTrace) {
      _logQuery(
        session,
        query,
        stopwatch,
        exception: error,
        trace: stackTrace,
      );
      rethrow;
    }
  }

  @override
  Future<int> execute(
    DatabaseSession session,
    String query, {
    int? timeoutInSeconds,
    Transaction? transaction,
    QueryParameters? parameters,
  }) async {
    final stopwatch = Stopwatch()..start();

    try {
      final result = await _executeRaw(
        session,
        query,
        timeoutInSeconds: timeoutInSeconds,
        transaction: transaction,
        parameters: parameters,
      );
      final affectedRows = result.affectedRows ?? result.rows.length;
      _logQuery(
        session,
        query,
        stopwatch,
        numRowsAffected: affectedRows,
      );
      return affectedRows;
    } catch (error, stackTrace) {
      _logQuery(
        session,
        query,
        stopwatch,
        exception: error,
        trace: stackTrace,
      );
      rethrow;
    }
  }

  @override
  Future<int> simpleExecute(
    DatabaseSession session,
    String query, {
    int? timeoutInSeconds,
    Transaction? transaction,
  }) async {
    return execute(
      session,
      query,
      timeoutInSeconds: timeoutInSeconds,
      transaction: transaction,
    );
  }

  @override
  Future<R> transaction<R>(
    TransactionFunction<R> transactionFunction, {
    required TransactionSettings settings,
    required DatabaseSession session,
  }) async {
    final connection = await poolManager.connect();
    FirebirdServerpodTransaction? transaction;
    try {
      final nativeTransaction = await connection.beginTransaction(
        settings: _mapTransactionSettings(settings),
      );
      transaction = FirebirdServerpodTransaction(nativeTransaction);

      final result = await transactionFunction(transaction);
      if (!transaction.isCancelled) {
        await transaction.nativeTransaction.commit();
      }
      return result;
    } catch (_) {
      final nativeTransaction = transaction?.nativeTransaction;
      if (nativeTransaction != null && !nativeTransaction.isClosed) {
        await nativeTransaction.rollback();
      }
      rethrow;
    } finally {
      await connection.close();
    }
  }

  Future<FirebirdExecutionResult> _executeRaw(
    DatabaseSession session,
    String query, {
    required int? timeoutInSeconds,
    required Transaction? transaction,
    QueryParameters? parameters,
  }) async {
    final timeout = _resolveTimeout(timeoutInSeconds);
    final firebirdParameters = _convertParameters(parameters);
    final firebirdTransaction = _castToFirebirdTransaction(transaction);

    if (firebirdTransaction != null) {
      poolManager.lastDatabaseOperationTime = DateTime.now();
      return firebirdTransaction.nativeTransaction.execute(
        query,
        parameters: firebirdParameters,
        timeout: timeout,
      );
    }

    final connection = await poolManager.connect();
    try {
      poolManager.lastDatabaseOperationTime = DateTime.now();
      return await connection.execute(
        query,
        parameters: firebirdParameters,
        timeout: timeout,
      );
    } finally {
      await connection.close();
    }
  }

  static FirebirdServerpodTransaction? _castToFirebirdTransaction(
    Transaction? transaction,
  ) {
    if (transaction == null) return null;
    if (transaction is! FirebirdServerpodTransaction) {
      throw ArgumentError.value(
        transaction,
        'transaction',
        'Transaction type does not match the required Firebird transaction '
            'type. Create the transaction from session.db.transaction().',
      );
    }
    return transaction;
  }

  static FirebirdStatementParameters? _convertParameters(
    QueryParameters? parameters,
  ) {
    if (parameters == null) return null;

    return switch (parameters) {
      QueryParametersNamed(:final parameters) =>
        FirebirdStatementParameters.named(
          Map<String, Object?>.from(parameters),
        ),
      QueryParametersPositional(:final parameters) =>
        FirebirdStatementParameters.positional(
          List<Object?>.from(parameters),
        ),
      _ => throw UnsupportedError(
        'Unsupported QueryParameters implementation '
        '${parameters.runtimeType}.',
      ),
    };
  }

  static Duration? _resolveTimeout(int? timeoutInSeconds) {
    if (timeoutInSeconds == null || timeoutInSeconds <= 0) return null;
    return Duration(seconds: timeoutInSeconds);
  }

  void _ensureValueEncoderConfigured() {
    ValueEncoder.set(poolManager.encoder);
  }

  Table _getTableOrAssert<T>({required String operation}) {
    final table = poolManager.serializationManager.getTableForType(T);
    assert(table is Table, '''
You need to specify a template type that is a subclass of TableRow.
E.g. myRows = await session.db.$operation<MyTableClass>(where: ...);
Current type was $T''');
    return table!;
  }

  List<Order>? _resolveOrderBy(
    List<Column>? orderByList,
    Column<dynamic>? orderBy,
    bool orderDescending,
  ) {
    assert(orderByList == null || orderBy == null);
    if (orderBy != null) {
      if (orderBy is Order) return [orderBy];
      return [orderDescending ? orderBy.desc() : orderBy.asc()];
    }
    if (orderByList == null || orderByList.isEmpty) return null;
    return orderByList.asOrderBy();
  }

  void _assertSlice02CReadShapeSupported({
    required Table table,
    required Expression? where,
    required List<Order>? orderBy,
    required Include? include,
  }) {
    if (include != null) {
      throw UnsupportedError(
        'Firebird generated includes are not implemented yet. '
        'Slice 02C supports only single-table reads.',
      );
    }

    if (where != null && _referencesForeignTable(table, where.columns)) {
      throw UnsupportedError(
        'Firebird Slice 02C does not yet support relation-based WHERE '
        'expressions. Use single-table filters until include/join support lands.',
      );
    }

    if (orderBy != null &&
        _referencesForeignTable(
          table,
          orderBy.map((entry) => entry.column),
        )) {
      throw UnsupportedError(
        'Firebird Slice 02C does not yet support relation-based ORDER BY '
        'expressions. Use single-table ordering until include/join support lands.',
      );
    }
  }

  bool _referencesForeignTable(Table table, Iterable<Column> columns) {
    for (final column in columns) {
      if (column.table.tableName != table.tableName ||
          column.table.queryPrefix != table.queryPrefix) {
        return true;
      }
    }
    return false;
  }

  List<T> _deserializePrefixedRows<T extends TableRow>(
    Table table,
    Iterable<Map<String, dynamic>> rows,
  ) {
    return rows
        .map((rawRow) => _resolveSingleTableRow(table, rawRow))
        .map(poolManager.serializationManager.deserialize<T>)
        .toList();
  }

  Map<String, dynamic> _resolveSingleTableRow(
    Table table,
    Map<String, dynamic> rawRow,
  ) {
    final caseInsensitiveKeys = <String, String>{
      for (final key in rawRow.keys) key.toLowerCase(): key,
    };
    final resolved = <String, dynamic>{};

    for (final column in table.columns) {
      final alias = truncateIdentifier(
        column.fieldQueryAlias,
        DatabaseConstants.pgsqlMaxNameLimitation,
      );
      final matchKey =
          rawRow.containsKey(alias)
              ? alias
              : caseInsensitiveKeys[alias.toLowerCase()];
      if (matchKey == null) continue;
      if (!rawRow.containsKey(matchKey)) continue;
      resolved[column.fieldName] = rawRow[matchKey];
    }

    return resolved;
  }

  static FirebirdTransactionSettings _mapTransactionSettings(
    TransactionSettings settings,
  ) {
    return FirebirdTransactionSettings(
      isolationLevel: switch (settings.isolationLevel) {
        IsolationLevel.readUncommitted =>
          FirebirdTransactionIsolationLevel.readUncommitted,
        IsolationLevel.readCommitted ||
        null => FirebirdTransactionIsolationLevel.readCommitted,
        IsolationLevel.repeatableRead =>
          FirebirdTransactionIsolationLevel.repeatableRead,
        IsolationLevel.serializable =>
          FirebirdTransactionIsolationLevel.serializable,
      },
    );
  }

  static void _logQuery(
    DatabaseSession session,
    String query,
    Stopwatch stopwatch, {
    int? numRowsAffected,
    Object? exception,
    StackTrace? trace,
  }) {
    stopwatch.stop();
    trace ??= StackTrace.current;
    session.logQuery?.call(
      query: query,
      duration: stopwatch.elapsed,
      numRowsAffected: numRowsAffected,
      error: exception?.toString(),
      stackTrace: trace,
    );
  }
}
