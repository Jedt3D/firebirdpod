import 'package:serverpod_database/serverpod_database.dart';
import 'package:serverpod_serialization/serverpod_serialization.dart';
import 'package:serverpod_shared/serverpod_shared.dart';

import '../runtime/firebird_execution_result.dart';
import '../runtime/firebird_transaction_settings.dart';
import '../sql/firebird_statement_parameters.dart';
import 'firebird_serverpod_database_result.dart';
import 'firebird_serverpod_pool_manager.dart';
import 'firebird_serverpod_select_builder.dart';
import 'firebird_serverpod_transaction.dart';

/// Serverpod database connection backed by `firebirdpod`.
///
/// The class now covers:
///
/// - Slice 02B raw query, execute, and transaction bridging
/// - Slice 02C generated single-table reads and counts
/// - Slice 02D generated single-table writes through Firebird `RETURNING`
class FirebirdServerpodDatabaseConnection
    extends DatabaseConnection<FirebirdServerpodPoolManager> {
  FirebirdServerpodDatabaseConnection(super.poolManager);

  @override
  Future<bool> testConnection() => poolManager.testConnection();

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
  }) async {
    final table = _getTableOrAssert<T>(operation: 'lockRows');

    _ensureValueEncoderConfigured();
    _assertSlice02CReadShapeSupported(
      table: table,
      where: where,
      orderBy: null,
      include: null,
    );

    final query = FirebirdSelectQueryBuilder(table: table)
        .withSelectFields([table.id])
        .withWhere(where)
        .withLockMode(lockMode, lockBehavior)
        .build();

    await this.query(
      session,
      query,
      timeoutInSeconds: 60,
      transaction: transaction,
    );
  }

  @override
  Future<List<T>> insert<T extends TableRow>(
    DatabaseSession session,
    List<T> rows, {
    Transaction? transaction,
    bool ignoreConflicts = false,
  }) async {
    if (rows.isEmpty) return <T>[];
    if (ignoreConflicts) {
      throw UnsupportedError(
        'Firebird generated insert(ignoreConflicts: true) is not implemented '
        'yet. Slice 02D keeps conflict-ignore behavior explicit instead of '
        'inventing a PostgreSQL-style compatibility shim.',
      );
    }

    if (rows.length > 1) {
      return _runInTransactionOrSavepoint(
        session,
        transaction,
        (tx) async => [
          for (final row in rows)
            await insert<T>(
              session,
              [row],
              transaction: tx,
              ignoreConflicts: false,
            ).then((results) => results.first),
        ],
      );
    }

    final row = rows.single;
    final table = row.table;
    final rowJson = Map<String, dynamic>.from(row.toJsonForDatabase() as Map);
    final statement = _buildInsertStatement(table, rowJson);
    final resolvedRows = await _queryResolvedRows(
      session,
      statement.sql,
      table: table,
      transaction: transaction,
      parameters: statement.parameters,
    );

    final merged = _mergeResultsWithNonPersistedFields(rows)(resolvedRows);
    return merged.map(poolManager.serializationManager.deserialize<T>).toList();
  }

  @override
  Future<T> insertRow<T extends TableRow>(
    DatabaseSession session,
    T row, {
    Transaction? transaction,
  }) async {
    final result = await insert<T>(
      session,
      [row],
      transaction: transaction,
    );

    if (result.length != 1) {
      throw StateError(
        'Failed to insert row, updated number of rows is ${result.length} != 1.',
      );
    }

    return result.first;
  }

  @override
  Future<List<T>> update<T extends TableRow>(
    DatabaseSession session,
    List<T> rows, {
    List<Column>? columns,
    Transaction? transaction,
  }) async {
    if (rows.isEmpty) return <T>[];
    if (rows.any((row) => row.id == null)) {
      throw ArgumentError.notNull('row.id');
    }

    if (rows.length > 1) {
      return _runInTransactionOrSavepoint(
        session,
        transaction,
        (tx) async => [
          for (final row in rows)
            await update<T>(
              session,
              [row],
              columns: columns,
              transaction: tx,
            ).then((results) => results.first),
        ],
      );
    }

    final row = rows.single;
    final table = row.table;
    final selectedColumns = (columns ?? table.managedColumns).toSet();
    if (columns != null) {
      _validateColumnsExists(selectedColumns, table.columns.toSet());
    }

    final rowJson = Map<String, dynamic>.from(row.toJsonForDatabase() as Map);
    final statement = _buildUpdateRowStatement(
      table,
      row.id!,
      rowJson,
      selectedColumns,
    );
    final resolvedRows = await _queryResolvedRows(
      session,
      statement.sql,
      table: table,
      transaction: transaction,
      parameters: statement.parameters,
    );

    final merged = _mergeResultsWithNonPersistedFields(rows)(resolvedRows);
    return merged.map(poolManager.serializationManager.deserialize<T>).toList();
  }

  @override
  Future<T> updateRow<T extends TableRow>(
    DatabaseSession session,
    T row, {
    List<Column>? columns,
    Transaction? transaction,
  }) async {
    final updated = await update<T>(
      session,
      [row],
      columns: columns,
      transaction: transaction,
    );

    if (updated.isEmpty) {
      throw StateError('Failed to update row, no rows updated.');
    }

    return updated.first;
  }

  @override
  Future<T> updateById<T extends TableRow>(
    DatabaseSession session,
    Object id, {
    required List<ColumnValue> columnValues,
    Transaction? transaction,
  }) async {
    final table = _getTableOrAssert<T>(operation: 'updateById');
    if (columnValues.isEmpty) {
      throw ArgumentError('columnValues cannot be empty');
    }

    _validateColumnsExists(
      columnValues.map((entry) => entry.column).toSet(),
      table.columns.toSet(),
    );

    final statement = _buildUpdateColumnValuesStatement(
      table,
      whereSql:
          '${_renderIdentifier(table.id.columnName)} = '
          '\$${columnValues.length + 1}',
      columnValues: columnValues,
      trailingParameters: [id],
    );
    final resolvedRows = await _queryResolvedRows(
      session,
      statement.sql,
      table: table,
      transaction: transaction,
      parameters: statement.parameters,
    );

    if (resolvedRows.isEmpty) {
      throw StateError('Failed to update row, no rows updated.');
    }

    return poolManager.serializationManager.deserialize<T>(resolvedRows.first);
  }

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
  }) async {
    final table = _getTableOrAssert<T>(operation: 'updateWhere');
    if (columnValues.isEmpty) {
      throw ArgumentError('columnValues cannot be empty');
    }

    final orderByCols = _resolveOrderBy(orderByList, orderBy, orderDescending);
    _ensureValueEncoderConfigured();
    _assertSlice02CReadShapeSupported(
      table: table,
      where: where,
      orderBy: orderByCols,
      include: null,
    );
    _validateColumnsExists(
      columnValues.map((entry) => entry.column).toSet(),
      table.columns.toSet(),
    );

    final requiresSelectedIds =
        limit != null || offset != null || orderByCols != null;

    if (!requiresSelectedIds) {
      final statement = _buildUpdateColumnValuesStatement(
        table,
        whereSql: _renderExpression(table, where),
        columnValues: columnValues,
      );
      final resolvedRows = await _queryResolvedRows(
        session,
        statement.sql,
        table: table,
        transaction: transaction,
        parameters: statement.parameters,
      );
      return resolvedRows
          .map(poolManager.serializationManager.deserialize<T>)
          .toList();
    }

    return _runInTransactionOrSavepoint(
      session,
      transaction,
      (tx) async {
        final ids = await _selectIdsForMutation(
          session,
          table,
          where: where,
          limit: limit,
          offset: offset,
          orderBy: orderByCols,
          transaction: tx,
        );
        if (ids.isEmpty) return <T>[];

        final statement = _buildUpdateColumnValuesStatement(
          table,
          whereSql: _buildIdInClause(table, ids.length, columnValues.length),
          columnValues: columnValues,
          trailingParameters: ids,
        );
        var resolvedRows = await _queryResolvedRows(
          session,
          statement.sql,
          table: table,
          transaction: tx,
          parameters: statement.parameters,
        );
        resolvedRows = _restoreSelectionOrder(
          resolvedRows,
          table,
          ids,
        );

        return resolvedRows
            .map(poolManager.serializationManager.deserialize<T>)
            .toList();
      },
    );
  }

  @override
  Future<List<T>> delete<T extends TableRow>(
    DatabaseSession session,
    List<T> rows, {
    Column? orderBy,
    List<Column>? orderByList,
    bool orderDescending = false,
    Transaction? transaction,
  }) async {
    if (rows.isEmpty) return <T>[];
    if (rows.any((row) => row.id == null)) {
      throw ArgumentError.notNull('row.id');
    }

    final table = rows.first.table;
    return deleteWhere<T>(
      session,
      table.id.inSet(rows.map((row) => row.id!).castToIdType().toSet()),
      orderBy: orderBy,
      orderByList: orderByList,
      orderDescending: orderDescending,
      transaction: transaction,
    );
  }

  @override
  Future<T> deleteRow<T extends TableRow>(
    DatabaseSession session,
    T row, {
    Transaction? transaction,
  }) async {
    final result = await delete<T>(
      session,
      [row],
      transaction: transaction,
    );

    if (result.isEmpty) {
      throw StateError('Failed to delete row, no rows deleted.');
    }

    return result.first;
  }

  @override
  Future<List<T>> deleteWhere<T extends TableRow>(
    DatabaseSession session,
    Expression where, {
    Column? orderBy,
    List<Column>? orderByList,
    bool orderDescending = false,
    Transaction? transaction,
  }) async {
    final table = _getTableOrAssert<T>(operation: 'deleteWhere');
    final orderByCols = _resolveOrderBy(orderByList, orderBy, orderDescending);

    _ensureValueEncoderConfigured();
    _assertSlice02CReadShapeSupported(
      table: table,
      where: where,
      orderBy: orderByCols,
      include: null,
    );

    final requiresSelectedIds =
        orderByCols != null;

    if (!requiresSelectedIds) {
      final resolvedRows = await _queryResolvedRows(
        session,
        'DELETE FROM ${_renderIdentifier(table.tableName)} '
        'WHERE ${_renderExpression(table, where)} RETURNING *',
        table: table,
        transaction: transaction,
      );
      return resolvedRows
          .map(poolManager.serializationManager.deserialize<T>)
          .toList();
    }

    return _runInTransactionOrSavepoint(
      session,
      transaction,
      (tx) async {
        final ids = await _selectIdsForMutation(
          session,
          table,
          where: where,
          orderBy: orderByCols,
          transaction: tx,
        );
        if (ids.isEmpty) return <T>[];

        final statement = _GeneratedStatement(
          sql:
              'DELETE FROM ${_renderIdentifier(table.tableName)} '
              'WHERE ${_buildIdInClause(table, ids.length, 0)} RETURNING *',
          parameters: QueryParameters.positional(ids),
        );
        var resolvedRows = await _queryResolvedRows(
          session,
          statement.sql,
          table: table,
          transaction: tx,
          parameters: statement.parameters,
        );
        resolvedRows = _restoreSelectionOrder(resolvedRows, table, ids);

        return resolvedRows
            .map(poolManager.serializationManager.deserialize<T>)
            .toList();
      },
    );
  }

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

  void _validateColumnsExists(Set<Column> columns, Set<Column> tableColumns) {
    final additionalColumns = columns.difference(tableColumns);
    if (additionalColumns.isNotEmpty) {
      throw ArgumentError.value(
        additionalColumns.toList().toString(),
        'columns',
        'Columns do not exist in table',
      );
    }
  }

  Future<R> _runInTransactionOrSavepoint<R>(
    DatabaseSession session,
    Transaction? transaction,
    Future<R> Function(Transaction transaction) action,
  ) async {
    final firebirdTransaction = _castToFirebirdTransaction(transaction);
    if (firebirdTransaction == null) {
      return this.transaction<R>(
        action,
        settings: const TransactionSettings(),
        session: session,
      );
    }

    Savepoint? savepoint;
    try {
      savepoint = await firebirdTransaction.createSavepoint();
      final result = await action(firebirdTransaction);
      await savepoint.release();
      return result;
    } catch (_) {
      await savepoint?.rollback();
      rethrow;
    }
  }

  Future<List<Map<String, dynamic>>> _queryResolvedRows(
    DatabaseSession session,
    String query, {
    required Table table,
    required Transaction? transaction,
    QueryParameters? parameters,
    int? timeoutInSeconds,
  }) async {
    final result = await this.query(
      session,
      query,
      timeoutInSeconds: timeoutInSeconds,
      transaction: transaction,
      parameters: parameters,
    );
    return result
        .map((row) => _resolveSingleTableRow(table, row.toColumnMap()))
        .toList();
  }

  _GeneratedStatement _buildInsertStatement(
    Table table,
    Map<String, dynamic> rowJson,
  ) {
    final columns = <String>[];
    final values = <String>[];
    final parameters = <Object?>[];

    for (final column in table.columns) {
      final rawValue = rowJson[column.columnName];
      final omitIdentityId =
          column.columnName == table.id.columnName &&
          rawValue == null &&
          column.hasDefault;
      if (omitIdentityId) continue;

      columns.add(_renderIdentifier(column.columnName));
      if (rawValue == null && column.hasDefault) {
        values.add('DEFAULT');
      } else {
        parameters.add(_normalizeMutationValue(column, rawValue));
        values.add('\$${parameters.length}');
      }
    }

    if (columns.isEmpty) {
      return _GeneratedStatement(
        sql:
            'INSERT INTO ${_renderIdentifier(table.tableName)} '
            'DEFAULT VALUES RETURNING *',
      );
    }

    return _GeneratedStatement(
      sql:
          'INSERT INTO ${_renderIdentifier(table.tableName)} '
          '(${columns.join(', ')}) VALUES (${values.join(', ')}) RETURNING *',
      parameters: QueryParameters.positional(parameters),
    );
  }

  _GeneratedStatement _buildUpdateRowStatement(
    Table table,
    Object id,
    Map<String, dynamic> rowJson,
    Set<Column> selectedColumns,
  ) {
    final assignments = <String>[];
    final parameters = <Object?>[];

    for (final column in selectedColumns) {
      if (column.columnName == table.id.columnName) continue;
      parameters.add(
        _normalizeMutationValue(column, rowJson[column.columnName]),
      );
      assignments.add(
        '${_renderIdentifier(column.columnName)} = \$${parameters.length}',
      );
    }

    if (assignments.isEmpty) {
      parameters.add(id);
      assignments.add(
        '${_renderIdentifier(table.id.columnName)} = \$${parameters.length}',
      );
    }

    parameters.add(id);
    return _GeneratedStatement(
      sql:
          'UPDATE ${_renderIdentifier(table.tableName)} '
          'SET ${assignments.join(', ')} '
          'WHERE ${_renderIdentifier(table.id.columnName)} = '
          '\$${parameters.length} RETURNING *',
      parameters: QueryParameters.positional(parameters),
    );
  }

  _GeneratedStatement _buildUpdateColumnValuesStatement(
    Table table, {
    required String whereSql,
    required List<ColumnValue> columnValues,
    List<Object?> trailingParameters = const <Object?>[],
  }) {
    final parameters = <Object?>[];
    final assignments = <String>[];

    for (final columnValue in columnValues) {
      parameters.add(
        _normalizeMutationValue(columnValue.column, columnValue.value),
      );
      assignments.add(
        '${_renderIdentifier(columnValue.column.columnName)} = '
        '\$${parameters.length}',
      );
    }

    parameters.addAll(trailingParameters);

    return _GeneratedStatement(
      sql:
          'UPDATE ${_renderIdentifier(table.tableName)} '
          'SET ${assignments.join(', ')} '
          'WHERE $whereSql RETURNING *',
      parameters: QueryParameters.positional(parameters),
    );
  }

  Future<List<Object>> _selectIdsForMutation(
    DatabaseSession session,
    Table table, {
    required Expression where,
    int? limit,
    int? offset,
    List<Order>? orderBy,
    required Transaction transaction,
  }) async {
    final query = FirebirdSelectQueryBuilder(table: table)
        .withSelectFields([table.id])
        .withWhere(where)
        .withOrderBy(orderBy)
        .withLimit(limit)
        .withOffset(offset)
        .build();

    final resolvedRows = await _queryResolvedRows(
      session,
      query,
      table: table,
      transaction: transaction,
      timeoutInSeconds: 60,
    );

    return resolvedRows
        .map((row) => row[table.id.fieldName])
        .whereType<Object>()
        .toList();
  }

  List<Map<String, dynamic>> _restoreSelectionOrder(
    List<Map<String, dynamic>> rows,
    Table table,
    List<Object> ids,
  ) {
    final orderById = <Object, int>{
      for (var index = 0; index < ids.length; index++) ids[index]: index,
    };

    final sortedRows = rows.toList();
    sortedRows.sort((a, b) {
      final aIndex = orderById[a[table.id.fieldName]] ?? ids.length;
      final bIndex = orderById[b[table.id.fieldName]] ?? ids.length;
      return aIndex.compareTo(bIndex);
    });
    return sortedRows;
  }

  String _buildIdInClause(Table table, int idCount, int parameterOffset) {
    final placeholders = List<String>.generate(
      idCount,
      (index) => '\$${parameterOffset + index + 1}',
    );
    return '${_renderIdentifier(table.id.columnName)} '
        'IN (${placeholders.join(', ')})';
  }

  List<Map<String, dynamic>> Function(Iterable<Map<String, dynamic>>)
  _mergeResultsWithNonPersistedFields<T extends TableRow>(List<T> rows) {
    return (Iterable<Map<String, dynamic>> dbResults) =>
        List<Map<String, dynamic>>.generate(dbResults.length, (index) {
          return {
            ...Map<String, dynamic>.from(rows[index].toJson() as Map),
            ...dbResults.elementAt(index),
          };
        });
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
      final directColumnKey = column.columnName;
      final directFieldKey = column.fieldName;
      final matchKey =
          rawRow.containsKey(alias)
              ? alias
              : rawRow.containsKey(directColumnKey)
              ? directColumnKey
              : rawRow.containsKey(directFieldKey)
              ? directFieldKey
              : caseInsensitiveKeys[alias.toLowerCase()] ??
                  caseInsensitiveKeys[directColumnKey.toLowerCase()] ??
                  caseInsensitiveKeys[directFieldKey.toLowerCase()];
      if (matchKey == null) continue;
      if (!rawRow.containsKey(matchKey)) continue;
      resolved[column.fieldName] = rawRow[matchKey];
    }

    return resolved;
  }

  String _renderIdentifier(String identifier) {
    return '"${identifier.replaceAll('"', '""').toUpperCase()}"';
  }

  Object? _normalizeMutationValue(Column column, Object? value) {
    if (value == null) return null;
    if (column is ColumnSerializable) {
      return SerializationManager.encode(value);
    }
    if (column is ColumnEnumExtended && value is Enum) {
      return switch (column.serialized) {
        EnumSerialization.byIndex => value.index,
        EnumSerialization.byName => value.name,
      };
    }
    if (value is Uri) return value.toString();
    if (value is BigInt) return value.toString();
    if (value is Enum) return value.name;
    return value;
  }

  String _renderColumn(Column column) {
    return '${_renderIdentifier(column.table.queryPrefix)}.'
        '${_renderIdentifier(column.columnName)}';
  }

  String _renderExpression(Table table, Expression expression) {
    var sql = expression.toString();
    for (final column in table.columns) {
      sql = sql.replaceAll(column.toString(), _renderColumn(column));
    }
    return sql;
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

class _GeneratedStatement {
  const _GeneratedStatement({
    required this.sql,
    this.parameters,
  });

  final String sql;
  final QueryParameters? parameters;
}
