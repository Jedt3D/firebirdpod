import 'package:serverpod_database/serverpod_database.dart';

import 'firebird_serverpod_pool_manager.dart';

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
  }) async => _unsupported('find');

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
  }) async => _unsupported('findFirstRow');

  @override
  Future<T?> findById<T extends TableRow>(
    DatabaseSession session,
    Object id, {
    Transaction? transaction,
    Include? include,
    LockMode? lockMode,
    LockBehavior? lockBehavior,
  }) async => _unsupported('findById');

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
  }) async => _unsupported('count');

  @override
  Future<DatabaseResult> simpleQuery(
    DatabaseSession session,
    String query, {
    int? timeoutInSeconds,
    Transaction? transaction,
  }) async => _unsupported('simpleQuery');

  @override
  Future<DatabaseResult> query(
    DatabaseSession session,
    String query, {
    int? timeoutInSeconds,
    Transaction? transaction,
    QueryParameters? parameters,
  }) async => _unsupported('query');

  @override
  Future<int> execute(
    DatabaseSession session,
    String query, {
    int? timeoutInSeconds,
    Transaction? transaction,
    QueryParameters? parameters,
  }) async => _unsupported('execute');

  @override
  Future<int> simpleExecute(
    DatabaseSession session,
    String query, {
    int? timeoutInSeconds,
    Transaction? transaction,
  }) async => _unsupported('simpleExecute');

  @override
  Future<R> transaction<R>(
    TransactionFunction<R> transactionFunction, {
    required TransactionSettings settings,
    required DatabaseSession session,
  }) async => _unsupported('transaction');
}
