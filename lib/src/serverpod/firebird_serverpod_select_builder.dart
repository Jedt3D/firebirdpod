// ignore_for_file: implementation_imports

import 'package:serverpod_database/serverpod_database.dart';
import 'package:serverpod_database/src/concepts/table_relation.dart';
import 'package:serverpod_shared/serverpod_shared.dart';

/// Firebird-native select builder for the Serverpod runtime bridge.
///
/// The builder now covers:
///
/// - Slice 02C single-table reads and counts
/// - Slice 02E object includes and hidden auto-joins for relation filters
///   and ordering
/// - Slice 02E many-relation filtering and ordering through Firebird-safe
///   CTE subqueries
class FirebirdSelectQueryBuilder {
  FirebirdSelectQueryBuilder({required Table table})
    : _table = table,
      _fields = table.columns {
    if (_fields.isEmpty) {
      throw ArgumentError.value(
        table,
        'table',
        'Must have at least one column.',
      );
    }
  }

  final Table _table;

  List<Column> _fields;
  List<Order>? _orderBy;
  int? _limit;
  int? _offset;
  Expression? _where;
  Expression? _manyRelationWhereAddition;
  bool _forceGroupBy = false;
  ColumnExpression? _having;
  Include? _include;
  bool _joinOneLevelManyRelationWhereExpressions = false;
  bool _wrapWhereInNot = false;
  TableRelation? _countTableRelation;
  LockMode? _lockMode;
  LockBehavior? _lockBehavior;

  FirebirdSelectQueryBuilder withSelectFields(List<Column> fields) {
    if (fields.isEmpty) {
      throw ArgumentError.value(fields, 'fields', 'Cannot be empty.');
    }
    _fields = fields;
    return this;
  }

  FirebirdSelectQueryBuilder withOrderBy(List<Order>? orderBy) {
    _orderBy = orderBy;
    return this;
  }

  FirebirdSelectQueryBuilder withLimit(int? limit) {
    if (limit != null && limit < 0) {
      throw ArgumentError.value(limit, 'limit', 'Must be >= 0.');
    }
    _limit = limit;
    return this;
  }

  FirebirdSelectQueryBuilder withOffset(int? offset) {
    if (offset != null && offset < 0) {
      throw ArgumentError.value(offset, 'offset', 'Must be >= 0.');
    }
    _offset = offset;
    return this;
  }

  FirebirdSelectQueryBuilder withWhere(Expression? where) {
    _where = where;
    return this;
  }

  FirebirdSelectQueryBuilder withManyRelationWhereAddition(
    Expression? manyRelationWhereAddition,
  ) {
    _manyRelationWhereAddition = manyRelationWhereAddition;
    return this;
  }

  FirebirdSelectQueryBuilder withInclude(Include? include) {
    _include = include;
    return this;
  }

  FirebirdSelectQueryBuilder withHaving(ColumnExpression? having) {
    _having = having;
    return this;
  }

  FirebirdSelectQueryBuilder withCountTableRelation(TableRelation relation) {
    _countTableRelation = relation;
    return this;
  }

  FirebirdSelectQueryBuilder enableOneLevelWhereExpressionJoins() {
    _joinOneLevelManyRelationWhereExpressions = true;
    return this;
  }

  FirebirdSelectQueryBuilder forceGroupBy() {
    _forceGroupBy = true;
    return this;
  }

  FirebirdSelectQueryBuilder withLockMode(
    LockMode? lockMode, [
    LockBehavior? lockBehavior,
  ]) {
    _lockMode = lockMode;
    _lockBehavior = lockBehavior;
    return this;
  }

  FirebirdSelectQueryBuilder _wrapWhereInNotStatement() {
    _wrapWhereInNot = true;
    return this;
  }

  String build() {
    final selectColumns = [
      ..._fields,
      ..._gatherIncludeColumns(_include, _table),
    ];
    final subQueries = _FirebirdSubQueries.gatherSubQueries(
      orderBy: _orderBy,
      where: _where,
    );
    final select = _buildSelectStatement(
      selectColumns,
      countTableRelation: _countTableRelation,
    );
    final joins = _buildJoinQuery(
      where: _where,
      manyRelationWhereAddition: _manyRelationWhereAddition,
      having: _having,
      orderBy: _orderBy,
      include: _include,
      subQueries: subQueries,
      countTableRelation: _countTableRelation,
      joinOneLevelManyRelations: _joinOneLevelManyRelationWhereExpressions,
    );
    final groupBy = _buildGroupByQuery(
      selectColumns,
      having: _having,
      whereAddition: _manyRelationWhereAddition,
      countTableRelation: _countTableRelation,
      forceGroupBy: _forceGroupBy,
    );
    final where = _buildWhereQuery(
      where: _where,
      manyRelationWhereAddition: _manyRelationWhereAddition,
      subQueries: subQueries,
      wrapWhereInNot: _wrapWhereInNot,
    );
    final orderBy = _buildOrderByQuery(orderBy: _orderBy, subQueries: subQueries);

    final buffer = StringBuffer();
    if (subQueries != null) {
      buffer.write('WITH ${subQueries.buildQueries()} ');
    }
    buffer.write('SELECT $select');
    buffer.write(' FROM ${_renderTableReference(_table)}');
    if (joins != null) {
      buffer.write(' $joins');
    }
    if (where != null) {
      buffer.write(' WHERE $where');
    }
    if (groupBy != null) {
      buffer.write(' GROUP BY $groupBy');
    }
    if (_having != null) {
      buffer.write(
        ' HAVING ${_normalizeQualifiedColumnReferences(_having.toString())}',
      );
    }
    if (orderBy != null) {
      buffer.write(' ORDER BY $orderBy');
    }

    final pagination = _buildPaginationClause();
    if (pagination != null) {
      buffer.write(' $pagination');
    }

    final lockClause = _buildLockClause();
    if (lockClause != null) {
      buffer.write(' $lockClause');
    }

    return buffer.toString();
  }

  String _buildSelectField(Column column) {
    final alias = truncateIdentifier(
      column.fieldQueryAlias,
      DatabaseConstants.pgsqlMaxNameLimitation,
    );
    return '${_renderColumn(column)} AS ${_renderAlias(alias)}';
  }

  String _buildSelectStatement(
    List<Column> selectColumns, {
    TableRelation? countTableRelation,
  }) {
    final entries = <String>[
      for (final column in selectColumns) _buildSelectField(column),
    ];

    if (countTableRelation != null) {
      entries.add(
        'COUNT(${_normalizeQualifiedColumnReferences(countTableRelation.foreignFieldNameWithJoins)}) '
        'AS ${_renderAlias('count')}',
      );
    }

    return entries.join(', ');
  }

  String? _buildJoinQuery({
    Expression? where,
    Expression? manyRelationWhereAddition,
    ColumnExpression? having,
    List<Order>? orderBy,
    Include? include,
    _FirebirdSubQueries? subQueries,
    TableRelation? countTableRelation,
    bool joinOneLevelManyRelations = false,
  }) {
    final joins = <String, String>{};
    if (where != null) {
      joins.addAll(
        _gatherWhereJoins(
          where.columns,
          joinOneLevelManyRelations: joinOneLevelManyRelations,
        ),
      );
    }

    if (manyRelationWhereAddition != null) {
      joins.addAll(_gatherWhereAdditionJoins(manyRelationWhereAddition.columns));
    }

    if (orderBy != null) {
      joins.addAll(_gatherOrderByJoins(orderBy, subQueries: subQueries));
    }

    if (include != null) {
      joins.addAll(_gatherIncludeJoins(include));
    }

    if (countTableRelation != null) {
      joins[countTableRelation.relationQueryAlias] = _buildJoinStatement(
        tableRelation: countTableRelation,
      );
    }

    if (having != null) {
      final havingJoin = _buildHavingJoin(having);
      joins[havingJoin.key] = havingJoin.value;
    }

    if (joins.isEmpty) return null;
    return joins.values.join(' ');
  }

  String? _buildGroupByQuery(
    List<Column> selectFields, {
    Expression? having,
    Expression? whereAddition,
    TableRelation? countTableRelation,
    bool forceGroupBy = false,
  }) {
    if (countTableRelation == null &&
        having == null &&
        whereAddition == null &&
        !forceGroupBy) {
      return null;
    }

    return selectFields.map(_renderColumn).join(', ');
  }

  String? _buildWhereQuery({
    Expression? where,
    Expression? manyRelationWhereAddition,
    _FirebirdSubQueries? subQueries,
    bool wrapWhereInNot = false,
  }) {
    final buffer = StringBuffer();

    if (where != null) {
      if (wrapWhereInNot) {
        buffer.write('NOT ');
      }
      buffer.write(_resolveWhereQuery(where: where, subQueries: subQueries));
    }

    if (manyRelationWhereAddition != null) {
      if (buffer.length > 0) {
        buffer.write(manyRelationWhereAddition is EveryExpression ? ' OR ' : ' AND ');
      }
      buffer.write(
        _normalizeQualifiedColumnReferences(manyRelationWhereAddition.toString()),
      );
    }

    return buffer.length == 0 ? null : buffer.toString();
  }

  String _resolveWhereQuery({
    required Expression<dynamic> where,
    _FirebirdSubQueries? subQueries,
  }) {
    if (where is TwoPartExpression) {
      return '(${where.subExpressions.map((expression) => _resolveWhereQuery(where: expression, subQueries: subQueries)).join(' ${where.operator} ')})';
    }

    if (where is NotExpression) {
      return where.wrapExpression(
        _resolveWhereQuery(where: where.subExpression, subQueries: subQueries),
      );
    }

    if (where is ColumnExpression && where.isManyRelationExpression) {
      final tableRelation = where.column.table.tableRelation;
      if (tableRelation == null) {
        throw _createStateErrorWithMessage('Table relation is null');
      }

      final expressionIndex = where.index;
      if (expressionIndex == null) {
        throw _createStateErrorWithMessage('Expression index is null');
      }

      final subQuery = subQueries?._whereCountQueries[expressionIndex];
      if (subQuery == null) {
        throw _createStateErrorWithMessage(
          'Sub query for expression index \'$expressionIndex\' is null',
        );
      }

      final parentField = _normalizeQualifiedColumnReferences(
        tableRelation.fieldNameWithJoins,
      );
      final aliasRef =
          '${_renderAlias(subQuery.alias)}.${_renderAlias(tableRelation.fieldQueryAlias)}';

      if (where is NoneExpression || where is EveryExpression) {
        return '$parentField NOT IN (SELECT $aliasRef FROM ${_renderAlias(subQuery.alias)})';
      }
      return '$parentField IN (SELECT $aliasRef FROM ${_renderAlias(subQuery.alias)})';
    }

    return _normalizeQualifiedColumnReferences(where.toString());
  }

  String? _buildOrderByQuery({
    List<Order>? orderBy,
    _FirebirdSubQueries? subQueries,
  }) {
    if (orderBy == null || orderBy.isEmpty) return null;

    final parts = <String>[];
    for (var index = 0; index < orderBy.length; index++) {
      final order = orderBy[index];
      final column = order.column;
      if (column is ColumnCount) {
        final queryAlias = subQueries?._orderByQueries[index]?.alias;
        if (queryAlias == null) {
          throw _createStateErrorWithMessage(
            'Query alias for order-by sub query is null.',
          );
        }
        final direction = order.orderDescending
            ? 'DESC NULLS LAST'
            : 'ASC NULLS FIRST';
        parts.add(
          '${_renderAlias(queryAlias)}.${_renderAlias('count')} $direction',
        );
      } else {
        final direction = order.orderDescending
            ? 'DESC NULLS FIRST'
            : 'ASC NULLS LAST';
        parts.add('${_renderColumn(column)} $direction');
      }
    }
    return parts.join(', ');
  }

  String? _buildPaginationClause() {
    if (_limit == null && (_offset == null || _offset == 0)) {
      return null;
    }

    final clauses = <String>[];
    if (_offset != null && _offset! > 0) {
      clauses.add('OFFSET ${_offset!} ROWS');
    }
    if (_limit != null) {
      final keyword = (_offset != null && _offset! > 0) ? 'NEXT' : 'FIRST';
      clauses.add('FETCH $keyword ${_limit!} ROWS ONLY');
    }
    return clauses.join(' ');
  }

  String? _buildLockClause() {
    final lockMode = _lockMode;
    if (lockMode == null) return null;

    if (lockMode != LockMode.forUpdate) {
      throw UnsupportedError(
        'Firebird currently supports only LockMode.forUpdate in the generated '
        'read path. Requested: $lockMode.',
      );
    }

    final lockBehavior = _lockBehavior ?? LockBehavior.wait;
    if (lockBehavior == LockBehavior.noWait) {
      throw UnsupportedError(
        'Firebird no-wait row locking is controlled by transaction settings, '
        'not by a SELECT clause. Use wait or skipLocked on the Firebird path.',
      );
    }

    final skipLocked = lockBehavior == LockBehavior.skipLocked
        ? ' SKIP LOCKED'
        : '';
    return 'FOR UPDATE WITH LOCK$skipLocked';
  }

  Map<String, String> _gatherWhereJoins(
    List<Column> columns, {
    bool joinOneLevelManyRelations = false,
  }) {
    final joins = <String, String>{};
    final columnsWithRelations = columns.where(
      (column) => column.table.tableRelation != null,
    );

    for (final column in columnsWithRelations) {
      final tableRelation = column.table.tableRelation;
      if (tableRelation == null) continue;

      final manyRelationColumn = column is ColumnCount;
      final subRelations = tableRelation.getRelations;
      final oneLevelManyRelation =
          manyRelationColumn && subRelations.length == 1;
      final skipLast =
          manyRelationColumn &&
          !(oneLevelManyRelation && joinOneLevelManyRelations);
      final lastEntryIndex = subRelations.length - 1;

      for (var index = 0; index < subRelations.length; index++) {
        final subRelation = subRelations[index];
        final lastEntry = index == lastEntryIndex;
        if (lastEntry && skipLast) continue;

        joins[subRelation.relationQueryAlias] = _buildJoinStatement(
          tableRelation: subRelation,
        );
      }
    }

    return joins;
  }

  Map<String, String> _gatherWhereAdditionJoins(
    List<Column> columns,
  ) {
    final joins = <String, String>{};
    final columnsWithRelations = columns.where(
      (column) => column.table.tableRelation != null,
    );

    for (final column in columnsWithRelations) {
      final tableRelation = column.table.tableRelation;
      if (tableRelation == null) continue;

      final lastRelation = tableRelation.lastRelation;
      joins[lastRelation.relationQueryAlias] = _buildJoinStatement(
        tableRelation: lastRelation,
      );
    }

    return joins;
  }

  Map<String, String> _gatherOrderByJoins(
    List<Order> orderBy, {
    _FirebirdSubQueries? subQueries,
  }) {
    final joins = <String, String>{};
    for (var orderIndex = 0; orderIndex < orderBy.length; orderIndex++) {
      final order = orderBy[orderIndex];
      final column = order.column;
      final tableRelation = column.table.tableRelation;
      if (tableRelation == null) continue;

      final manyRelationColumn = column is ColumnCount;
      final subRelations = tableRelation.getRelations;
      final lastEntryIndex = subRelations.length - 1;

      for (var index = 0; index < subRelations.length; index++) {
        final subRelation = subRelations[index];
        final lastEntry = index == lastEntryIndex;

        if (lastEntry && manyRelationColumn) {
          final queryAlias = subQueries?._orderByQueries[orderIndex]?.alias;
          if (queryAlias == null) {
            throw _createStateErrorWithMessage(
              'Missing query alias for order-by sub query with index $orderIndex.',
            );
          }
          joins[queryAlias] = _buildSubQueryJoinStatement(
            tableRelation: tableRelation,
            queryAlias: queryAlias,
          );
        } else {
          joins[subRelation.relationQueryAlias] = _buildJoinStatement(
            tableRelation: subRelation,
          );
        }
      }
    }

    return joins;
  }

  Map<String, String> _gatherIncludeJoins(Include include) {
    final joins = <String, String>{};
    final includeTables = _gatherIncludeObjectTables(include, include.table);
    final tablesWithRelations = includeTables.where(
      (table) => table.tableRelation != null,
    );

    for (final table in tablesWithRelations) {
      final tableRelation = table.tableRelation;
      for (final subRelation in tableRelation?.getRelations ?? <TableRelation>[]) {
        joins[subRelation.relationQueryAlias] = _buildJoinStatement(
          tableRelation: subRelation,
        );
      }
    }

    return joins;
  }

  MapEntry<String, String> _buildHavingJoin(ColumnExpression having) {
    final tableRelation = having.column.table.tableRelation;
    if (tableRelation == null) {
      throw _createStateErrorWithMessage('Table relation is null.');
    }

    final lastRelation = tableRelation.lastRelation;
    return MapEntry(
      lastRelation.relationQueryAlias,
      _buildJoinStatement(tableRelation: lastRelation),
    );
  }

  String _buildSubQueryJoinStatement({
    required TableRelation tableRelation,
    required String queryAlias,
  }) {
    return 'LEFT JOIN ${_renderAlias(queryAlias)} ON '
        '${_normalizeQualifiedColumnReferences(tableRelation.fieldNameWithJoins)} = '
        '${_renderAlias(queryAlias)}.${_renderAlias(tableRelation.fieldQueryAlias)}';
  }

  String _buildJoinStatement({required TableRelation tableRelation}) {
    return 'LEFT JOIN ${_renderSchemaIdentifier(tableRelation.foreignTableName)} '
        'AS ${_renderAlias(tableRelation.relationQueryAlias)} ON '
        '${_normalizeQualifiedColumnReferences(tableRelation.fieldNameWithJoins)} = '
        '${_normalizeQualifiedColumnReferences(tableRelation.foreignFieldNameWithJoins)}';
  }

  String _renderColumn(Column column) {
    return '${_renderAlias(column.table.queryPrefix)}.'
        '${_renderSchemaIdentifier(column.columnName)}';
  }

  String _renderTableReference(Table table) {
    return '${_renderSchemaIdentifier(table.tableName)} '
        'AS ${_renderAlias(table.queryPrefix)}';
  }

  String _renderSchemaIdentifier(String identifier) {
    return '"${identifier.replaceAll('"', '""').toUpperCase()}"';
  }

  String _renderAlias(String identifier) {
    return '"${identifier.replaceAll('"', '""')}"';
  }
}

/// Firebird-native select builder for paged `IncludeList` relation queries.
///
/// This builder keeps per-parent pagination explicit by using a windowed
/// `ROW_NUMBER()` partitioned by the relation foreign key.
class FirebirdPerParentIncludeListQueryBuilder {
  FirebirdPerParentIncludeListQueryBuilder({
    required Table table,
    required Column partitionColumn,
  }) : _table = table,
       _partitionColumn = partitionColumn,
       _fields = table.columns {
    if (_fields.isEmpty) {
      throw ArgumentError.value(
        table,
        'table',
        'Must have at least one column.',
      );
    }
  }

  final Table _table;
  final Column _partitionColumn;

  List<Column> _fields;
  List<Order>? _orderBy;
  int? _limit;
  int? _offset;
  Expression? _where;
  Include? _include;

  static const _windowedQueryAlias = 'FIREBIRD_LIST_WINDOWED';
  static const _parentIdAlias = '__firebird_parent_id';
  static const _rowNumberAlias = '__firebird_row_number';

  FirebirdPerParentIncludeListQueryBuilder withSelectFields(
    List<Column> fields,
  ) {
    if (fields.isEmpty) {
      throw ArgumentError.value(fields, 'fields', 'Cannot be empty.');
    }
    _fields = fields;
    return this;
  }

  FirebirdPerParentIncludeListQueryBuilder withOrderBy(List<Order>? orderBy) {
    _orderBy = orderBy;
    return this;
  }

  FirebirdPerParentIncludeListQueryBuilder withLimit(int? limit) {
    if (limit != null && limit < 0) {
      throw ArgumentError.value(limit, 'limit', 'Must be >= 0.');
    }
    _limit = limit;
    return this;
  }

  FirebirdPerParentIncludeListQueryBuilder withOffset(int? offset) {
    if (offset != null && offset < 0) {
      throw ArgumentError.value(offset, 'offset', 'Must be >= 0.');
    }
    _offset = offset;
    return this;
  }

  FirebirdPerParentIncludeListQueryBuilder withWhere(Expression? where) {
    _where = where;
    return this;
  }

  FirebirdPerParentIncludeListQueryBuilder withInclude(Include? include) {
    _include = include;
    return this;
  }

  String build() {
    final selectColumns = [
      ..._fields,
      ..._gatherIncludeColumns(_include, _table),
    ];
    final helper = FirebirdSelectQueryBuilder(table: _table)
      ..withWhere(_where)
      ..withOrderBy(_orderBy)
      ..withInclude(_include);

    final subQueries = _FirebirdSubQueries.gatherSubQueries(
      orderBy: _orderBy,
      where: _where,
    );
    final joins = helper._buildJoinQuery(
      where: _where,
      orderBy: _orderBy,
      include: _include,
      subQueries: subQueries,
    );
    final where = helper._buildWhereQuery(where: _where, subQueries: subQueries);
    final ctes = <String>[
      if (subQueries != null) ...subQueries.buildQueryClauses(),
      '${helper._renderAlias(_windowedQueryAlias)} AS ('
          'SELECT ${_buildSelectList(helper, selectColumns, subQueries)} '
          'FROM ${helper._renderTableReference(_table)}'
          '${joins == null ? '' : ' $joins'}'
          '${where == null ? '' : ' WHERE $where'}'
          ')',
    ];

    return 'WITH ${ctes.join(', ')} '
        'SELECT * FROM ${helper._renderAlias(_windowedQueryAlias)} '
        'WHERE ${helper._renderAlias(_rowNumberAlias)} ${_buildRowLimitClause()} '
        'ORDER BY ${helper._renderAlias(_parentIdAlias)}, '
        '${helper._renderAlias(_rowNumberAlias)}';
  }

  String _buildSelectList(
    FirebirdSelectQueryBuilder helper,
    List<Column> selectColumns,
    _FirebirdSubQueries? subQueries,
  ) {
    final entries = <String>[
      for (final column in selectColumns) helper._buildSelectField(column),
      '${helper._renderColumn(_partitionColumn)} AS ${helper._renderAlias(_parentIdAlias)}',
      'ROW_NUMBER() OVER ('
          'PARTITION BY ${helper._renderColumn(_partitionColumn)}'
          '${_buildWindowOrderByClause(helper, subQueries)}'
          ') AS ${helper._renderAlias(_rowNumberAlias)}',
    ];
    return entries.join(', ');
  }

  String _buildWindowOrderByClause(
    FirebirdSelectQueryBuilder helper,
    _FirebirdSubQueries? subQueries,
  ) {
    final orderByEntries = _effectiveWindowOrderBy();
    if (orderByEntries.isEmpty) return '';

    final clauses = <String>[];
    for (var index = 0; index < orderByEntries.length; index++) {
      final order = orderByEntries[index];
      final column = order.column;
      if (column is ColumnCount) {
        final queryAlias = subQueries?._orderByQueries[index]?.alias;
        if (queryAlias == null) {
          throw _createStateErrorWithMessage(
            'Query alias for order-by sub query is null.',
          );
        }
        final direction = order.orderDescending
            ? 'DESC NULLS LAST'
            : 'ASC NULLS FIRST';
        clauses.add(
          '${helper._renderAlias(queryAlias)}.${helper._renderAlias('count')} $direction',
        );
      } else {
        final direction = order.orderDescending
            ? 'DESC NULLS FIRST'
            : 'ASC NULLS LAST';
        clauses.add('${helper._renderColumn(column)} $direction');
      }
    }

    return ' ORDER BY ${clauses.join(', ')}';
  }

  List<Order> _effectiveWindowOrderBy() {
    final orderBy = <Order>[...?_orderBy];
    final hasPrimaryKeyOrdering = orderBy.any(
      (entry) =>
          entry.column.table.queryPrefix == _table.id.table.queryPrefix &&
          entry.column.fieldName == _table.id.fieldName,
    );

    if (!hasPrimaryKeyOrdering) {
      orderBy.add(_table.id.asc());
    }

    return orderBy;
  }

  String _buildRowLimitClause() {
    final index = _offset ?? 0;
    final start = index + 1;

    if (_limit == null) {
      return '>= $start';
    }

    final end = index + _limit!;
    return 'BETWEEN $start AND $end';
  }
}

/// Firebird-native count builder for the generated read slice.
class FirebirdCountQueryBuilder {
  FirebirdCountQueryBuilder({required Table table}) : _table = table;

  final Table _table;

  String _alias = 'C';
  Expression? _where;
  int? _limit;

  FirebirdCountQueryBuilder withCountAlias(String alias) {
    _alias = alias;
    return this;
  }

  FirebirdCountQueryBuilder withWhere(Expression? where) {
    _where = where;
    return this;
  }

  FirebirdCountQueryBuilder withLimit(int? limit) {
    if (limit != null && limit < 0) {
      throw ArgumentError.value(limit, 'limit', 'Must be >= 0.');
    }
    _limit = limit;
    return this;
  }

  String build() {
    final sourceQuery = FirebirdSelectQueryBuilder(table: _table)
        .withSelectFields([_table.id])
        .withWhere(_where)
        .withLimit(_limit)
        .build();

    return 'SELECT COUNT(*) AS "${_alias.replaceAll('"', '""')}" '
        'FROM ($sourceQuery) ${_renderAlias('FIREBIRD_COUNT_SOURCE')}';
  }
}

class _FirebirdSubQuery {
  _FirebirdSubQuery(this.query, this.alias);

  final String query;
  final String alias;
}

class _FirebirdSubQueries {
  static const String orderByPrefix = 'order_by';
  static const String whereCountPrefix = 'where_count';
  static const String whereNonePrefix = 'where_none';
  static const String whereAnyPrefix = 'where_any';
  static const String whereEveryPrefix = 'where_every';

  final Map<int, _FirebirdSubQuery> _orderByQueries = {};
  final Map<int, _FirebirdSubQuery> _whereCountQueries = {};

  bool get isEmpty => _orderByQueries.isEmpty && _whereCountQueries.isEmpty;

  static _FirebirdSubQueries? gatherSubQueries({
    List<Order>? orderBy,
    Expression? where,
  }) {
    final subQueries = _FirebirdSubQueries();
    if (orderBy != null) {
      subQueries._orderByQueries.addAll(_gatherOrderBySubQueries(orderBy));
    }
    if (where != null) {
      subQueries._whereCountQueries.addAll(_gatherWhereSubQueries(where));
    }
    return subQueries.isEmpty ? null : subQueries;
  }

  static String buildUniqueQueryAlias(
    String prefix,
    String queryAlias,
    int index,
  ) {
    final alias = '${prefix}_${queryAlias}_$index';
    return truncateIdentifier(alias, DatabaseConstants.pgsqlMaxNameLimitation);
  }

  static Map<int, _FirebirdSubQuery> _gatherOrderBySubQueries(
    List<Order> orderBy,
  ) {
    final subQueries = <int, _FirebirdSubQuery>{};

    for (var index = 0; index < orderBy.length; index++) {
      final order = orderBy[index];
      final column = order.column;
      if (column is! ColumnCount) continue;

      final tableRelation = column.table.tableRelation;
      if (tableRelation == null) {
        throw _createStateErrorWithMessage('Table relation is null');
      }

      final relationQueryAlias = tableRelation.relationQueryAlias;
      final uniqueRelationQueryAlias = buildUniqueQueryAlias(
        orderByPrefix,
        relationQueryAlias,
        index,
      );

      final subQuery = FirebirdSelectQueryBuilder(table: tableRelation.fieldTable)
          .withWhere(column.innerWhere)
          .enableOneLevelWhereExpressionJoins()
          .withSelectFields([tableRelation.fieldColumn])
          .withCountTableRelation(tableRelation.lastRelation)
          .build();

      subQueries[index] = _FirebirdSubQuery(subQuery, uniqueRelationQueryAlias);
    }

    return subQueries;
  }

  static Map<int, _FirebirdSubQuery> _gatherWhereSubQueries(Expression where) {
    final subQueries = <int, _FirebirdSubQuery>{};

    where.forEachDepthFirstIndexed((index, expression) {
      if (expression is! ColumnExpression || !expression.isManyRelationExpression) {
        return;
      }

      final column = expression.column;
      if (column is! ColumnCount) return;

      final tableRelation = column.table.tableRelation;
      if (tableRelation == null) {
        throw _createStateErrorWithMessage('Table relation is null');
      }

      final relationQueryAlias = tableRelation.relationQueryAlias;
      expression.index = index;

      if (expression is NoneExpression) {
        subQueries[index] = _buildWhereNoneSubQuery(
          relationQueryAlias,
          index,
          tableRelation,
          column,
          expression,
        );
      } else if (expression is AnyExpression) {
        subQueries[index] = _buildWhereAnySubQuery(
          relationQueryAlias,
          index,
          tableRelation,
          column,
          expression,
        );
      } else if (expression is EveryExpression) {
        subQueries[index] = _buildWhereEverySubQuery(
          relationQueryAlias,
          index,
          tableRelation,
          column,
          expression,
        );
      } else {
        subQueries[index] = _buildWhereCountSubQuery(
          relationQueryAlias,
          index,
          tableRelation,
          column,
          expression,
        );
      }
    });

    return subQueries;
  }

  static _FirebirdSubQuery _buildWhereCountSubQuery(
    String relationQueryAlias,
    int index,
    TableRelation tableRelation,
    ColumnCount column,
    ColumnExpression<dynamic> expression,
  ) {
    final uniqueRelationQueryAlias = buildUniqueQueryAlias(
      whereCountPrefix,
      relationQueryAlias,
      index,
    );

    final subQuery = FirebirdSelectQueryBuilder(table: tableRelation.fieldTable)
        .withWhere(column.innerWhere)
        .withSelectFields([tableRelation.fieldColumn])
        .enableOneLevelWhereExpressionJoins()
        .withHaving(expression)
        .build();

    return _FirebirdSubQuery(subQuery, uniqueRelationQueryAlias);
  }

  static _FirebirdSubQuery _buildWhereNoneSubQuery(
    String relationQueryAlias,
    int index,
    TableRelation tableRelation,
    ColumnCount column,
    ColumnExpression<dynamic> expression,
  ) {
    final uniqueRelationQueryAlias = buildUniqueQueryAlias(
      whereNonePrefix,
      relationQueryAlias,
      index,
    );

    final subQuery = FirebirdSelectQueryBuilder(table: tableRelation.fieldTable)
        .withWhere(column.innerWhere)
        .withManyRelationWhereAddition(expression)
        .withSelectFields([tableRelation.fieldColumn])
        .enableOneLevelWhereExpressionJoins()
        .forceGroupBy()
        .build();

    return _FirebirdSubQuery(subQuery, uniqueRelationQueryAlias);
  }

  static _FirebirdSubQuery _buildWhereAnySubQuery(
    String relationQueryAlias,
    int index,
    TableRelation tableRelation,
    ColumnCount column,
    ColumnExpression<dynamic> expression,
  ) {
    final uniqueRelationQueryAlias = buildUniqueQueryAlias(
      whereAnyPrefix,
      relationQueryAlias,
      index,
    );

    final subQuery = FirebirdSelectQueryBuilder(table: tableRelation.fieldTable)
        .withWhere(column.innerWhere)
        .withManyRelationWhereAddition(expression)
        .withSelectFields([tableRelation.fieldColumn])
        .enableOneLevelWhereExpressionJoins()
        .forceGroupBy()
        .build();

    return _FirebirdSubQuery(subQuery, uniqueRelationQueryAlias);
  }

  static _FirebirdSubQuery _buildWhereEverySubQuery(
    String relationQueryAlias,
    int index,
    TableRelation tableRelation,
    ColumnCount column,
    ColumnExpression<dynamic> expression,
  ) {
    final uniqueRelationQueryAlias = buildUniqueQueryAlias(
      whereEveryPrefix,
      relationQueryAlias,
      index,
    );

    final subQuery = FirebirdSelectQueryBuilder(table: tableRelation.fieldTable)
        .withWhere(column.innerWhere)
        .withManyRelationWhereAddition(expression)
        .withSelectFields([tableRelation.fieldColumn])
        .enableOneLevelWhereExpressionJoins()
        ._wrapWhereInNotStatement()
        .forceGroupBy()
        .build();

    return _FirebirdSubQuery(subQuery, uniqueRelationQueryAlias);
  }

  String buildQueries() => buildQueryClauses().join(', ');

  List<String> buildQueryClauses() {
    return [
      ..._formatQueries(_orderByQueries),
      ..._formatQueries(_whereCountQueries),
    ];
  }

  List<String> _formatQueries(Map<int, _FirebirdSubQuery> subQueries) {
    final queries = <String>[];
    subQueries.forEach((_, subQuery) {
      queries.add('${_renderAlias(subQuery.alias)} AS (${subQuery.query})');
    });
    return queries;
  }
}

List<Column> _gatherIncludeColumns(Include? include, Table table) {
  if (include == null) return const [];

  final fields = <String, Column>{};
  for (final includeTable in _gatherIncludeObjectTables(include, table)) {
    for (final column in includeTable.columns) {
      fields['${includeTable.queryPrefix}.${column.columnName}'] = column;
    }
  }
  return fields.values.toList();
}

List<Table> _gatherIncludeObjectTables(Include? include, Table table) {
  final tables = <Table>[];
  if (include == null) return tables;

  include.includes.forEach((relationField, relationInclude) {
    if (relationInclude == null || relationInclude is IncludeList) {
      return;
    }

    final relationTable = table.getRelationTable(relationField);
    if (relationTable == null) return;

    tables.add(relationTable);
    tables.addAll(_gatherIncludeObjectTables(relationInclude, relationTable));
  });

  return tables;
}

String _normalizeQualifiedColumnReferences(String sql) {
  final referencePattern = RegExp(r'"([^"]+)"\."([^"]+)"');
  return sql.replaceAllMapped(referencePattern, (match) {
    final alias = match.group(1)!;
    final column = match.group(2)!;
    return '"$alias"."${column.toUpperCase()}"';
  });
}

String _renderAlias(String identifier) {
  return '"${identifier.replaceAll('"', '""')}"';
}

StateError _createStateErrorWithMessage(String message) {
  const stateErrorMessage =
      'This likely means that the code generator did not '
      'create the table relations correctly.';
  return StateError('$message - $stateErrorMessage');
}
