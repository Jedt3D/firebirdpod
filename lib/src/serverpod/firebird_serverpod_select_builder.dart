import 'package:serverpod_database/serverpod_database.dart';
import 'package:serverpod_shared/serverpod_shared.dart';

/// Firebird-native select builder for the Serverpod runtime bridge.
///
/// The builder now covers:
///
/// - Slice 02C single-table reads and counts
/// - Slice 02E object-include joins for generated relation loading
///
/// List includes are still handled as follow-up queries in the connection layer.
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
  Include? _include;
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

  FirebirdSelectQueryBuilder withInclude(Include? include) {
    _include = include;
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

  String build() {
    final includeTables = _gatherIncludeObjectTables(_include, _table);
    final selectColumns = [
      ..._fields,
      ..._gatherIncludeColumns(_include, _table),
    ];
    final select = selectColumns.map(_buildSelectField).join(', ');
    final joins = _gatherIncludeJoinStatements(includeTables);
    final buffer = StringBuffer(
      'SELECT $select FROM ${_renderTableReference(_table)}',
    );

    if (joins.isNotEmpty) {
      buffer.write(' ${joins.values.join(' ')}');
    }

    if (_where != null) {
      buffer.write(' WHERE ${_renderExpression(_where!)}');
    }

    final orderBy = _buildOrderByClause();
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
    return '${_renderColumn(column)} AS "${_escapeAlias(alias)}"';
  }

  String? _buildOrderByClause() {
    final orderBy = _orderBy;
    if (orderBy == null || orderBy.isEmpty) return null;
    return orderBy
        .map(
          (entry) =>
              '${_renderColumn(entry.column)} ${entry.orderDescending ? 'DESC' : 'ASC'}',
        )
        .join(', ');
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

  Map<String, String> _gatherIncludeJoinStatements(List<Table> includeTables) {
    final joins = <String, String>{};
    if (includeTables.isEmpty) return joins;

    final tablesByQueryPrefix = <String, Table>{
      _table.queryPrefix: _table,
      for (final table in includeTables) table.queryPrefix: table,
    };

    for (final table in includeTables) {
      final relation = table.tableRelation;
      if (relation == null) continue;

      for (final subRelation in relation.getRelations) {
        final foreignTable =
            tablesByQueryPrefix[subRelation.relationQueryAlias];
        if (foreignTable == null) continue;
        joins[subRelation.relationQueryAlias] = _buildJoinStatement(
          tableRelation: subRelation,
          foreignTable: foreignTable,
        );
      }
    }

    return joins;
  }

  String _buildJoinStatement({
    required dynamic tableRelation,
    required Table foreignTable,
  }) {
    final foreignColumn = foreignTable.columns.firstWhere(
      (column) => column.fieldName == tableRelation.foreignFieldName,
      orElse: () {
        throw StateError(
          'Missing foreign column for relation ${tableRelation.relationQueryAlias}.',
        );
      },
    );

    return 'LEFT JOIN ${_renderTableReference(foreignTable)} ON '
        '${_renderColumn(tableRelation.fieldColumn)} = '
        '${_renderColumn(foreignColumn)}';
  }

  String _renderExpression(Expression expression) {
    var sql = expression.toString();
    for (final column in expression.columns) {
      sql = sql.replaceAll(column.toString(), _renderColumn(column));
    }
    return sql;
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
    return '"${_escapeAlias(identifier)}"';
  }

  String _escapeAlias(String identifier) {
    return identifier.replaceAll('"', '""');
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
    final joins = _gatherIncludeJoinStatements(
      _gatherIncludeObjectTables(_include, _table),
    );
    final buffer = StringBuffer(
      'WITH ${_renderAlias(_windowedQueryAlias)} AS ('
      'SELECT ${_buildSelectList(selectColumns)}'
      ' FROM ${_renderTableReference(_table)}',
    );

    if (joins.isNotEmpty) {
      buffer.write(' ${joins.values.join(' ')}');
    }

    if (_where != null) {
      buffer.write(' WHERE ${_renderExpression(_where!)}');
    }

    buffer.write(') SELECT * FROM ${_renderAlias(_windowedQueryAlias)}');
    buffer.write(
      ' WHERE ${_renderAlias(_rowNumberAlias)} ${_buildRowLimitClause()}',
    );
    buffer.write(
      ' ORDER BY ${_renderAlias(_parentIdAlias)}, ${_renderAlias(_rowNumberAlias)}',
    );

    return buffer.toString();
  }

  String _buildSelectList(List<Column> selectColumns) {
    final entries = <String>[
      for (final column in selectColumns) _buildSelectField(column),
      '${_renderColumn(_partitionColumn)} AS ${_renderAlias(_parentIdAlias)}',
      'ROW_NUMBER() OVER ('
          'PARTITION BY ${_renderColumn(_partitionColumn)}'
          '${_buildWindowOrderByClause()}'
          ') AS ${_renderAlias(_rowNumberAlias)}',
    ];
    return entries.join(', ');
  }

  String _buildSelectField(Column column) {
    final alias = truncateIdentifier(
      column.fieldQueryAlias,
      DatabaseConstants.pgsqlMaxNameLimitation,
    );
    return '${_renderColumn(column)} AS "${_escapeAlias(alias)}"';
  }

  String _buildWindowOrderByClause() {
    final orderByEntries = _effectiveWindowOrderBy();
    if (orderByEntries.isEmpty) return '';

    final sql = orderByEntries
        .map(
          (entry) =>
              '${_renderColumn(entry.column)} ${entry.orderDescending ? 'DESC' : 'ASC'}',
        )
        .join(', ');
    return ' ORDER BY $sql';
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

  Map<String, String> _gatherIncludeJoinStatements(List<Table> includeTables) {
    final joins = <String, String>{};
    if (includeTables.isEmpty) return joins;

    final tablesByQueryPrefix = <String, Table>{
      _table.queryPrefix: _table,
      for (final table in includeTables) table.queryPrefix: table,
    };

    for (final table in includeTables) {
      final relation = table.tableRelation;
      if (relation == null) continue;

      for (final subRelation in relation.getRelations) {
        final foreignTable =
            tablesByQueryPrefix[subRelation.relationQueryAlias];
        if (foreignTable == null) continue;
        joins[subRelation.relationQueryAlias] = _buildJoinStatement(
          tableRelation: subRelation,
          foreignTable: foreignTable,
        );
      }
    }

    return joins;
  }

  String _buildJoinStatement({
    required dynamic tableRelation,
    required Table foreignTable,
  }) {
    final foreignColumn = foreignTable.columns.firstWhere(
      (column) => column.fieldName == tableRelation.foreignFieldName,
      orElse: () {
        throw StateError(
          'Missing foreign column for relation ${tableRelation.relationQueryAlias}.',
        );
      },
    );

    return 'LEFT JOIN ${_renderTableReference(foreignTable)} ON '
        '${_renderColumn(tableRelation.fieldColumn)} = '
        '${_renderColumn(foreignColumn)}';
  }

  String _renderExpression(Expression expression) {
    var sql = expression.toString();
    for (final column in expression.columns) {
      sql = sql.replaceAll(column.toString(), _renderColumn(column));
    }
    return sql;
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
    return '"${_escapeAlias(identifier)}"';
  }

  String _escapeAlias(String identifier) {
    return identifier.replaceAll('"', '""');
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
    final alias = _alias.replaceAll('"', '""');

    if (_limit == null) {
      final buffer = StringBuffer(
        'SELECT COUNT(*) AS "$alias" '
        'FROM ${_renderTableReference(_table)}',
      );
      if (_where != null) {
        buffer.write(' WHERE ${_renderExpression(_where!)}');
      }
      return buffer.toString();
    }

    final inner = StringBuffer(
      'SELECT 1 AS ${_renderSchemaIdentifier('LIMITED_ROW')} '
      'FROM ${_renderTableReference(_table)}',
    );
    if (_where != null) {
      inner.write(' WHERE ${_renderExpression(_where!)}');
    }
    inner.write(' FETCH FIRST ${_limit!} ROWS ONLY');

    return 'SELECT COUNT(*) AS "$alias" '
        'FROM (${inner.toString()}) "FIREBIRD_COUNT_SOURCE"';
  }

  String _renderExpression(Expression expression) {
    var sql = expression.toString();
    for (final column in expression.columns) {
      sql = sql.replaceAll(column.toString(), _renderColumn(column));
    }
    return sql;
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
