import 'package:serverpod_database/serverpod_database.dart';
import 'package:serverpod_shared/serverpod_shared.dart';

/// Firebird-native select builder for the first generated Serverpod read slice.
///
/// Slice 02C intentionally stays within single-table read queries. Includes and
/// relation joins are handled in later slices once the baseline read path is
/// proven end to end.
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
  LockMode? _lockMode;
  LockBehavior? _lockBehavior;

  FirebirdSelectQueryBuilder withSelectFields(List<Column> fields) {
    if (fields.isEmpty) {
      throw ArgumentError.value(
        fields,
        'fields',
        'Cannot be empty.',
      );
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

  FirebirdSelectQueryBuilder withLockMode(
    LockMode? lockMode, [
    LockBehavior? lockBehavior,
  ]) {
    _lockMode = lockMode;
    _lockBehavior = lockBehavior;
    return this;
  }

  String build() {
    final select = _fields.map(_buildSelectField).join(', ');
    final buffer = StringBuffer(
      'SELECT $select FROM ${_renderTableIdentifier(_table.tableName)}',
    );

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
    return '${_renderColumn(column)} AS "${_escapeIdentifier(alias)}"';
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
        'Firebird only supports LockMode.forUpdate in the generated read path. '
        'Requested: $lockMode.',
      );
    }

    final lockBehavior = _lockBehavior ?? LockBehavior.wait;
    if (lockBehavior == LockBehavior.noWait) {
      throw UnsupportedError(
        'Firebird no-wait row locking is controlled by transaction parameters, '
        'not a SELECT clause. Use wait or skipLocked for Slice 02C.',
      );
    }

    final skipLocked = lockBehavior == LockBehavior.skipLocked
        ? ' SKIP LOCKED'
        : '';
    return 'FOR UPDATE WITH LOCK$skipLocked';
  }

  String _escapeIdentifier(String identifier) {
    return identifier.replaceAll('"', '""');
  }

  String _renderColumn(Column column) {
    return '${_renderTableIdentifier(column.table.queryPrefix)}.'
        '${_renderTableIdentifier(column.columnName)}';
  }

  String _renderExpression(Expression expression) {
    var sql = expression.toString();
    for (final column in _table.columns) {
      sql = sql.replaceAll(column.toString(), _renderColumn(column));
    }
    return sql;
  }

  String _renderTableIdentifier(String identifier) {
    return '"${_escapeIdentifier(identifier.toUpperCase())}"';
  }
}

/// Firebird-native count builder for the first generated read slice.
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
        'FROM ${_renderIdentifier(_table.tableName)}',
      );
      if (_where != null) {
        buffer.write(' WHERE ${_renderExpression(_where!)}');
      }
      return buffer.toString();
    }

    final inner = StringBuffer(
      'SELECT 1 AS ${_renderIdentifier('LIMITED_ROW')} '
      'FROM ${_renderIdentifier(_table.tableName)}',
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
    for (final column in _table.columns) {
      sql = sql.replaceAll(column.toString(), _renderColumn(column));
    }
    return sql;
  }

  String _renderColumn(Column column) {
    return '${_renderIdentifier(column.table.queryPrefix)}.'
        '${_renderIdentifier(column.columnName)}';
  }

  String _renderIdentifier(String identifier) {
    return '"${identifier.replaceAll('"', '""').toUpperCase()}"';
  }
}
