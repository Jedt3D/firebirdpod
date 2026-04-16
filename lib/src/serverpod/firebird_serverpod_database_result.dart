import 'dart:collection';

import 'package:serverpod_database/serverpod_database.dart';

import '../runtime/firebird_execution_result.dart';

class _FirebirdServerpodDatabaseResultSchemaColumn
    implements DatabaseResultSchemaColumn {
  _FirebirdServerpodDatabaseResultSchemaColumn({required this.columnName});

  @override
  final String? columnName;
}

class _FirebirdServerpodDatabaseResultSchema implements DatabaseResultSchema {
  _FirebirdServerpodDatabaseResultSchema(this._columnNames);

  final List<String> _columnNames;

  @override
  Iterable<DatabaseResultSchemaColumn> get columns => _columnNames.map(
    (columnName) =>
        _FirebirdServerpodDatabaseResultSchemaColumn(columnName: columnName),
  );
}

class _FirebirdServerpodDatabaseResultRow extends DatabaseResultRow {
  _FirebirdServerpodDatabaseResultRow(Map<String, Object?> values)
    : _columnMap = UnmodifiableMapView<String, dynamic>(
        Map<String, dynamic>.from(values),
      ),
      super(values.values.toList(growable: false));

  final Map<String, dynamic> _columnMap;

  @override
  Map<String, dynamic> toColumnMap() => _columnMap;
}

/// Serverpod-facing database result built from a `firebirdpod` execution
/// result.
class FirebirdServerpodDatabaseResult extends DatabaseResult {
  FirebirdServerpodDatabaseResult(this._result)
    : _columnNames = _resolveColumnNames(_result),
      super(
        _result.rows
            .map((row) => _FirebirdServerpodDatabaseResultRow(row))
            .toList(growable: false),
      );

  final FirebirdExecutionResult _result;
  final List<String> _columnNames;

  @override
  int get affectedRowCount => _result.affectedRows ?? _result.rows.length;

  @override
  DatabaseResultSchema get schema =>
      _FirebirdServerpodDatabaseResultSchema(_columnNames);

  static List<String> _resolveColumnNames(FirebirdExecutionResult result) {
    if (result.rows.isEmpty) return const [];
    return result.rows.first.keys.toList(growable: false);
  }
}
