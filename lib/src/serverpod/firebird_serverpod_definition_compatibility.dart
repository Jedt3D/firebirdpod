import 'package:serverpod_database/serverpod_database.dart';

/// Adapts Serverpod definitions to the current Firebird compatibility surface.
///
/// Firebird does not currently expose a compatible Serverpod-style UUID
/// default expression for schema round-tripping in this adapter, so UUID
/// database defaults are normalized away at the Firebird boundary and the
/// Firebird write path generates UUIDs when a model insert leaves them null.
class FirebirdCompatibilitySerializationManager
    extends SerializationManagerServer {
  FirebirdCompatibilitySerializationManager(this._delegate);

  final SerializationManagerServer _delegate;

  late final List<TableDefinition> _targetTableDefinitions =
      firebirdCompatibleTargetTableDefinitions(
        _delegate.getTargetTableDefinitions(),
      );

  @override
  T deserialize<T>(dynamic data, [Type? t]) =>
      _delegate.deserialize<T>(data, t);

  @override
  String? getClassNameForObject(Object? data) =>
      _delegate.getClassNameForObject(data);

  @override
  dynamic deserializeByClassName(Map<String, dynamic> data) =>
      _delegate.deserializeByClassName(data);

  @override
  String getModuleName() => _delegate.getModuleName();

  @override
  Table? getTableForType(Type t) => _delegate.getTableForType(t);

  @override
  List<TableDefinition> getTargetTableDefinitions() => _targetTableDefinitions;
}

List<TableDefinition> firebirdCompatibleTargetTableDefinitions(
  List<TableDefinition> tables,
) {
  return [
    for (final table in tables)
      table.copyWith(
        name: _uppercaseIdentifier(table.name),
        columns: [
          for (final column in table.columns)
            _normalizeFirebirdCompatibleColumn(column),
        ],
        foreignKeys: [
          for (final foreignKey in table.foreignKeys)
            foreignKey.copyWith(
              constraintName: _uppercaseIdentifier(foreignKey.constraintName),
              columns: [
                for (final column in foreignKey.columns)
                  _uppercaseIdentifier(column),
              ],
              referenceTable: _uppercaseIdentifier(foreignKey.referenceTable),
              referenceColumns: [
                for (final column in foreignKey.referenceColumns)
                  _uppercaseIdentifier(column),
              ],
            ),
        ],
        indexes: [
          for (final index in table.indexes)
            index.copyWith(
              indexName: _uppercaseIdentifier(index.indexName),
              elements: [
                for (final element in index.elements)
                  element.type == IndexElementDefinitionType.column
                      ? element.copyWith(
                          definition: _uppercaseIdentifier(element.definition),
                        )
                      : element,
              ],
            ),
        ],
      ),
  ];
}

ColumnDefinition _normalizeFirebirdCompatibleColumn(ColumnDefinition column) {
  final normalized = column.copyWith(name: _uppercaseIdentifier(column.name));

  if (column.columnType == ColumnType.json) {
    return normalized.copyWith(columnType: ColumnType.text);
  }

  if (column.columnType == ColumnType.uuid &&
      (column.columnDefault == defaultUuidValueRandom ||
          column.columnDefault == defaultUuidValueRandomV7)) {
    return normalized.copyWith(columnDefault: null);
  }

  return normalized;
}

String _uppercaseIdentifier(String value) => value.toUpperCase();
