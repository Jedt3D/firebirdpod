import 'package:serverpod_database/serverpod_database.dart';

/// Firebird-backed schema analyzer for Serverpod.
///
/// The current baseline focuses on the metadata needed for the first live
/// drift-analysis slice:
///
/// - tables
/// - columns
/// - indexes
/// - foreign keys
///
/// Firebird-specific caveat:
/// `BLOB SUB_TYPE TEXT` does not by itself preserve whether the source column
/// was modeled as `text` or `json`, so this baseline currently resolves those
/// columns as `text`.
class FirebirdServerpodDatabaseAnalyzer extends DatabaseAnalyzer {
  FirebirdServerpodDatabaseAnalyzer({required super.database});

  static const _defaultSchema = 'public';

  @override
  Future<String> getCurrentDatabaseName() async {
    final result = await database.unsafeQuery('''
      select mon\$database_name as db_name
      from mon\$database
      ''');
    if (result.isEmpty) {
      return 'firebird';
    }

    final value = result.first.toColumnMap()['DB_NAME'];
    if (value is String && value.isNotEmpty) {
      return value;
    }
    return 'firebird';
  }

  @override
  Future<List<TableDefinition>> getTableDefinitions() async {
    final result = await database.unsafeQuery('''
select trim(r.rdb\$relation_name) as "TABLE_NAME"
from rdb\$relations r
where coalesce(r.rdb\$system_flag, 0) = 0
  and r.rdb\$view_blr is null
order by r.rdb\$relation_name
''');

    final tableNames = result
        .map((row) => row.toColumnMap()['TABLE_NAME'])
        .whereType<String>()
        .toList();

    return Future.wait(
      tableNames.map((tableName) async {
        final columns = await getColumnDefinitions(
          schemaName: _defaultSchema,
          tableName: tableName,
        );
        final foreignKeys = await getForeignKeyDefinitions(
          schemaName: _defaultSchema,
          tableName: tableName,
        );
        final indexes = await getIndexDefinitions(
          schemaName: _defaultSchema,
          tableName: tableName,
        );

        return TableDefinition(
          name: tableName,
          schema: _defaultSchema,
          columns: columns,
          foreignKeys: foreignKeys,
          indexes: indexes,
        );
      }),
    );
  }

  @override
  Future<List<ColumnDefinition>> getColumnDefinitions({
    required String schemaName,
    required String tableName,
  }) async {
    final queryResult = await database.unsafeQuery('''
select trim(rf.rdb\$field_name) as "COLUMN_NAME",
       coalesce(rf.rdb\$null_flag, 0) as "NULL_FLAG",
       rf.rdb\$default_source as "DEFAULT_SOURCE",
       rf.rdb\$identity_type as "IDENTITY_TYPE",
       f.rdb\$field_type as "FIELD_TYPE",
       f.rdb\$field_sub_type as "FIELD_SUB_TYPE",
       f.rdb\$field_length as "FIELD_LENGTH",
       f.rdb\$field_precision as "FIELD_PRECISION",
       f.rdb\$field_scale as "FIELD_SCALE",
       f.rdb\$dimensions as "DIMENSIONS",
       f.rdb\$character_length as "CHARACTER_LENGTH",
       trim(cs.rdb\$character_set_name) as "CHARACTER_SET_NAME"
from rdb\$relation_fields rf
join rdb\$fields f
  on f.rdb\$field_name = rf.rdb\$field_source
left join rdb\$character_sets cs
  on cs.rdb\$character_set_id = f.rdb\$character_set_id
where upper(rf.rdb\$relation_name) = '${tableName.toUpperCase()}'
order by rf.rdb\$field_position
''');

    return queryResult.map((row) {
      final column = row.toColumnMap();
      final columnName = column['COLUMN_NAME'] as String;
      final fieldType = _asInt(column['FIELD_TYPE']);
      final fieldSubType = _asInt(column['FIELD_SUB_TYPE']);
      final fieldScale = _asInt(column['FIELD_SCALE']);
      final characterLength = _asInt(column['CHARACTER_LENGTH']);
      final charsetName = column['CHARACTER_SET_NAME'] as String?;
      final isIdentity = column['IDENTITY_TYPE'] != null;

      final columnType = _resolveColumnType(
        fieldType: fieldType,
        fieldSubType: fieldSubType,
        fieldScale: fieldScale,
        characterLength: characterLength,
        charsetName: charsetName,
      );

      return ColumnDefinition(
        name: columnName,
        columnType: columnType,
        isNullable: _asInt(column['NULL_FLAG']) != 1,
        columnDefault: _firebirdSqlToAbstractDefault(
          column['DEFAULT_SOURCE']?.toString(),
          columnType,
          isIdentity: isIdentity,
        ),
      );
    }).toList();
  }

  @override
  Future<List<IndexDefinition>> getIndexDefinitions({
    required String schemaName,
    required String tableName,
  }) async {
    final queryResult = await database.unsafeQuery('''
select trim(ix.rdb\$index_name) as "INDEX_NAME",
       coalesce(ix.rdb\$unique_flag, 0) as "UNIQUE_FLAG",
       seg.rdb\$field_position as "FIELD_POSITION",
       trim(seg.rdb\$field_name) as "FIELD_NAME"
from rdb\$indices ix
join rdb\$index_segments seg
  on seg.rdb\$index_name = ix.rdb\$index_name
left join rdb\$relation_constraints rc
  on rc.rdb\$index_name = ix.rdb\$index_name
where upper(ix.rdb\$relation_name) = '${tableName.toUpperCase()}'
  and coalesce(ix.rdb\$system_flag, 0) = 0
  and rc.rdb\$constraint_name is null
order by ix.rdb\$index_name, seg.rdb\$field_position
''');

    final grouped = <String, List<Map<String, dynamic>>>{};
    for (final row in queryResult) {
      final column = row.toColumnMap();
      final indexName = column['INDEX_NAME'] as String;
      grouped
          .putIfAbsent(indexName, () => <Map<String, dynamic>>[])
          .add(column);
    }

    return grouped.entries.map((entry) {
      final rows = entry.value;
      return IndexDefinition(
        indexName: entry.key,
        elements: rows
            .map(
              (row) => IndexElementDefinition(
                type: IndexElementDefinitionType.column,
                definition: row['FIELD_NAME'] as String? ?? '',
              ),
            )
            .toList(),
        type: 'btree',
        isUnique: _asInt(rows.first['UNIQUE_FLAG']) == 1,
        isPrimary: false,
      );
    }).toList();
  }

  @override
  Future<List<ForeignKeyDefinition>> getForeignKeyDefinitions({
    required String schemaName,
    required String tableName,
  }) async {
    final queryResult = await database.unsafeQuery('''
select trim(rc.rdb\$constraint_name) as "CONSTRAINT_NAME",
       seg.rdb\$field_position as "FIELD_POSITION",
       trim(seg.rdb\$field_name) as "FIELD_NAME",
       trim(rcu.rdb\$relation_name) as "REFERENCE_TABLE",
       trim(refseg.rdb\$field_name) as "REFERENCE_FIELD",
       trim(refc.rdb\$update_rule) as "UPDATE_RULE",
       trim(refc.rdb\$delete_rule) as "DELETE_RULE"
from rdb\$relation_constraints rc
join rdb\$ref_constraints refc
  on refc.rdb\$constraint_name = rc.rdb\$constraint_name
join rdb\$relation_constraints rcu
  on rcu.rdb\$constraint_name = refc.rdb\$const_name_uq
join rdb\$index_segments seg
  on seg.rdb\$index_name = rc.rdb\$index_name
join rdb\$index_segments refseg
  on refseg.rdb\$index_name = rcu.rdb\$index_name
 and refseg.rdb\$field_position = seg.rdb\$field_position
where upper(rc.rdb\$relation_name) = '${tableName.toUpperCase()}'
  and rc.rdb\$constraint_type = 'FOREIGN KEY'
order by rc.rdb\$constraint_name, seg.rdb\$field_position
''');

    final grouped = <String, List<Map<String, dynamic>>>{};
    for (final row in queryResult) {
      final column = row.toColumnMap();
      final constraintName = column['CONSTRAINT_NAME'] as String;
      grouped
          .putIfAbsent(constraintName, () => <Map<String, dynamic>>[])
          .add(column);
    }

    return grouped.entries.map((entry) {
      final rows = entry.value;
      return ForeignKeyDefinition(
        constraintName: entry.key,
        columns: rows.map((row) => row['FIELD_NAME'] as String).toList(),
        referenceTable: rows.first['REFERENCE_TABLE'] as String,
        referenceTableSchema: _defaultSchema,
        referenceColumns: rows
            .map((row) => row['REFERENCE_FIELD'] as String)
            .toList(),
        onUpdate: _parseForeignKeyAction(rows.first['UPDATE_RULE'] as String?),
        onDelete: _parseForeignKeyAction(rows.first['DELETE_RULE'] as String?),
      );
    }).toList();
  }

  static int? _asInt(Object? value) {
    if (value is int) return value;
    if (value is num) return value.toInt();
    return null;
  }

  static ColumnType _resolveColumnType({
    required int? fieldType,
    required int? fieldSubType,
    required int? fieldScale,
    required int? characterLength,
    required String? charsetName,
  }) {
    switch (fieldType) {
      case 7:
      case 8:
        return ColumnType.integer;
      case 16:
        return (fieldScale == null || fieldScale == 0)
            ? ColumnType.bigint
            : ColumnType.unknown;
      case 23:
        return ColumnType.boolean;
      case 27:
        return ColumnType.doublePrecision;
      case 35:
        return ColumnType.timestampWithoutTimeZone;
      case 14:
      case 37:
        final normalizedCharset = charsetName?.toUpperCase();
        if (normalizedCharset == 'ASCII' && characterLength == 36) {
          return ColumnType.uuid;
        }
        return ColumnType.text;
      case 261:
        return switch (fieldSubType) {
          0 => ColumnType.bytea,
          1 => ColumnType.text,
          _ => ColumnType.unknown,
        };
      default:
        return ColumnType.unknown;
    }
  }

  static String? _firebirdSqlToAbstractDefault(
    String? sql,
    ColumnType columnType, {
    required bool isIdentity,
  }) {
    if (isIdentity) {
      return defaultIntSerial;
    }

    if (sql == null || sql.trim().isEmpty) {
      return null;
    }

    final normalized = sql
        .replaceAll('\r', ' ')
        .replaceAll('\n', ' ')
        .replaceAll(RegExp(r'\s+'), ' ')
        .trim();
    final expression = normalized.toUpperCase().startsWith('DEFAULT ')
        ? normalized.substring(8).trim()
        : normalized;
    final upper = expression.toUpperCase();

    if (upper == 'CURRENT_TIMESTAMP') {
      return defaultDateTimeValueNow;
    }
    if (upper == 'TRUE') {
      return defaultBooleanTrue;
    }
    if (upper == 'FALSE') {
      return defaultBooleanFalse;
    }
    if (upper == 'UUID_TO_CHAR(GEN_UUID())') {
      return defaultUuidValueRandom;
    }

    if (columnType == ColumnType.timestampWithoutTimeZone) {
      final match = RegExp(
        r"^TIMESTAMP\s*'([^']+)'$",
        caseSensitive: false,
      ).firstMatch(expression);
      if (match != null) {
        return DateTime.parse('${match.group(1)}Z').toIso8601String();
      }
    }

    return expression;
  }

  static ForeignKeyAction? _parseForeignKeyAction(String? value) {
    if (value == null || value.isEmpty) return null;

    return switch (value.toUpperCase()) {
      'CASCADE' => ForeignKeyAction.cascade,
      'SET NULL' => ForeignKeyAction.setNull,
      'SET DEFAULT' => ForeignKeyAction.setDefault,
      'RESTRICT' => ForeignKeyAction.restrict,
      'NO ACTION' => ForeignKeyAction.noAction,
      _ => null,
    };
  }
}
