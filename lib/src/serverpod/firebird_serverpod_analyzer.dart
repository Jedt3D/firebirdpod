import 'package:serverpod_database/serverpod_database.dart';

/// Minimal Firebird analyzer for the Phase 02 runtime proof.
///
/// This is intentionally narrow: it provides enough metadata for
/// `Serverpod.start()` integrity checks in the proof app without pretending
/// that the full Phase 03 schema analyzer is finished.
class FirebirdServerpodDatabaseAnalyzer extends DatabaseAnalyzer {
  FirebirdServerpodDatabaseAnalyzer({required super.database});

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
  Future<List<TableDefinition>> getTableDefinitions() async => const [];

  @override
  Future<List<ColumnDefinition>> getColumnDefinitions({
    required String schemaName,
    required String tableName,
  }) async => const [];

  @override
  Future<List<IndexDefinition>> getIndexDefinitions({
    required String schemaName,
    required String tableName,
  }) async => const [];

  @override
  Future<List<ForeignKeyDefinition>> getForeignKeyDefinitions({
    required String schemaName,
    required String tableName,
  }) async => const [];
}
