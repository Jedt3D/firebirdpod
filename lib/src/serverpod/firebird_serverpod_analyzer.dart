import 'package:serverpod_database/serverpod_database.dart';

/// Placeholder analyzer until Phase 03 schema and migration work begins.
class FirebirdServerpodDatabaseAnalyzer extends DatabaseAnalyzer {
  FirebirdServerpodDatabaseAnalyzer({required super.database});

  Never _unsupported(String operation) {
    throw UnsupportedError(
      'Firebird analyzer operation "$operation" is not implemented yet. '
      'This belongs to Phase 03 schema and migration work.',
    );
  }

  @override
  Future<String> getCurrentDatabaseName() async => _unsupported(
    'getCurrentDatabaseName',
  );

  @override
  Future<List<TableDefinition>> getTableDefinitions() async => _unsupported(
    'getTableDefinitions',
  );

  @override
  Future<List<ColumnDefinition>> getColumnDefinitions({
    required String schemaName,
    required String tableName,
  }) async => _unsupported('getColumnDefinitions');

  @override
  Future<List<IndexDefinition>> getIndexDefinitions({
    required String schemaName,
    required String tableName,
  }) async => _unsupported('getIndexDefinitions');

  @override
  Future<List<ForeignKeyDefinition>> getForeignKeyDefinitions({
    required String schemaName,
    required String tableName,
  }) async => _unsupported('getForeignKeyDefinitions');
}
