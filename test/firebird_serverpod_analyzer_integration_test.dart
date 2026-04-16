import 'package:firebirdpod/firebirdpod.dart';
import 'package:serverpod_database/serverpod_database.dart';
import 'package:test/test.dart';

import 'firebird_test_support.dart';

void main() {
  group('Firebird Serverpod analyzer', () {
    test(
      'analyze round-trips supported Firebird schema metadata and passes integrity verification',
      () async {
        if (!shouldRunDirectIntegrationTests()) {
          markTestSkipped(
            'set FIREBIRDPOD_RUN_FBCLIENT_DIRECT=1 to run live Firebird analyzer tests',
          );
          return;
        }

        final targetDefinition = _buildTargetDefinition();
        final harness = await _createHarness(targetDefinition);
        addTearDown(harness.dispose);

        final liveDatabase = await harness.session.db.analyzer.analyze();
        final parent = liveDatabase.findTableNamed(
          'firebirdpod_analyzer_parent',
        );
        final child = liveDatabase.findTableNamed('firebirdpod_analyzer_child');

        expect(parent, isNotNull);
        expect(child, isNotNull);
        expect(
          parent!.like(
            targetDefinition.findTableNamed('firebirdpod_analyzer_parent')!,
          ),
          isEmpty,
        );
        expect(
          child!.like(
            targetDefinition.findTableNamed('firebirdpod_analyzer_child')!,
          ),
          isEmpty,
        );
        expect(
          await MigrationManager.verifyDatabaseIntegrity(harness.session),
          isTrue,
        );
      },
    );

    test('verifyDatabaseIntegrity reports a supported drift mismatch', () async {
      if (!shouldRunDirectIntegrationTests()) {
        markTestSkipped(
          'set FIREBIRDPOD_RUN_FBCLIENT_DIRECT=1 to run live Firebird analyzer tests',
        );
        return;
      }

      final liveDefinition = _buildTargetDefinition();
      final expectedDefinition = liveDefinition.copyWith(
        tables: [
          liveDefinition.findTableNamed('firebirdpod_analyzer_parent')!,
          liveDefinition
              .findTableNamed('firebirdpod_analyzer_child')!
              .copyWith(
                indexes: [
                  ...liveDefinition
                      .findTableNamed('firebirdpod_analyzer_child')!
                      .indexes,
                  IndexDefinition(
                    indexName: 'fbpod_analyzer_child_parent_idx',
                    elements: [
                      IndexElementDefinition(
                        type: IndexElementDefinitionType.column,
                        definition: 'parentId',
                      ),
                    ],
                    type: 'btree',
                    isUnique: false,
                    isPrimary: false,
                  ),
                ],
              ),
        ],
      );

      final harness = await _createHarness(
        liveDefinition,
        expectedDefinition: expectedDefinition,
      );
      addTearDown(harness.dispose);

      expect(
        await MigrationManager.verifyDatabaseIntegrity(harness.session),
        isFalse,
      );
    });
  });
}

DatabaseDefinition _buildTargetDefinition() {
  return DatabaseDefinition(
    moduleName: 'app',
    tables: [
      TableDefinition(
        name: 'firebirdpod_analyzer_parent',
        dartName: 'Parent',
        schema: 'public',
        columns: [
          ColumnDefinition(
            name: 'id',
            columnType: ColumnType.bigint,
            isNullable: false,
            columnDefault: defaultIntSerial,
          ),
          ColumnDefinition(
            name: 'name',
            columnType: ColumnType.text,
            isNullable: false,
          ),
          ColumnDefinition(
            name: 'isActive',
            columnType: ColumnType.boolean,
            isNullable: false,
            columnDefault: defaultBooleanTrue,
          ),
          ColumnDefinition(
            name: 'createdAt',
            columnType: ColumnType.timestampWithoutTimeZone,
            isNullable: false,
            columnDefault: defaultDateTimeValueNow,
          ),
          ColumnDefinition(
            name: 'publicId',
            columnType: ColumnType.uuid,
            isNullable: false,
          ),
          ColumnDefinition(
            name: 'payload',
            columnType: ColumnType.bytea,
            isNullable: true,
          ),
        ],
        foreignKeys: const [],
        indexes: [
          IndexDefinition(
            indexName: 'fbpod_analyzer_parent_public_id_uq',
            elements: [
              IndexElementDefinition(
                type: IndexElementDefinitionType.column,
                definition: 'publicId',
              ),
            ],
            type: 'btree',
            isUnique: true,
            isPrimary: false,
          ),
        ],
      ),
      TableDefinition(
        name: 'firebirdpod_analyzer_child',
        dartName: 'Child',
        schema: 'public',
        columns: [
          ColumnDefinition(
            name: 'id',
            columnType: ColumnType.integer,
            isNullable: false,
            columnDefault: defaultIntSerial,
          ),
          ColumnDefinition(
            name: 'parentId',
            columnType: ColumnType.bigint,
            isNullable: false,
          ),
          ColumnDefinition(
            name: 'notes',
            columnType: ColumnType.text,
            isNullable: true,
          ),
        ],
        foreignKeys: [
          ForeignKeyDefinition(
            constraintName: 'fbpod_analyzer_child_parent_fk',
            columns: const ['parentId'],
            referenceTable: 'firebirdpod_analyzer_parent',
            referenceTableSchema: 'public',
            referenceColumns: const ['id'],
            onDelete: ForeignKeyAction.cascade,
            onUpdate: ForeignKeyAction.noAction,
          ),
        ],
        indexes: const [],
      ),
    ],
    installedModules: const [],
    migrationApiVersion: 1,
  );
}

Future<_AnalyzerHarness> _createHarness(
  DatabaseDefinition liveDefinition, {
  DatabaseDefinition? expectedDefinition,
}) async {
  registerFirebirdServerpodDialect();

  final generator = const FirebirdServerpodSqlGenerator();
  final definitionSql = generator.generateDatabaseDefinitionSql(
    liveDefinition,
    installedModules: const [],
  );
  final targetTables = (expectedDefinition ?? liveDefinition).tables;
  final serializationManager = _AnalyzerTestSerializationManager(targetTables);
  final poolManager = FirebirdServerpodPoolManager(
    serializationManager,
    null,
    FirebirdServerpodDatabaseConfig(
      host: 'localhost',
      port: 3050,
      user: firebirdTestUser(),
      password: firebirdTestPassword(),
      name: firebirdTestDatabasePath(),
      charset: 'UTF8',
      fbClientLibraryPath: firebirdClientLibraryPath(),
    ),
  )..start();

  late Database database;
  final session = _TestSession(() => database);
  database = DatabaseConstructor.create(
    session: session,
    poolManager: poolManager,
  );

  await _cleanupAnalyzerArtifacts(session);
  await session.db.unsafeSimpleExecute(definitionSql);

  return _AnalyzerHarness(
    liveDefinition: liveDefinition,
    poolManager: poolManager,
    session: session,
  );
}

class _AnalyzerHarness {
  _AnalyzerHarness({
    required this.liveDefinition,
    required this.poolManager,
    required this.session,
  });

  final DatabaseDefinition liveDefinition;
  final FirebirdServerpodPoolManager poolManager;
  final _TestSession session;

  Future<void> dispose() async {
    await _cleanupAnalyzerArtifacts(session);
    await poolManager.stop();
  }
}

Future<void> _cleanupAnalyzerArtifacts(DatabaseSession session) async {
  await _dropTableIfExists(session, 'firebirdpod_analyzer_child');
  await _dropTableIfExists(session, 'firebirdpod_analyzer_parent');
}

Future<String?> _lookupStoredTableName(
  DatabaseSession session,
  String tableName,
) async {
  final result = await session.db.unsafeQuery('''
select trim(rdb\$relation_name) as "TABLE_NAME"
from rdb\$relations
where upper(rdb\$relation_name) = '${tableName.toUpperCase()}'
  and coalesce(rdb\$system_flag, 0) = 0
''');

  if (result.isEmpty) {
    return null;
  }

  return result.first.toColumnMap()['TABLE_NAME']?.toString();
}

Future<void> _dropTableIfExists(
  DatabaseSession session,
  String tableName,
) async {
  final storedName = await _lookupStoredTableName(session, tableName);
  if (storedName == null) {
    return;
  }

  await session.db.unsafeExecute('drop table "$storedName"');
}

class _AnalyzerTestSerializationManager extends SerializationManagerServer {
  _AnalyzerTestSerializationManager(this._targetTables);

  final List<TableDefinition> _targetTables;

  @override
  String getModuleName() => 'test';

  @override
  Table? getTableForType(Type t) => null;

  @override
  List<TableDefinition> getTargetTableDefinitions() => _targetTables;
}

class _TestSession implements DatabaseSession {
  _TestSession(this._database);

  final Database Function() _database;

  @override
  Database get db => _database();

  @override
  Transaction? get transaction => null;

  @override
  LogQueryFunction? get logQuery => null;

  @override
  LogWarningFunction? get logWarning => null;
}
