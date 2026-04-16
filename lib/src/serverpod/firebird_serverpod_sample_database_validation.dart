import 'dart:io';

import 'package:serverpod_database/serverpod_database.dart';
import 'package:serverpod_shared/serverpod_shared.dart';

import '../schema/firebird_serverpod_sql_generator.dart';
import 'firebird_serverpod_config.dart';
import 'firebird_serverpod_pool_manager.dart';
import 'firebird_serverpod_registration.dart';

/// The validation group a sample database belongs to.
enum FirebirdSampleDatabaseKind {
  /// Raw converted sample databases.
  converted,

  /// Curated Firebird-native fixture databases.
  curatedNative,
}

/// Static registration for a known sample database in this workspace.
class FirebirdSampleDatabaseTarget {
  const FirebirdSampleDatabaseTarget({
    required this.name,
    required this.path,
    required this.kind,
  });

  final String name;
  final String path;
  final FirebirdSampleDatabaseKind kind;

  String get label => switch (kind) {
    FirebirdSampleDatabaseKind.converted => '$name (converted)',
    FirebirdSampleDatabaseKind.curatedNative => '$name (native)',
  };
}

/// A specific validation issue found while profiling a sample database.
class FirebirdSampleDatabaseValidationIssue {
  const FirebirdSampleDatabaseValidationIssue({
    required this.category,
    required this.tableName,
    this.columnName,
    required this.detail,
  });

  final String category;
  final String tableName;
  final String? columnName;
  final String detail;

  String get displayLocation => columnName == null
      ? tableName
      : '$tableName.$columnName';
}

/// Result of validating one sample database against the current Firebird baseline.
class FirebirdSampleDatabaseValidationResult {
  const FirebirdSampleDatabaseValidationResult({
    required this.target,
    required this.tableCount,
    required this.viewCount,
    required this.triggerCount,
    required this.procedureCount,
    required this.sequenceCount,
    required this.columnCount,
    required this.unknownColumns,
    required this.unresolvedDefaults,
    required this.generatorError,
  });

  final FirebirdSampleDatabaseTarget target;
  final int tableCount;
  final int viewCount;
  final int triggerCount;
  final int procedureCount;
  final int sequenceCount;
  final int columnCount;
  final List<FirebirdSampleDatabaseValidationIssue> unknownColumns;
  final List<FirebirdSampleDatabaseValidationIssue> unresolvedDefaults;
  final String? generatorError;

  int get issueCount =>
      unknownColumns.length +
      unresolvedDefaults.length +
      (generatorError == null ? 0 : 1);

  bool get generatorCompatible => generatorError == null;

  bool get passesZeroGapBaseline =>
      unknownColumns.isEmpty &&
      unresolvedDefaults.isEmpty &&
      generatorCompatible;
}

/// Default workspace sample databases used for Slice 03D validation.
const firebirdSampleDatabaseTargets = <FirebirdSampleDatabaseTarget>[
  FirebirdSampleDatabaseTarget(
    name: 'car_database',
    path: '/Users/worajedt/GitHub/FireDart/databases/firebird/car_database.fdb',
    kind: FirebirdSampleDatabaseKind.converted,
  ),
  FirebirdSampleDatabaseTarget(
    name: 'chinook',
    path: '/Users/worajedt/GitHub/FireDart/databases/firebird/chinook.fdb',
    kind: FirebirdSampleDatabaseKind.converted,
  ),
  FirebirdSampleDatabaseTarget(
    name: 'northwind',
    path: '/Users/worajedt/GitHub/FireDart/databases/firebird/northwind.fdb',
    kind: FirebirdSampleDatabaseKind.converted,
  ),
  FirebirdSampleDatabaseTarget(
    name: 'sakila_master',
    path: '/Users/worajedt/GitHub/FireDart/databases/firebird/sakila_master.fdb',
    kind: FirebirdSampleDatabaseKind.converted,
  ),
  FirebirdSampleDatabaseTarget(
    name: 'car_database_native',
    path:
        '/Users/worajedt/GitHub/FireDart/databases/firebird_native/car_database_native.fdb',
    kind: FirebirdSampleDatabaseKind.curatedNative,
  ),
  FirebirdSampleDatabaseTarget(
    name: 'chinook_native',
    path:
        '/Users/worajedt/GitHub/FireDart/databases/firebird_native/chinook_native.fdb',
    kind: FirebirdSampleDatabaseKind.curatedNative,
  ),
  FirebirdSampleDatabaseTarget(
    name: 'northwind_native',
    path:
        '/Users/worajedt/GitHub/FireDart/databases/firebird_native/northwind_native.fdb',
    kind: FirebirdSampleDatabaseKind.curatedNative,
  ),
  FirebirdSampleDatabaseTarget(
    name: 'sakila_master_native',
    path:
        '/Users/worajedt/GitHub/FireDart/databases/firebird_native/sakila_master_native.fdb',
    kind: FirebirdSampleDatabaseKind.curatedNative,
  ),
];

/// Validates a sample database against the current Firebird analyzer and generator baseline.
Future<FirebirdSampleDatabaseValidationResult> validateSampleDatabase(
  FirebirdSampleDatabaseTarget target,
) async {
  if (!File(target.path).existsSync()) {
    throw ArgumentError.value(
      target.path,
      'target.path',
      'Sample database does not exist.',
    );
  }

  registerFirebirdServerpodDialect();

  final poolManager = FirebirdServerpodPoolManager(
    _ValidationSerializationManager(),
    null,
    FirebirdServerpodDatabaseConfig(
      host: Platform.environment['FIREBIRDPOD_TEST_HOST'] ?? 'localhost',
      port: _readInt(Platform.environment['FIREBIRDPOD_TEST_PORT']) ?? 3050,
      user: Platform.environment['FIREBIRDPOD_TEST_USER'] ?? 'sysdba',
      password: Platform.environment['FIREBIRDPOD_TEST_PASSWORD'] ?? 'masterkey',
      name: target.path,
      charset: 'UTF8',
      fbClientLibraryPath:
          Platform.environment['FIREBIRDPOD_FBCLIENT_LIB'] ??
          '/Library/Frameworks/Firebird.framework/Versions/A/Resources/lib/libfbclient.dylib',
    ),
  )..start();

  late Database database;
  final session = _ValidationSession(() => database);
  database = DatabaseConstructor.create(
    session: session,
    poolManager: poolManager,
  );

  try {
    final analyzer = session.db.analyzer;
    final liveDatabase = DatabaseDefinition(
      name: await analyzer.getCurrentDatabaseName(),
      moduleName: 'validation',
      tables: await analyzer.getTableDefinitions(),
      installedModules: const [],
      migrationApiVersion: DatabaseConstants.migrationApiVersion,
    );
    final unknownColumns = <FirebirdSampleDatabaseValidationIssue>[];

    for (final table in liveDatabase.tables) {
      for (final column in table.columns) {
        if (column.columnType == ColumnType.unknown) {
          unknownColumns.add(
            FirebirdSampleDatabaseValidationIssue(
              category: 'unknown-column-type',
              tableName: table.name,
              columnName: column.name,
              detail: 'Analyzer resolved this column as ColumnType.unknown.',
            ),
          );
        }
      }
    }

    final unresolvedDefaults = await _findUnresolvedDefaults(
      session: session,
      liveDatabase: liveDatabase,
    );

    String? generatorError;
    try {
      const generator = FirebirdServerpodSqlGenerator();
      generator.generateDatabaseDefinitionSql(
        liveDatabase.copyWith(
          moduleName: 'validation',
          installedModules: const [],
        ),
        installedModules: const [],
      );
    } catch (error) {
      generatorError = '$error';
    }

    return FirebirdSampleDatabaseValidationResult(
      target: target,
      tableCount: liveDatabase.tables.length,
      viewCount: await _countObjects(
        session,
        '''
select count(*) as "OBJECT_COUNT"
from rdb\$relations
where coalesce(rdb\$system_flag, 0) = 0
  and rdb\$view_blr is not null
''',
      ),
      triggerCount: await _countObjects(
        session,
        '''
select count(*) as "OBJECT_COUNT"
from rdb\$triggers
where coalesce(rdb\$system_flag, 0) = 0
''',
      ),
      procedureCount: await _countObjects(
        session,
        '''
select count(*) as "OBJECT_COUNT"
from rdb\$procedures
where coalesce(rdb\$system_flag, 0) = 0
''',
      ),
      sequenceCount: await _countObjects(
        session,
        '''
select count(*) as "OBJECT_COUNT"
from rdb\$generators
where coalesce(rdb\$system_flag, 0) = 0
''',
      ),
      columnCount: liveDatabase.tables.fold<int>(
        0,
        (count, table) => count + table.columns.length,
      ),
      unknownColumns: unknownColumns,
      unresolvedDefaults: unresolvedDefaults,
      generatorError: generatorError,
    );
  } finally {
    await poolManager.stop();
  }
}

Future<int> _countObjects(DatabaseSession session, String sql) async {
  final result = await session.db.unsafeQuery(sql);
  if (result.isEmpty) return 0;
  final value = result.first.toColumnMap()['OBJECT_COUNT'];
  if (value is int) return value;
  if (value is num) return value.toInt();
  return int.parse('$value');
}

Future<List<FirebirdSampleDatabaseValidationIssue>> _findUnresolvedDefaults({
  required DatabaseSession session,
  required DatabaseDefinition liveDatabase,
}) async {
  final result = await session.db.unsafeQuery('''
select trim(rf.rdb\$relation_name) as "TABLE_NAME",
       trim(rf.rdb\$field_name) as "COLUMN_NAME",
       trim(rf.rdb\$default_source) as "DEFAULT_SOURCE",
       rf.rdb\$identity_type as "IDENTITY_TYPE"
from rdb\$relation_fields rf
join rdb\$relations r
  on r.rdb\$relation_name = rf.rdb\$relation_name
where coalesce(r.rdb\$system_flag, 0) = 0
  and r.rdb\$view_blr is null
  and rf.rdb\$default_source is not null
order by rf.rdb\$relation_name, rf.rdb\$field_position
''');

  final issues = <FirebirdSampleDatabaseValidationIssue>[];
  for (final row in result) {
    final values = row.toColumnMap();
    final tableName = values['TABLE_NAME'] as String;
    final columnName = values['COLUMN_NAME'] as String;
    final defaultSource = values['DEFAULT_SOURCE']?.toString() ?? '';
    final identityType = values['IDENTITY_TYPE'];
    final normalizedDefault = defaultSource
        .replaceAll('\r', ' ')
        .replaceAll('\n', ' ')
        .replaceAll(RegExp(r'\s+'), ' ')
        .trim()
        .toUpperCase();

    if (identityType != null) {
      continue;
    }
    if (normalizedDefault == 'DEFAULT NULL' || normalizedDefault == 'NULL') {
      continue;
    }

    final table = liveDatabase.findTableNamed(tableName);
    final column = table?.findColumnNamed(columnName);
    if (column == null) {
      issues.add(
        FirebirdSampleDatabaseValidationIssue(
          category: 'default-without-analyzed-column',
          tableName: tableName,
          columnName: columnName,
          detail: defaultSource,
        ),
      );
      continue;
    }

    if (column.columnDefault == null) {
      issues.add(
        FirebirdSampleDatabaseValidationIssue(
          category: 'unresolved-default',
          tableName: tableName,
          columnName: columnName,
          detail: defaultSource,
        ),
      );
    }
  }

  return issues;
}

int? _readInt(String? value) {
  if (value == null || value.isEmpty) return null;
  return int.tryParse(value);
}

class _ValidationSerializationManager extends SerializationManagerServer {
  _ValidationSerializationManager();

  @override
  String getModuleName() => 'validation';

  @override
  Table? getTableForType(Type t) => null;

  @override
  List<TableDefinition> getTargetTableDefinitions() => const [];
}

class _ValidationSession implements DatabaseSession {
  _ValidationSession(this._database);

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
