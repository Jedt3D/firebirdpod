import 'package:serverpod_database/serverpod_database.dart';

Future<void> cleanupModuleArtifacts(
  DatabaseSession session,
  Iterable<TableDefinition> tables,
) async {
  final tableList = tables.toList();

  final foreignKeys = <({String tableName, String constraintName})>{
    for (final table in tableList)
      for (final foreignKey in table.foreignKeys)
        (tableName: table.name, constraintName: foreignKey.constraintName),
  };

  for (final foreignKey in foreignKeys) {
    await dropConstraintIfExists(
      session,
      tableName: foreignKey.tableName,
      constraintName: foreignKey.constraintName,
    );
  }

  for (final table in tableList.reversed) {
    await dropTableIfExists(session, table.name);
  }
}

Future<void> dropConstraintIfExists(
  DatabaseSession session, {
  required String tableName,
  required String constraintName,
}) async {
  final storedTableName = await lookupStoredTableName(session, tableName);
  if (storedTableName == null) {
    return;
  }

  final storedConstraintName = await lookupStoredConstraintName(
    session,
    tableName: tableName,
    constraintName: constraintName,
  );
  if (storedConstraintName == null) {
    return;
  }

  await session.db.unsafeExecute(
    'ALTER TABLE "$storedTableName" DROP CONSTRAINT "$storedConstraintName"',
  );
}

Future<void> dropTableIfExists(
  DatabaseSession session,
  String tableName,
) async {
  final storedName = await lookupStoredTableName(session, tableName);
  if (storedName == null) {
    return;
  }

  await session.db.unsafeExecute('DROP TABLE "$storedName"');
}

Future<String?> lookupStoredConstraintName(
  DatabaseSession session, {
  required String tableName,
  required String constraintName,
}) async {
  final result = await session.db.unsafeQuery('''
select trim(rc.rdb\$constraint_name) as "CONSTRAINT_NAME"
from rdb\$relation_constraints rc
join rdb\$relations r
  on r.rdb\$relation_name = rc.rdb\$relation_name
where upper(rc.rdb\$relation_name) = '${tableName.toUpperCase()}'
  and upper(rc.rdb\$constraint_name) = '${constraintName.toUpperCase()}'
  and coalesce(r.rdb\$system_flag, 0) = 0
''');

  if (result.isEmpty) {
    return null;
  }

  return result.first.toColumnMap()['CONSTRAINT_NAME']?.toString();
}

Future<String?> lookupStoredTableName(
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
