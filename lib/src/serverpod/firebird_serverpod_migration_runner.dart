import 'package:serverpod_database/serverpod_database.dart';

import '../runtime/firebird_database_exception.dart';

/// Firebird migration runner for Serverpod.
///
/// The runner bootstraps a dedicated migration-lock table and keeps a singleton
/// row lock for the duration of the migration transaction. This gives us
/// one-migration-at-a-time behavior using ordinary Firebird locking semantics.
class FirebirdServerpodMigrationRunner extends MigrationRunner {
  const FirebirdServerpodMigrationRunner({required super.runMode});

  static const migrationLockTableName = 'serverpod_migration_lock';

  static const _bootstrapMigrationLockTableSql = '''
execute block as
begin
  if (not exists(
    select 1
    from rdb\$relations
    where rdb\$relation_name = 'serverpod_migration_lock'
  )) then
    execute statement '
      create table "serverpod_migration_lock" (
        "id" smallint not null primary key,
        "acquired_at" timestamp not null
      )
    ';
end
''';

  static const _touchMigrationLockRowSql = '''
update "serverpod_migration_lock"
set "acquired_at" = current_timestamp
where "id" = 1
''';

  static const _insertMigrationLockRowSql = '''
insert into "serverpod_migration_lock" ("id", "acquired_at")
values (1, current_timestamp)
''';

  static const _acquireMigrationLockSql = '''
select "id"
from "serverpod_migration_lock"
where "id" = 1
for update with lock
''';

  @override
  Future<void> runMigrations(
    DatabaseSession session,
    Future<void> Function(Transaction? transaction) action,
  ) async {
    await session.db.transaction((transaction) async {
      await session.db.unsafeExecute(
        _bootstrapMigrationLockTableSql,
        transaction: transaction,
      );
    });
    await session.db.transaction((transaction) async {
      final updatedRows = await session.db.unsafeExecute(
        _touchMigrationLockRowSql,
        transaction: transaction,
      );
      if (updatedRows != 0) {
        return;
      }

      try {
        await session.db.unsafeExecute(
          _insertMigrationLockRowSql,
          transaction: transaction,
        );
      } on FirebirdDatabaseException catch (error) {
        if (!_isDuplicateKeyViolation(error)) {
          rethrow;
        }
      }
    });

    await session.db.transaction((transaction) async {
      await session.db.unsafeQuery(
        _acquireMigrationLockSql,
        transaction: transaction,
      );

      await action(transaction);
    });
  }

  static bool _isDuplicateKeyViolation(FirebirdDatabaseException error) {
    return error.message.contains(
      'violation of PRIMARY or UNIQUE KEY constraint',
    );
  }
}
