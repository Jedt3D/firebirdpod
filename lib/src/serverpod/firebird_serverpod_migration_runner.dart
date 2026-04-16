import 'package:serverpod_database/serverpod_database.dart';

/// Placeholder migration runner until Phase 03 lands.
class FirebirdServerpodMigrationRunner extends MigrationRunner {
  const FirebirdServerpodMigrationRunner({required super.runMode});

  @override
  Future<void> runMigrations(
    DatabaseSession session,
    Future<void> Function(Transaction? transaction) action,
  ) async {
    throw UnsupportedError(
      'Firebird migration execution is not implemented yet. This belongs to '
      'Phase 03 schema and migration work.',
    );
  }
}
