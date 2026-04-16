import 'package:meta/meta.dart';

/// Serverpod-aligned isolation levels exposed by the Firebird adapter.
enum FirebirdTransactionIsolationLevel {
  /// Firebird has no true dirty-read mode, so this maps to read committed.
  readUncommitted,

  /// Maps to Firebird READ COMMITTED READ CONSISTENCY.
  readCommitted,

  /// Maps to Firebird SNAPSHOT (concurrency) transactions.
  repeatableRead,

  /// Maps to Firebird CONSISTENCY transactions.
  serializable,
}

@immutable
class FirebirdTransactionSettings {
  const FirebirdTransactionSettings({
    this.isolationLevel = FirebirdTransactionIsolationLevel.readCommitted,
  });

  final FirebirdTransactionIsolationLevel isolationLevel;

  @override
  bool operator ==(Object other) {
    if (identical(this, other)) return true;
    return other is FirebirdTransactionSettings &&
        other.isolationLevel == isolationLevel;
  }

  @override
  int get hashCode => isolationLevel.hashCode;
}
