import 'dart:convert';

import 'package:serverpod_database/serverpod_database.dart';

import '../runtime/firebird_savepoint.dart';
import '../runtime/firebird_transaction.dart';

class FirebirdServerpodSavepoint implements Savepoint {
  FirebirdServerpodSavepoint(this._savepoint);

  final FirebirdSavepoint _savepoint;

  @override
  String get id => _savepoint.id;

  @override
  Future<void> release() => _savepoint.release();

  @override
  Future<void> rollback() => _savepoint.rollback();
}

/// Serverpod transaction wrapper over the `firebirdpod` explicit transaction.
class FirebirdServerpodTransaction implements Transaction {
  FirebirdServerpodTransaction(this._transaction);

  final FirebirdTransaction _transaction;

  @override
  final Map<String, dynamic> runtimeParameters = <String, dynamic>{};

  bool get isCancelled => _transaction.isRolledBack;

  FirebirdTransaction get nativeTransaction => _transaction;

  @override
  Future<void> cancel() => _transaction.cancel();

  @override
  Future<Savepoint> createSavepoint() async {
    final savepoint = await _transaction.createSavepoint();
    return FirebirdServerpodSavepoint(savepoint);
  }

  @override
  Future<void> setRuntimeParameters(
    RuntimeParametersListBuilder builder,
  ) async {
    final groups = builder(RuntimeParametersBuilder());

    for (final group in groups) {
      final unsupportedReason = _unsupportedRuntimeParametersReason(group);
      if (unsupportedReason != null) {
        throw UnsupportedError(unsupportedReason);
      }

      final values = <String, Object>{};
      for (final entry in group.options.entries) {
        values[entry.key] = _normalizeRuntimeParameterValue(
          entry.key,
          entry.value,
        );
      }

      await _transaction.setRuntimeParameters(
        (params) => [params.transactionContextValues(values)],
      );
      runtimeParameters.addAll(group.options);
    }
  }

  String? _unsupportedRuntimeParametersReason(RuntimeParameters group) {
    if (group is SearchPathsConfig) {
      return 'Firebird does not support PostgreSQL search_path runtime '
          'parameters.';
    }
    if (group is HnswIndexQueryOptions ||
        group is IvfflatIndexQueryOptions ||
        group is VectorIndexQueryOptions) {
      return 'Firebird does not support PostgreSQL vector runtime '
          'parameters.';
    }
    return null;
  }

  Object _normalizeRuntimeParameterValue(String key, Object? value) {
    if (value == null) {
      throw UnsupportedError(
        'Firebird transaction runtime parameter "$key" cannot be null.',
      );
    }
    if (value is String || value is num || value is bool) return value;
    if (value is Duration) return value.inMicroseconds;
    if (value is DateTime) return value.toUtc().toIso8601String();
    if (value is Uri) return value.toString();
    if (value is BigInt) return value.toString();
    if (value is Enum) return value.name;
    if (value is List || value is Map || value is Set) {
      return jsonEncode(value);
    }

    throw UnsupportedError(
      'Firebird transaction runtime parameter "$key" has unsupported value '
      'type ${value.runtimeType}.',
    );
  }
}
