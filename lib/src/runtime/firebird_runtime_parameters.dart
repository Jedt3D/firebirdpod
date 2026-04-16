import 'package:meta/meta.dart';

import 'firebird_native_client.dart';

typedef FirebirdRuntimeParametersListBuilder =
    List<FirebirdRuntimeParameters> Function(
      FirebirdRuntimeParametersBuilder params,
    );

@immutable
abstract class FirebirdRuntimeParameters {
  const FirebirdRuntimeParameters();

  Map<String, Object> get options;

  Future<void> apply(FirebirdNativeTransaction transaction) async {
    for (final entry in options.entries) {
      await transaction.setTransactionContext(entry.key, entry.value);
    }
  }
}

/// Transaction-local values stored in Firebird's USER_TRANSACTION context.
class FirebirdTransactionContextParameters extends FirebirdRuntimeParameters {
  FirebirdTransactionContextParameters(Map<String, Object> options)
    : _options = Map<String, Object>.unmodifiable(
        Map<String, Object>.fromEntries(
          options.entries.map((entry) {
            final key = entry.key.trim();
            if (key.isEmpty) {
              throw ArgumentError.value(
                entry.key,
                'options',
                'Transaction context keys must not be empty.',
              );
            }
            return MapEntry<String, Object>(key, entry.value);
          }),
        ),
      );

  final Map<String, Object> _options;

  @override
  Map<String, Object> get options => _options;
}

class FirebirdRuntimeParametersBuilder {
  const FirebirdRuntimeParametersBuilder();

  FirebirdTransactionContextParameters transactionContextValue(
    String key,
    Object value,
  ) => FirebirdTransactionContextParameters(<String, Object>{key: value});

  FirebirdTransactionContextParameters transactionContextValues(
    Map<String, Object> values,
  ) => FirebirdTransactionContextParameters(values);
}
