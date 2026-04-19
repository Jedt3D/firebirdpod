import 'firebird_cancel_mode.dart';
import 'firebird_connection_options.dart';
import 'firebird_execution_result.dart';
import 'firebird_transaction_settings.dart';

abstract interface class FirebirdNativeClient {
  Future<FirebirdNativeConnection> attach(FirebirdConnectionOptions options);
}

abstract interface class FirebirdNativeConnection {
  Future<FirebirdNativeStatement> prepareStatement(String sql);
  Future<FirebirdNativeTransaction> beginTransaction({
    bool readOnly = false,
    FirebirdTransactionSettings settings = const FirebirdTransactionSettings(),
  });
  Future<void> resetRetainedState();
  Future<Duration?> getStatementTimeout();
  Future<void> setStatementTimeout(Duration? timeout);

  /// Low-level attachment control hook.
  ///
  /// Direct blocking-FFI adapters should not treat this alone as proof that a
  /// running statement can be cancelled from the same isolate that started it.
  Future<void> cancelOperation(FirebirdCancelMode mode);
  Future<void> close();
}

abstract interface class FirebirdNativeStatement {
  Future<String> getPlan({bool detailed = true});
  Future<Duration?> getTimeout();
  Future<void> setTimeout(Duration? timeout);
  Future<FirebirdExecutionResult> execute(List<Object?> values);
  Future<void> close();
}

abstract interface class FirebirdNativeTransaction {
  Future<FirebirdNativeStatement> prepareStatement(String sql);
  Future<FirebirdNativeSavepoint> createSavepoint(String id);
  Future<void> setTransactionContext(String key, Object value);
  Future<void> commit();
  Future<void> rollback();
  Future<void> close();
}

abstract interface class FirebirdNativeSavepoint {
  String get id;
  Future<void> release();
  Future<void> rollback();
}
