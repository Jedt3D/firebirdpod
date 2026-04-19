import 'package:meta/meta.dart';

import '../runtime/firebird_connection.dart';
import '../runtime/firebird_database_exception.dart';
import '../runtime/firebird_execution_result.dart';
import '../runtime/firebird_statement.dart';
import '../sql/firebird_parameter_style.dart';
import '../sql/firebird_statement_parameters.dart';

/// Timeout-state inspection and timeout-aware execution observation.
class FirebirdTimeoutDiagnostics {
  const FirebirdTimeoutDiagnostics(this._connection);

  final FirebirdConnection _connection;

  Future<FirebirdConnectionTimeoutState> captureConnectionState() async {
    final configuredTimeout = await _connection.getStatementTimeout();
    final systemContextMilliseconds = await _readSystemContextTimeout();
    return FirebirdConnectionTimeoutState(
      configuredTimeout: configuredTimeout,
      systemContextMilliseconds: systemContextMilliseconds,
    );
  }

  Future<FirebirdStatementTimeoutState> captureStatementState(
    FirebirdStatement statement,
  ) async {
    final configuredTimeout = await statement.getTimeout();
    return FirebirdStatementTimeoutState(
      sourceSql: statement.preparedSql.sourceSql,
      normalizedSql: statement.preparedSql.sql,
      parameterStyle: statement.preparedSql.parameterStyle,
      parameterCount: statement.preparedSql.parameterCount,
      configuredTimeout: configuredTimeout,
    );
  }

  Future<FirebirdObservedExecution> observeExecution(
    String query, {
    FirebirdStatementParameters? parameters,
    Duration? timeout,
  }) async {
    final connectionState = await captureConnectionState();
    final statement = await _connection.prepare(query);

    try {
      if (timeout != null) {
        await statement.setTimeout(timeout);
      }

      final statementState = await captureStatementState(statement);
      final startedAt = DateTime.now();
      final stopwatch = Stopwatch()..start();

      FirebirdExecutionResult? result;
      FirebirdDatabaseException? error;

      try {
        result = await statement.execute(parameters);
      } on FirebirdDatabaseException catch (exception) {
        error = exception;
      } finally {
        stopwatch.stop();
      }

      return FirebirdObservedExecution(
        sourceSql: statement.preparedSql.sourceSql,
        normalizedSql: statement.preparedSql.sql,
        parameterStyle: statement.preparedSql.parameterStyle,
        parameterCount: statement.preparedSql.parameterCount,
        connectionTimeout: connectionState.configuredTimeout,
        connectionSystemContextMilliseconds:
            connectionState.systemContextMilliseconds,
        requestedStatementTimeout: timeout,
        statementTimeout: statementState.configuredTimeout,
        startedAt: startedAt,
        finishedAt: startedAt.add(stopwatch.elapsed),
        elapsed: stopwatch.elapsed,
        result: result,
        error: error,
      );
    } finally {
      await statement.close();
    }
  }

  Future<int> _readSystemContextTimeout() async {
    final result = await _connection.execute('''
      select cast(coalesce(rdb\$get_context('SYSTEM', 'STATEMENT_TIMEOUT'), '0') as bigint) as RESULT_VALUE
      from rdb\$database
      ''');
    final value = result.singleRow?['RESULT_VALUE'];
    return switch (value) {
      int intValue => intValue,
      BigInt bigIntValue => bigIntValue.toInt(),
      _ => throw StateError('Unexpected statement-timeout value type: $value.'),
    };
  }
}

extension FirebirdConnectionTimeoutObserver on FirebirdConnection {
  FirebirdTimeoutDiagnostics get timeoutDiagnostics =>
      FirebirdTimeoutDiagnostics(this);
}

@immutable
class FirebirdConnectionTimeoutState {
  const FirebirdConnectionTimeoutState({
    required this.configuredTimeout,
    required this.systemContextMilliseconds,
  });

  final Duration? configuredTimeout;
  final int systemContextMilliseconds;
}

@immutable
class FirebirdStatementTimeoutState {
  const FirebirdStatementTimeoutState({
    required this.sourceSql,
    required this.normalizedSql,
    required this.parameterStyle,
    required this.parameterCount,
    required this.configuredTimeout,
  });

  final String sourceSql;
  final String normalizedSql;
  final FirebirdParameterStyle parameterStyle;
  final int parameterCount;
  final Duration? configuredTimeout;
}

@immutable
class FirebirdObservedExecution {
  const FirebirdObservedExecution({
    required this.sourceSql,
    required this.normalizedSql,
    required this.parameterStyle,
    required this.parameterCount,
    required this.connectionTimeout,
    required this.connectionSystemContextMilliseconds,
    required this.requestedStatementTimeout,
    required this.statementTimeout,
    required this.startedAt,
    required this.finishedAt,
    required this.elapsed,
    this.result,
    this.error,
  });

  final String sourceSql;
  final String normalizedSql;
  final FirebirdParameterStyle parameterStyle;
  final int parameterCount;
  final Duration? connectionTimeout;
  final int connectionSystemContextMilliseconds;
  final Duration? requestedStatementTimeout;
  final Duration? statementTimeout;
  final DateTime startedAt;
  final DateTime finishedAt;
  final Duration elapsed;
  final FirebirdExecutionResult? result;
  final FirebirdDatabaseException? error;

  bool get succeeded => error == null;
  bool get timedOut => error?.isTimeout ?? false;
  bool get cancelled => error?.isCancelled ?? false;
}
