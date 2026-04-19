import 'package:fbdb/fbclient.dart' as fbclient;
import 'package:firebirdpod/firebirdpod.dart';
import 'package:test/test.dart';

void main() {
  group('Firebird runtime seam', () {
    test('endpoint attaches using the supplied connection options', () async {
      final client = _FakeNativeClient();
      final endpoint = FirebirdEndpoint(
        client: client,
        options: const FirebirdConnectionOptions(
          database: '/data/example.fdb',
          host: 'dbhost',
          port: 3051,
          user: 'app',
          password: 'secret',
        ),
      );

      final connection = await endpoint.connect();

      expect(client.attachCalls, hasLength(1));
      expect(
        client.attachCalls.single.attachmentString,
        'dbhost/3051:/data/example.fdb',
      );
      expect(connection.isClosed, isFalse);
    });

    test('endpoint applies default statement timeout after attach', () async {
      final client = _FakeNativeClient();

      await FirebirdEndpoint(
        client: client,
        options: const FirebirdConnectionOptions(
          database: '/data/example.fdb',
          statementTimeout: Duration(milliseconds: 250),
        ),
      ).connect();

      expect(client.connection.statementTimeoutAssignments, [
        const Duration(milliseconds: 250),
      ]);
    });

    test(
      'prepare normalizes SQL before delegating to the native statement layer',
      () async {
        final client = _FakeNativeClient();
        final connection = await FirebirdEndpoint(
          client: client,
          options: const FirebirdConnectionOptions(
            database: '/data/example.fdb',
          ),
        ).connect();

        final statement = await connection.prepare(
          r'SELECT @tenant_id, @id FROM account WHERE id = @id',
        );

        expect(client.connection.preparedSql, [
          'SELECT ?, ? FROM account WHERE id = ?',
        ]);
        expect(
          statement.preparedSql.parameterStyle,
          FirebirdParameterStyle.named,
        );
        expect(statement.isClosed, isFalse);
      },
    );

    test(
      'statement execution binds values using the prepared layout on every call',
      () async {
        final client = _FakeNativeClient();
        final connection = await FirebirdEndpoint(
          client: client,
          options: const FirebirdConnectionOptions(
            database: '/data/example.fdb',
          ),
        ).connect();
        final statement = await connection.prepare(
          r'SELECT $2, $1, $2 FROM account WHERE id = $1',
        );

        await statement.execute(
          FirebirdStatementParameters.positional(['first', 'second']),
        );
        await statement.execute(
          FirebirdStatementParameters.positional(['left', 'right']),
        );

        final nativeStatement = client.connection.statements.single;
        expect(nativeStatement.executions, [
          ['second', 'first', 'second', 'first'],
          ['right', 'left', 'right', 'left'],
        ]);
      },
    );

    test(
      'connection execute uses a transient prepared statement and closes it',
      () async {
        final client = _FakeNativeClient();
        final connection = await FirebirdEndpoint(
          client: client,
          options: const FirebirdConnectionOptions(
            database: '/data/example.fdb',
          ),
        ).connect();

        final result = await connection.execute(
          r'SELECT * FROM account WHERE tenant_id = @tenant_id',
          parameters: FirebirdStatementParameters.named({'tenant_id': 44}),
        );

        expect(result.affectedRows, 1);
        expect(client.connection.statements, hasLength(1));
        expect(client.connection.statements.single.closeCallCount, 1);
      },
    );

    test(
      'connection execute can set a per-statement timeout before execution',
      () async {
        final client = _FakeNativeClient();
        final connection = await FirebirdEndpoint(
          client: client,
          options: const FirebirdConnectionOptions(
            database: '/data/example.fdb',
          ),
        ).connect();

        await connection.execute(
          r'SELECT * FROM account WHERE id = $1',
          parameters: FirebirdStatementParameters.positional([1]),
          timeout: const Duration(milliseconds: 12),
        );

        expect(
          client.connection.statements.single.timeout,
          const Duration(milliseconds: 12),
        );
      },
    );

    test(
      'closing a connection closes any still-open statements before the native connection',
      () async {
        final client = _FakeNativeClient();
        final connection = await FirebirdEndpoint(
          client: client,
          options: const FirebirdConnectionOptions(
            database: '/data/example.fdb',
          ),
        ).connect();

        await connection.prepare(r'SELECT * FROM account WHERE id = $1');
        await connection.prepare(r'SELECT * FROM account WHERE tenant_id = $1');

        await connection.close();

        expect(connection.isClosed, isTrue);
        expect(
          client.connection.statements.map(
            (statement) => statement.closeCallCount,
          ),
          everyElement(1),
        );
        expect(client.connection.closeCallCount, 1);
      },
    );

    test(
      'connection timeout and cancellation delegate to the native layer',
      () async {
        final client = _FakeNativeClient();
        final connection = await FirebirdEndpoint(
          client: client,
          options: const FirebirdConnectionOptions(
            database: '/data/example.fdb',
          ),
        ).connect();

        await connection.setStatementTimeout(const Duration(milliseconds: 90));
        final timeout = await connection.getStatementTimeout();
        await connection.cancelCurrentOperation();

        expect(timeout, const Duration(milliseconds: 90));
        expect(
          client.connection.statementTimeoutAssignments.last,
          const Duration(milliseconds: 90),
        );
        expect(client.connection.cancelModes, [FirebirdCancelMode.raise]);
      },
    );

    test('beginTransaction delegates readOnly to the native layer', () async {
      final client = _FakeNativeClient();
      final connection = await FirebirdEndpoint(
        client: client,
        options: const FirebirdConnectionOptions(database: '/data/example.fdb'),
      ).connect();

      final settings = const FirebirdTransactionSettings(
        isolationLevel: FirebirdTransactionIsolationLevel.serializable,
      );
      final transaction = await connection.beginTransaction(
        readOnly: true,
        settings: settings,
      );

      expect(client.connection.beginTransactionCalls, hasLength(1));
      expect(client.connection.beginTransactionCalls.single.readOnly, isTrue);
      expect(client.connection.beginTransactionCalls.single.settings, settings);
      expect(transaction.isClosed, isFalse);
      expect(transaction.readOnly, isTrue);
      expect(transaction.settings, settings);
    });

    test(
      'transaction execution binds values using the prepared layout and commit closes the native transaction',
      () async {
        final client = _FakeNativeClient();
        final connection = await FirebirdEndpoint(
          client: client,
          options: const FirebirdConnectionOptions(
            database: '/data/example.fdb',
          ),
        ).connect();

        final transaction = await connection.beginTransaction();
        final statement = await transaction.prepare(
          r'UPDATE account SET tenant_id = $2 WHERE id = $1',
        );

        await statement.execute(
          FirebirdStatementParameters.positional([44, 55]),
        );
        await transaction.commit();

        final nativeTransaction = client.connection.transactions.single;
        expect(nativeTransaction.preparedSql, [
          'UPDATE account SET tenant_id = ? WHERE id = ?',
        ]);
        expect(nativeTransaction.statements.single.executions, [
          [55, 44],
        ]);
        expect(nativeTransaction.commitCallCount, 1);
        expect(transaction.isCommitted, isTrue);
        expect(transaction.isClosed, isTrue);
      },
    );

    test('statement timeout delegates to the native statement layer', () async {
      final client = _FakeNativeClient();
      final connection = await FirebirdEndpoint(
        client: client,
        options: const FirebirdConnectionOptions(database: '/data/example.fdb'),
      ).connect();
      final statement = await connection.prepare(
        r'SELECT * FROM account WHERE id = $1',
      );

      await statement.setTimeout(const Duration(milliseconds: 33));
      final timeout = await statement.getTimeout();

      expect(timeout, const Duration(milliseconds: 33));
      expect(
        client.connection.statements.single.timeout,
        const Duration(milliseconds: 33),
      );
    });

    test('statement plan delegates to the native statement layer', () async {
      final client = _FakeNativeClient();
      final connection = await FirebirdEndpoint(
        client: client,
        options: const FirebirdConnectionOptions(database: '/data/example.fdb'),
      ).connect();
      final statement = await connection.prepare(
        r'SELECT * FROM account WHERE id = $1',
      );

      final detailedPlan = await statement.getPlan();
      final legacyPlan = await statement.getPlan(detailed: false);

      expect(detailedPlan, 'Select Expression -> Table "ACCOUNT" Full Scan');
      expect(legacyPlan, 'PLAN (ACCOUNT NATURAL)');
      expect(client.connection.statements.single.planRequests, [true, false]);
    });

    test(
      'closing a transaction rolls it back and releases the native transaction',
      () async {
        final client = _FakeNativeClient();
        final connection = await FirebirdEndpoint(
          client: client,
          options: const FirebirdConnectionOptions(
            database: '/data/example.fdb',
          ),
        ).connect();

        final transaction = await connection.beginTransaction();
        await transaction.prepare(r'SELECT * FROM account WHERE id = $1');

        await transaction.close();

        final nativeTransaction = client.connection.transactions.single;
        expect(nativeTransaction.rollbackCallCount, 1);
        expect(nativeTransaction.closeCallCount, 1);
        expect(transaction.isRolledBack, isTrue);
        expect(transaction.isClosed, isTrue);
      },
    );

    test('transaction savepoints delegate release and rollback', () async {
      final client = _FakeNativeClient();
      final connection = await FirebirdEndpoint(
        client: client,
        options: const FirebirdConnectionOptions(database: '/data/example.fdb'),
      ).connect();

      final transaction = await connection.beginTransaction();
      final savepoint = await transaction.createSavepoint();

      expect(savepoint.id, 'firebirdpod_sp_1');
      expect(client.connection.transactions.single.createdSavepointIds, [
        'firebirdpod_sp_1',
      ]);

      await savepoint.rollback();
      await savepoint.release();

      final nativeTransaction = client.connection.transactions.single;
      expect(nativeTransaction.rolledBackSavepointIds, ['firebirdpod_sp_1']);
      expect(nativeTransaction.releasedSavepointIds, ['firebirdpod_sp_1']);
      expect(savepoint.isReleased, isTrue);
    });

    test(
      'transaction runtime parameters are stored locally and applied natively',
      () async {
        final client = _FakeNativeClient();
        final connection = await FirebirdEndpoint(
          client: client,
          options: const FirebirdConnectionOptions(
            database: '/data/example.fdb',
          ),
        ).connect();

        final transaction = await connection.beginTransaction();
        await transaction.setRuntimeParameters((params) {
          return [
            params.transactionContextValue('REQUEST_ID', 'req-123'),
            params.transactionContextValues({
              'TENANT_ID': 44,
              'TRACE_ENABLED': true,
            }),
          ];
        });

        expect(transaction.runtimeParameters, {
          'REQUEST_ID': 'req-123',
          'TENANT_ID': 44,
          'TRACE_ENABLED': true,
        });
        expect(
          client.connection.transactions.single.transactionContextAssignments
              .map((entry) => '${entry.key}=${entry.value}')
              .toList(),
          ['REQUEST_ID=req-123', 'TENANT_ID=44', 'TRACE_ENABLED=true'],
        );
      },
    );

    test('transaction cancel delegates to rollback', () async {
      final client = _FakeNativeClient();
      final connection = await FirebirdEndpoint(
        client: client,
        options: const FirebirdConnectionOptions(database: '/data/example.fdb'),
      ).connect();

      final transaction = await connection.beginTransaction();
      await transaction.cancel();

      final nativeTransaction = client.connection.transactions.single;
      expect(nativeTransaction.rollbackCallCount, 1);
      expect(transaction.isRolledBack, isTrue);
      expect(transaction.isClosed, isTrue);
    });

    test(
      'transaction readConsistency captures transaction consistency state',
      () async {
        final client = _FakeNativeClient();
        final connection = await FirebirdEndpoint(
          client: client,
          options: const FirebirdConnectionOptions(
            database: '/data/example.fdb',
          ),
        ).connect();

        final transaction = await connection.beginTransaction(
          settings: const FirebirdTransactionSettings(
            isolationLevel: FirebirdTransactionIsolationLevel.readCommitted,
          ),
        );

        final state = await transaction.readConsistency.captureState();

        expect(state.attachmentId, 42);
        expect(state.transactionId, 100);
        expect(state.isolationLevelName, 'READ COMMITTED');
        expect(state.isReadOnly, isFalse);
        expect(state.monitorIsolationMode, 4);
        expect(
          state.monitorIsolationModeName,
          'READ COMMITTED READ CONSISTENCY',
        );
        expect(state.usesReadConsistency, isTrue);
      },
    );

    test(
      'resetForReuse closes open resources recreates retained state and restores the default timeout',
      () async {
        final client = _FakeNativeClient();
        final connection = await FirebirdEndpoint(
          client: client,
          options: const FirebirdConnectionOptions(
            database: '/data/example.fdb',
            statementTimeout: Duration(milliseconds: 250),
          ),
        ).connect();

        final statement = await connection.prepare(
          r'SELECT * FROM account WHERE id = $1',
        );
        final transaction = await connection.beginTransaction();

        await connection.setStatementTimeout(const Duration(milliseconds: 90));
        await connection.resetForReuse();

        final nativeTransaction = client.connection.transactions.single;
        expect(statement.isClosed, isTrue);
        expect(transaction.isRolledBack, isTrue);
        expect(transaction.isClosed, isTrue);
        expect(nativeTransaction.rollbackCallCount, 1);
        expect(nativeTransaction.closeCallCount, 1);
        expect(client.connection.resetRetainedStateCallCount, 1);
        expect(client.connection.statementTimeoutAssignments, [
          const Duration(milliseconds: 250),
          const Duration(milliseconds: 90),
          const Duration(milliseconds: 250),
        ]);
      },
    );

    test(
      'connection queryPlans explain uses a transient prepared statement and closes it',
      () async {
        final client = _FakeNativeClient();
        final connection = await FirebirdEndpoint(
          client: client,
          options: const FirebirdConnectionOptions(
            database: '/data/example.fdb',
          ),
        ).connect();

        final plan = await connection.queryPlans.explain(
          'SELECT @tenant_id FROM account WHERE id = @id',
        );

        expect(plan.normalizedSql, 'SELECT ? FROM account WHERE id = ?');
        expect(plan.parameterStyle, FirebirdParameterStyle.named);
        expect(plan.parameterCount, 2);
        expect(plan.detailed, isTrue);
        expect(plan.lines, ['Select Expression -> Table "ACCOUNT" Full Scan']);
        expect(client.connection.statements, hasLength(1));
        expect(client.connection.statements.single.closeCallCount, 1);
        expect(client.connection.statements.single.planRequests, [true]);
      },
    );

    test(
      'connection timeoutDiagnostics captures connection and statement timeout state',
      () async {
        final client = _FakeNativeClient();
        final connection = await FirebirdEndpoint(
          client: client,
          options: const FirebirdConnectionOptions(
            database: '/data/example.fdb',
          ),
        ).connect();

        await connection.setStatementTimeout(const Duration(milliseconds: 250));
        final connectionState = await connection.timeoutDiagnostics
            .captureConnectionState();

        final statement = await connection.prepare(
          r'SELECT * FROM account WHERE id = $1',
        );
        final statementState = await connection.timeoutDiagnostics
            .captureStatementState(statement);
        await statement.setTimeout(const Duration(milliseconds: 33));
        final overriddenStatementState = await connection.timeoutDiagnostics
            .captureStatementState(statement);

        expect(
          connectionState.configuredTimeout,
          const Duration(milliseconds: 250),
        );
        expect(connectionState.systemContextMilliseconds, 250);
        expect(statementState.configuredTimeout, isNull);
        expect(
          overriddenStatementState.configuredTimeout,
          const Duration(milliseconds: 33),
        );
        expect(
          overriddenStatementState.parameterStyle,
          FirebirdParameterStyle.positional,
        );
        expect(overriddenStatementState.parameterCount, 1);
      },
    );

    test(
      'connection timeoutDiagnostics observes timeout-classified execution and closes its transient statement',
      () async {
        final client = _FakeNativeClient();
        final connection = await FirebirdEndpoint(
          client: client,
          options: const FirebirdConnectionOptions(
            database: '/data/example.fdb',
          ),
        ).connect();

        await connection.setStatementTimeout(const Duration(milliseconds: 90));

        final observed = await connection.timeoutDiagnostics.observeExecution(
          'SELECT * FROM slow_timeout_probe',
          timeout: const Duration(milliseconds: 20),
        );

        expect(observed.succeeded, isFalse);
        expect(observed.timedOut, isTrue);
        expect(observed.cancelled, isTrue);
        expect(observed.connectionTimeout, const Duration(milliseconds: 90));
        expect(observed.connectionSystemContextMilliseconds, 90);
        expect(
          observed.requestedStatementTimeout,
          const Duration(milliseconds: 20),
        );
        expect(observed.statementTimeout, const Duration(milliseconds: 20));
        expect(observed.parameterStyle, FirebirdParameterStyle.none);
        expect(observed.parameterCount, 0);
        expect(observed.error?.operation, 'execute');
        expect(client.connection.statements, hasLength(2));
        expect(
          client.connection.statements.map(
            (statement) => statement.closeCallCount,
          ),
          everyElement(1),
        );
      },
    );

    test(
      'connection cancellationDiagnostics captures attachment and timeout state',
      () async {
        final client = _FakeNativeClient();
        final connection = await FirebirdEndpoint(
          client: client,
          options: const FirebirdConnectionOptions(
            database: '/data/example.fdb',
          ),
        ).connect();

        await connection.setStatementTimeout(const Duration(milliseconds: 120));
        final state = await connection.cancellationDiagnostics
            .captureConnectionState();

        expect(state.attachmentId, 42);
        expect(state.configuredTimeout, const Duration(milliseconds: 120));
      },
    );

    test(
      'connection cancellationDiagnostics reports no active operation for same-isolate raise requests',
      () async {
        final client = _FakeNativeClient();
        client.connection.raiseRequestReportsNothingToCancel = true;
        final connection = await FirebirdEndpoint(
          client: client,
          options: const FirebirdConnectionOptions(
            database: '/data/example.fdb',
          ),
        ).connect();

        final observed = await connection.cancellationDiagnostics
            .observeRequest();

        expect(observed.attachmentId, 42);
        expect(observed.requestAccepted, isFalse);
        expect(observed.reportedNothingToCancel, isTrue);
        expect(observed.cancelled, isFalse);
        expect(observed.timedOut, isFalse);
        expect(observed.requestedMode, FirebirdCancelMode.raise);
        expect(observed.requestError?.operation, 'cancel operation');
        expect(observed.connectionUsableAfterObservation, isTrue);
        expect(observed.connectionProbeError, isNull);
        expect(observed.probedAttachmentId, 42);
        expect(client.connection.cancelModes, [FirebirdCancelMode.raise]);
        expect(client.connection.statements, hasLength(2));
      },
    );

    test(
      'connection cancellationDiagnostics observes abort invalidating the attachment',
      () async {
        final client = _FakeNativeClient();
        final connection = await FirebirdEndpoint(
          client: client,
          options: const FirebirdConnectionOptions(
            database: '/data/example.fdb',
          ),
        ).connect();

        final observed = await connection.cancellationDiagnostics
            .observeRequest(mode: FirebirdCancelMode.abort);

        expect(observed.requestAccepted, isTrue);
        expect(observed.cancelled, isFalse);
        expect(observed.likelyForcedAbort, isTrue);
        expect(observed.connectionUsableAfterObservation, isFalse);
        expect(observed.connectionProbeError?.operation, 'prepare');
        expect(
          observed.connectionProbeError?.errorCodes,
          contains(fbclient.FbErrorCodes.isc_att_shutdown),
        );
      },
    );

    test(
      'closing a connection closes any still-open transactions before the native connection',
      () async {
        final client = _FakeNativeClient();
        final connection = await FirebirdEndpoint(
          client: client,
          options: const FirebirdConnectionOptions(
            database: '/data/example.fdb',
          ),
        ).connect();

        await connection.beginTransaction();

        await connection.close();

        expect(client.connection.transactions, hasLength(1));
        expect(client.connection.transactions.single.rollbackCallCount, 1);
        expect(client.connection.transactions.single.closeCallCount, 1);
        expect(client.connection.closeCallCount, 1);
      },
    );

    test(
      'generatedId picks the first returned column from a single-row result',
      () {
        const result = FirebirdExecutionResult(
          rows: [
            {'ID': 44, 'NAME': 'carol'},
          ],
        );

        expect(result.generatedId(), 44);
        expect(result.generatedId(columnName: 'NAME'), 'carol');
      },
    );
  });
}

class _FakeNativeClient implements FirebirdNativeClient {
  final List<FirebirdConnectionOptions> attachCalls = [];
  final _FakeNativeConnection connection = _FakeNativeConnection();

  @override
  Future<FirebirdNativeConnection> attach(
    FirebirdConnectionOptions options,
  ) async {
    attachCalls.add(options);
    return connection;
  }
}

class _FakeNativeConnection implements FirebirdNativeConnection {
  final List<String> preparedSql = [];
  final List<_FakeNativeStatement> statements = [];
  final List<_FakeBeginTransactionCall> beginTransactionCalls = [];
  final List<_FakeNativeTransaction> transactions = [];
  final List<Duration?> statementTimeoutAssignments = [];
  final List<FirebirdCancelMode> cancelModes = [];
  int attachmentId = 42;
  int resetRetainedStateCallCount = 0;
  int closeCallCount = 0;
  Duration? statementTimeout;
  FirebirdCancelMode? pendingCancelMode;
  bool isUsable = true;
  bool raiseRequestReportsNothingToCancel = false;

  @override
  Future<void> cancelOperation(FirebirdCancelMode mode) async {
    cancelModes.add(mode);
    if (mode == FirebirdCancelMode.raise &&
        raiseRequestReportsNothingToCancel) {
      throw FirebirdDatabaseException(
        operation: 'cancel operation',
        message: 'nothing to cancel',
        errorCodes: [335544933],
      );
    }
    switch (mode) {
      case FirebirdCancelMode.disable:
      case FirebirdCancelMode.enable:
        pendingCancelMode = null;
        break;
      case FirebirdCancelMode.raise:
        pendingCancelMode = mode;
        break;
      case FirebirdCancelMode.abort:
        pendingCancelMode = null;
        isUsable = false;
        break;
    }
  }

  @override
  Future<void> close() async {
    closeCallCount++;
  }

  @override
  Future<Duration?> getStatementTimeout() async => statementTimeout;

  @override
  Future<void> resetRetainedState() async {
    resetRetainedStateCallCount++;
  }

  @override
  Future<FirebirdNativeTransaction> beginTransaction({
    bool readOnly = false,
    FirebirdTransactionSettings settings = const FirebirdTransactionSettings(),
  }) async {
    beginTransactionCalls.add(
      _FakeBeginTransactionCall(readOnly: readOnly, settings: settings),
    );
    final transaction = _FakeNativeTransaction(
      transactionId: transactions.length + 100,
      readOnly: readOnly,
      settings: settings,
    );
    transactions.add(transaction);
    return transaction;
  }

  @override
  Future<void> setStatementTimeout(Duration? timeout) async {
    statementTimeout = timeout;
    statementTimeoutAssignments.add(timeout);
  }

  @override
  Future<FirebirdNativeStatement> prepareStatement(String sql) async {
    if (!isUsable) {
      throw FirebirdDatabaseException(
        operation: 'prepare',
        message: 'attachment shutdown',
        errorCodes: [fbclient.FbErrorCodes.isc_att_shutdown],
      );
    }

    preparedSql.add(sql);
    final statement = _FakeNativeStatement(ownerConnection: this);
    if (sql.contains('current_connection as ATTACHMENT_ID')) {
      statement.executeHandler = (_) async => FirebirdExecutionResult(
        rows: [
          {'ATTACHMENT_ID': attachmentId},
        ],
      );
    }
    if (sql.contains("rdb\$get_context('SYSTEM', 'STATEMENT_TIMEOUT')")) {
      statement.executeHandler = (_) async => FirebirdExecutionResult(
        rows: [
          {'RESULT_VALUE': statementTimeout?.inMilliseconds ?? 0},
        ],
      );
    }
    if (sql == 'SELECT * FROM slow_timeout_probe') {
      statement.executeHandler = (_) async {
        throw FirebirdDatabaseException(
          operation: 'execute',
          message: 'statement timeout',
          errorCodes: [
            fbclient.FbErrorCodes.isc_cancelled,
            fbclient.FbErrorCodes.isc_req_stmt_timeout,
          ],
        );
      };
    }
    statements.add(statement);
    return statement;
  }
}

class _FakeNativeTransaction implements FirebirdNativeTransaction {
  _FakeNativeTransaction({
    required this.transactionId,
    required this.readOnly,
    required this.settings,
  });

  final int transactionId;
  final bool readOnly;
  final FirebirdTransactionSettings settings;
  final List<String> preparedSql = [];
  final List<_FakeNativeStatement> statements = [];
  final List<String> createdSavepointIds = [];
  final List<String> releasedSavepointIds = [];
  final List<String> rolledBackSavepointIds = [];
  final List<MapEntry<String, Object>> transactionContextAssignments = [];
  int commitCallCount = 0;
  int rollbackCallCount = 0;
  int closeCallCount = 0;

  @override
  Future<void> close() async {
    closeCallCount++;
  }

  @override
  Future<void> commit() async {
    commitCallCount++;
  }

  @override
  Future<FirebirdNativeSavepoint> createSavepoint(String id) async {
    createdSavepointIds.add(id);
    return _FakeNativeSavepoint(id: id, transaction: this);
  }

  @override
  Future<FirebirdNativeStatement> prepareStatement(String sql) async {
    preparedSql.add(sql);
    final statement = _FakeNativeStatement();
    if (sql.contains('from mon\$transactions')) {
      statement.executeHandler = (_) async => FirebirdExecutionResult(
        rows: [
          {
            'ATTACHMENT_ID': 42,
            'TRANSACTION_ID': transactionId,
            'ISOLATION_LEVEL': _fakeIsolationLevelName(settings),
            'READ_ONLY': readOnly ? 'TRUE' : 'FALSE',
            'MONITOR_ISOLATION_MODE': _fakeMonitorIsolationMode(settings),
            'LOCK_TIMEOUT_SECONDS': -1,
          },
        ],
      );
    }
    statements.add(statement);
    return statement;
  }

  @override
  Future<void> rollback() async {
    rollbackCallCount++;
  }

  @override
  Future<void> setTransactionContext(String key, Object value) async {
    transactionContextAssignments.add(MapEntry<String, Object>(key, value));
  }
}

String _fakeIsolationLevelName(FirebirdTransactionSettings settings) {
  return switch (settings.isolationLevel) {
    FirebirdTransactionIsolationLevel.readUncommitted => 'READ COMMITTED',
    FirebirdTransactionIsolationLevel.readCommitted => 'READ COMMITTED',
    FirebirdTransactionIsolationLevel.repeatableRead => 'SNAPSHOT',
    FirebirdTransactionIsolationLevel.serializable => 'CONSISTENCY',
  };
}

int _fakeMonitorIsolationMode(FirebirdTransactionSettings settings) {
  return switch (settings.isolationLevel) {
    FirebirdTransactionIsolationLevel.readUncommitted => 4,
    FirebirdTransactionIsolationLevel.readCommitted => 4,
    FirebirdTransactionIsolationLevel.repeatableRead => 1,
    FirebirdTransactionIsolationLevel.serializable => 0,
  };
}

class _FakeNativeStatement implements FirebirdNativeStatement {
  _FakeNativeStatement({this.ownerConnection});

  final _FakeNativeConnection? ownerConnection;
  final List<List<Object?>> executions = [];
  final List<bool> planRequests = [];
  Future<FirebirdExecutionResult> Function(List<Object?> values)?
  executeHandler;
  int closeCallCount = 0;
  Duration? timeout;

  @override
  Future<String> getPlan({bool detailed = true}) async {
    planRequests.add(detailed);
    return detailed
        ? 'Select Expression -> Table "ACCOUNT" Full Scan'
        : 'PLAN (ACCOUNT NATURAL)';
  }

  @override
  Future<void> close() async {
    closeCallCount++;
    if (ownerConnection case final owner? when !owner.isUsable) {
      throw FirebirdDatabaseException(
        operation: 'close statement',
        message: 'attachment shutdown',
        errorCodes: [fbclient.FbErrorCodes.isc_att_shutdown],
      );
    }
  }

  @override
  Future<Duration?> getTimeout() async => timeout;

  @override
  Future<void> setTimeout(Duration? timeout) async {
    this.timeout = timeout;
  }

  @override
  Future<FirebirdExecutionResult> execute(List<Object?> values) async {
    executions.add(List<Object?>.from(values));
    if (ownerConnection case final owner?) {
      if (!owner.isUsable) {
        throw FirebirdDatabaseException(
          operation: 'execute',
          message: 'attachment shutdown',
          errorCodes: [fbclient.FbErrorCodes.isc_att_shutdown],
        );
      }
      if (owner.pendingCancelMode == FirebirdCancelMode.raise) {
        owner.pendingCancelMode = null;
        throw FirebirdDatabaseException(
          operation: 'execute',
          message: 'operation cancelled',
          errorCodes: [fbclient.FbErrorCodes.isc_cancelled],
        );
      }
    }
    if (executeHandler case final handler?) {
      return handler(values);
    }
    return const FirebirdExecutionResult(affectedRows: 1);
  }
}

class _FakeBeginTransactionCall {
  const _FakeBeginTransactionCall({
    required this.readOnly,
    required this.settings,
  });

  final bool readOnly;
  final FirebirdTransactionSettings settings;
}

class _FakeNativeSavepoint implements FirebirdNativeSavepoint {
  _FakeNativeSavepoint({
    required this.id,
    required _FakeNativeTransaction transaction,
  }) : _transaction = transaction;

  final _FakeNativeTransaction _transaction;

  @override
  final String id;

  @override
  Future<void> release() async {
    _transaction.releasedSavepointIds.add(id);
  }

  @override
  Future<void> rollback() async {
    _transaction.rolledBackSavepointIds.add(id);
  }
}
