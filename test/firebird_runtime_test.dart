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

      expect(
        client.connection.statementTimeoutAssignments,
        [const Duration(milliseconds: 250)],
      );
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
          options: const FirebirdConnectionOptions(database: '/data/example.fdb'),
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

    test('connection timeout and cancellation delegate to the native layer', () async {
      final client = _FakeNativeClient();
      final connection = await FirebirdEndpoint(
        client: client,
        options: const FirebirdConnectionOptions(database: '/data/example.fdb'),
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
    });

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
          options: const FirebirdConnectionOptions(database: '/data/example.fdb'),
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

    test(
      'closing a transaction rolls it back and releases the native transaction',
      () async {
        final client = _FakeNativeClient();
        final connection = await FirebirdEndpoint(
          client: client,
          options: const FirebirdConnectionOptions(database: '/data/example.fdb'),
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
      expect(
        client.connection.transactions.single.createdSavepointIds,
        ['firebirdpod_sp_1'],
      );

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
          [
            'REQUEST_ID=req-123',
            'TENANT_ID=44',
            'TRACE_ENABLED=true',
          ],
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
      'closing a connection closes any still-open transactions before the native connection',
      () async {
        final client = _FakeNativeClient();
        final connection = await FirebirdEndpoint(
          client: client,
          options: const FirebirdConnectionOptions(database: '/data/example.fdb'),
        ).connect();

        await connection.beginTransaction();

        await connection.close();

        expect(client.connection.transactions, hasLength(1));
        expect(client.connection.transactions.single.rollbackCallCount, 1);
        expect(client.connection.transactions.single.closeCallCount, 1);
        expect(client.connection.closeCallCount, 1);
      },
    );

    test('generatedId picks the first returned column from a single-row result', () {
      const result = FirebirdExecutionResult(
        rows: [
          {'ID': 44, 'NAME': 'carol'},
        ],
      );

      expect(result.generatedId(), 44);
      expect(result.generatedId(columnName: 'NAME'), 'carol');
    });
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
  int resetRetainedStateCallCount = 0;
  int closeCallCount = 0;
  Duration? statementTimeout;

  @override
  Future<void> cancelOperation(FirebirdCancelMode mode) async {
    cancelModes.add(mode);
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
    final transaction = _FakeNativeTransaction();
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
    preparedSql.add(sql);
    final statement = _FakeNativeStatement();
    statements.add(statement);
    return statement;
  }
}

class _FakeNativeTransaction implements FirebirdNativeTransaction {
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

class _FakeNativeStatement implements FirebirdNativeStatement {
  final List<List<Object?>> executions = [];
  int closeCallCount = 0;
  Duration? timeout;

  @override
  Future<void> close() async {
    closeCallCount++;
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
