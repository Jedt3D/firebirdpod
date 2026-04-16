import 'package:serverpod/serverpod.dart';
import 'package:serverpod_database/serverpod_database.dart';

import '../serverpod/firebird_serverpod_config.dart';
import '../serverpod/firebird_serverpod_registration.dart';

const defaultFirebirdEmployeeProofDatabasePath =
    '/Users/worajedt/GitHub/FireDart/databases/firebird/employee.fdb';

const defaultFirebirdClientLibraryPath =
    '/Library/Frameworks/Firebird.framework/Versions/A/Resources/lib/libfbclient.dylib';

class FirebirdEmployeeDirectoryEndpoint extends Endpoint {
  Future<List<Map<String, Object?>>> listEmployees(
    Session session,
    int limit,
  ) async {
    final safeLimit = _normalizeLimit(limit);
    final result = await session.db.unsafeQuery('''
      select first $safeLimit
        emp_no,
        first_name,
        last_name,
        phone_ext,
        job_country,
        hire_date
      from employee
      order by emp_no
      ''');

    return result.map((row) => _mapEmployeeRow(row.toColumnMap())).toList();
  }

  Future<Map<String, Object?>?> lookupEmployee(
    Session session,
    int employeeNumber,
  ) async {
    final result = await session.db.unsafeQuery(
      '''
      select
        emp_no,
        first_name,
        last_name,
        phone_ext,
        job_country,
        hire_date
      from employee
      where emp_no = @employeeNumber
      ''',
      parameters: QueryParameters.named({'employeeNumber': employeeNumber}),
    );

    if (result.isEmpty) return null;
    return _mapEmployeeRow(result.first.toColumnMap());
  }

  Future<Map<String, Object?>> databaseOverview(Session session) async {
    return session.db.transaction(
      (transaction) async {
        final employeeCountResult = await session.db.unsafeQuery('''
        select count(*) as employee_count
        from employee
        ''', transaction: transaction);

        final countryCountResult = await session.db.unsafeQuery('''
        select count(*) as country_count
        from country
        ''', transaction: transaction);

        final firstEmployeeResult = await session.db.unsafeQuery('''
        select first 1
          emp_no,
          first_name,
          last_name,
          phone_ext,
          job_country,
          hire_date
        from employee
        order by emp_no
        ''', transaction: transaction);

        return {
          'employeeCount': _readInt(
            employeeCountResult.first.toColumnMap(),
            'EMPLOYEE_COUNT',
          ),
          'countryCount': _readInt(
            countryCountResult.first.toColumnMap(),
            'COUNTRY_COUNT',
          ),
          'sampleEmployee': firstEmployeeResult.isEmpty
              ? null
              : _mapEmployeeRow(firstEmployeeResult.first.toColumnMap()),
        };
      },
      settings: const TransactionSettings(
        isolationLevel: IsolationLevel.readCommitted,
      ),
    );
  }

  int _normalizeLimit(int limit) {
    if (limit <= 0) {
      throw RangeError.range(limit, 1, 50, 'limit');
    }

    return limit > 50 ? 50 : limit;
  }

  int _readInt(Map<String, dynamic> row, String columnName) {
    final value = row[columnName];
    if (value is int) return value;
    if (value is num) return value.toInt();
    throw StateError('Expected integer value in column $columnName.');
  }

  Map<String, Object?> _mapEmployeeRow(Map<String, dynamic> row) {
    return {
      'employeeNumber': row['EMP_NO'],
      'firstName': row['FIRST_NAME'],
      'lastName': row['LAST_NAME'],
      'phoneExtension': row['PHONE_EXT'],
      'jobCountry': row['JOB_COUNTRY'],
      'hireDate': row['HIRE_DATE'],
    };
  }
}

class FirebirdEmployeeProofEndpoints extends EndpointDispatch {
  static const endpointName = 'employeeDirectory';

  @override
  void initializeEndpoints(Server server) {
    final endpoints = <String, Endpoint>{
      endpointName: FirebirdEmployeeDirectoryEndpoint()
        ..initialize(server, endpointName, null),
    };

    connectors[endpointName] = EndpointConnector(
      name: endpointName,
      endpoint: endpoints[endpointName]!,
      methodConnectors: {
        'listEmployees': MethodConnector(
          name: 'listEmployees',
          params: {
            'limit': ParameterDescription(
              name: 'limit',
              type: getType<int>(),
              nullable: false,
            ),
          },
          call: (session, params) async =>
              (endpoints[endpointName]! as FirebirdEmployeeDirectoryEndpoint)
                  .listEmployees(session, params['limit'] as int),
        ),
        'lookupEmployee': MethodConnector(
          name: 'lookupEmployee',
          params: {
            'employeeNumber': ParameterDescription(
              name: 'employeeNumber',
              type: getType<int>(),
              nullable: false,
            ),
          },
          call: (session, params) async =>
              (endpoints[endpointName]! as FirebirdEmployeeDirectoryEndpoint)
                  .lookupEmployee(session, params['employeeNumber'] as int),
        ),
        'databaseOverview': MethodConnector(
          name: 'databaseOverview',
          params: const {},
          call: (session, params) async =>
              (endpoints[endpointName]! as FirebirdEmployeeDirectoryEndpoint)
                  .databaseOverview(session),
        ),
      },
    );
  }
}

class FirebirdEmployeeProofSerializationManager
    extends SerializationManagerServer {
  @override
  String getModuleName() => 'firebirdpodProof';

  @override
  Table? getTableForType(Type t) => null;

  @override
  List<TableDefinition> getTargetTableDefinitions() => const [];
}

ServerpodConfig buildFirebirdEmployeeProofConfig({
  required String databasePath,
  String host = 'localhost',
  int port = 3050,
  String user = 'sysdba',
  String password = 'masterkey',
  String charset = 'UTF8',
  String? role,
  String? fbClientLibraryPath,
}) {
  return ServerpodConfig(
    apiServer: ServerConfig(
      port: 0,
      publicScheme: 'http',
      publicHost: 'localhost',
      publicPort: 0,
    ),
    database: FirebirdServerpodDatabaseConfig(
      host: host,
      port: port,
      user: user,
      password: password,
      name: databasePath,
      charset: charset,
      role: role,
      fbClientLibraryPath: fbClientLibraryPath,
      defaultStatementTimeout: const Duration(seconds: 15),
      maxConnectionCount: 4,
    ),
    sessionLogs: SessionLogConfig(
      persistentEnabled: false,
      consoleEnabled: false,
      cleanupInterval: null,
      retentionPeriod: null,
      retentionCount: null,
    ),
    futureCallExecutionEnabled: false,
  );
}

class FirebirdServerpodEmployeeProofApp {
  FirebirdServerpodEmployeeProofApp._({
    required this.pod,
    required this.endpoints,
    required this.serializationManager,
    required this.databaseConfig,
  });

  final Serverpod pod;
  final FirebirdEmployeeProofEndpoints endpoints;
  final FirebirdEmployeeProofSerializationManager serializationManager;
  final FirebirdServerpodDatabaseConfig databaseConfig;

  static FirebirdServerpodEmployeeProofApp create({
    String databasePath = defaultFirebirdEmployeeProofDatabasePath,
    String host = 'localhost',
    int port = 3050,
    String user = 'sysdba',
    String password = 'masterkey',
    String charset = 'UTF8',
    String? role,
    String? fbClientLibraryPath = defaultFirebirdClientLibraryPath,
  }) {
    registerFirebirdServerpodDialect();

    final serializationManager = FirebirdEmployeeProofSerializationManager();
    final endpoints = FirebirdEmployeeProofEndpoints();
    final config = buildFirebirdEmployeeProofConfig(
      databasePath: databasePath,
      host: host,
      port: port,
      user: user,
      password: password,
      charset: charset,
      role: role,
      fbClientLibraryPath: fbClientLibraryPath,
    );

    final pod = Serverpod(
      ['-m', 'development'],
      serializationManager,
      endpoints,
      config: config,
    );

    return FirebirdServerpodEmployeeProofApp._(
      pod: pod,
      endpoints: endpoints,
      serializationManager: serializationManager,
      databaseConfig: config.database! as FirebirdServerpodDatabaseConfig,
    );
  }

  Future<List<Map<String, Object?>>> listEmployees({int limit = 5}) {
    return _call<List<Map<String, Object?>>>(
      'listEmployees',
      parameters: {'limit': limit},
    );
  }

  Future<Map<String, Object?>?> lookupEmployee({required int employeeNumber}) {
    return _call<Map<String, Object?>?>(
      'lookupEmployee',
      parameters: {'employeeNumber': employeeNumber},
    );
  }

  Future<Map<String, Object?>> databaseOverview() {
    return _call<Map<String, Object?>>('databaseOverview');
  }

  Future<T> _call<T>(
    String methodName, {
    Map<String, dynamic> parameters = const {},
  }) async {
    final session = await pod.createSession(enableLogging: false);
    try {
      final context = await endpoints.getMethodCallContext(
        createSessionCallback: (_) => session,
        endpointPath: FirebirdEmployeeProofEndpoints.endpointName,
        methodName: methodName,
        parameters: parameters,
        serializationManager: serializationManager,
      );

      final result = await context.method.call(session, context.arguments);
      return result as T;
    } finally {
      await session.close();
    }
  }

  Future<void> close() async {
    await pod.shutdown(exitProcess: false);
  }
}
