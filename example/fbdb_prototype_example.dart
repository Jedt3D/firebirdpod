import 'dart:io';

import 'package:firebirdpod/firebirdpod.dart';

Future<void> main() async {
  final database =
      Platform.environment['FIREBIRDPOD_EXAMPLE_DATABASE'] ??
      '/Users/worajedt/GitHub/FireDart/databases/firebird/employee.fdb';
  final host = Platform.environment['FIREBIRDPOD_EXAMPLE_HOST'] ?? 'localhost';
  final portValue = Platform.environment['FIREBIRDPOD_EXAMPLE_PORT'];
  final port = portValue == null ? null : int.parse(portValue);
  final user = Platform.environment['FIREBIRDPOD_EXAMPLE_USER'] ?? 'sysdba';
  final password =
      Platform.environment['FIREBIRDPOD_EXAMPLE_PASSWORD'] ?? 'masterkey';
  final fbClientLibraryPath =
      Platform.environment['FIREBIRDPOD_FBCLIENT_LIB'] ??
      '/Library/Frameworks/Firebird.framework/Versions/A/Resources/lib/libfbclient.dylib';

  final endpoint = FirebirdEndpoint(
    client: FirebirdFbdbPrototypeClient(
      fbClientLibraryPath: fbClientLibraryPath,
    ),
    options: FirebirdConnectionOptions(
      host: host,
      port: port,
      database: database,
      user: user,
      password: password,
    ),
  );

  final connection = await endpoint.connect();
  try {
    final employeeRows = await connection.execute('''
      select first 3 emp_no, first_name, last_name
      from employee
      order by emp_no
      ''');
    stdout.writeln('Employee sample rows:');
    for (final row in employeeRows.rows) {
      stdout.writeln(
        '  emp_no=${row['EMP_NO'] ?? row['emp_no']}, '
        'first_name=${row['FIRST_NAME'] ?? row['first_name']}, '
        'last_name=${row['LAST_NAME'] ?? row['last_name']}',
      );
    }

    final arithmeticRows = await connection.execute(r'''
      select cast($1 as integer) + cast($2 as integer) as result_value
      from rdb$database
      ''', parameters: FirebirdStatementParameters.positional([20, 22]));
    stdout.writeln('Arithmetic result rows: ${arithmeticRows.rows}');

    final updateResult = await connection.execute(r'''
      update employee
      set phone_ext = phone_ext
      where emp_no = $1
      ''', parameters: FirebirdStatementParameters.positional([2]));
    stdout.writeln('No-op update affected rows: ${updateResult.affectedRows}');
  } finally {
    await connection.close();
  }
}
