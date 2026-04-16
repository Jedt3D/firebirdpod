import 'dart:io';

import 'package:firebirdpod/firebirdpod.dart';
import 'package:test/test.dart';

void main() {
  test('direct fbclient transport can attach and run a small real query', () async {
    if (Platform.environment['FIREBIRDPOD_RUN_FBCLIENT_DIRECT'] != '1') {
      return;
    }

    final endpoint = FirebirdEndpoint(
      client: FirebirdFbClientNativeClient(
        fbClientLibraryPath:
            Platform.environment['FIREBIRDPOD_FBCLIENT_LIB'] ??
            '/Library/Frameworks/Firebird.framework/Versions/A/Resources/lib/libfbclient.dylib',
      ),
      options: FirebirdConnectionOptions(
        host: Platform.environment['FIREBIRDPOD_TEST_HOST'] ?? 'localhost',
        port: _readInt(Platform.environment['FIREBIRDPOD_TEST_PORT']),
        database:
            Platform.environment['FIREBIRDPOD_TEST_DATABASE'] ??
            '/Users/worajedt/GitHub/FireDart/databases/firebird/employee.fdb',
        user: Platform.environment['FIREBIRDPOD_TEST_USER'] ?? 'sysdba',
        password:
            Platform.environment['FIREBIRDPOD_TEST_PASSWORD'] ?? 'masterkey',
      ),
    );

    final connection = await endpoint.connect();
    addTearDown(connection.close);

    final result = await connection.execute('''
      select first 2 emp_no, first_name, hire_date
      from employee
      order by emp_no
      ''');

    expect(result.rows, hasLength(2));
    expect(result.rows.first.keys, contains('EMP_NO'));
    expect(result.rows.first['HIRE_DATE'], isA<DateTime>());

    final arithmeticResult = await connection.execute(r'''
      select cast($1 as integer) + cast($2 as integer) as result_value
      from rdb$database
      ''', parameters: FirebirdStatementParameters.positional([20, 22]));

    expect(arithmeticResult.rows.single['RESULT_VALUE'], 42);

    final updateResult = await connection.execute(r'''
      update employee
      set phone_ext = phone_ext
      where emp_no = $1
      ''', parameters: FirebirdStatementParameters.positional([2]));

    expect(updateResult.affectedRows, 1);
  });
}

int? _readInt(String? value) {
  if (value == null || value.isEmpty) return null;
  return int.parse(value);
}
