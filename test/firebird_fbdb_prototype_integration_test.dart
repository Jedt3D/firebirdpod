import 'dart:io';

import 'package:firebirdpod/firebirdpod.dart';
import 'package:test/test.dart';

void main() {
  test('fbdb prototype can attach and run a small real query', () async {
    if (Platform.environment['FIREBIRDPOD_RUN_FBDB_PROTOTYPE'] != '1') {
      return;
    }

    final endpoint = _buildEndpoint();

    final connection = await endpoint.connect();
    addTearDown(connection.close);

    final result = await connection.execute('''
      select first 2 emp_no, first_name
      from employee
      order by emp_no
      ''');

    expect(result.rows, hasLength(2));
    expect(result.rows.first.keys, contains('EMP_NO'));

    final updateResult = await connection.execute(r'''
      update employee
      set phone_ext = phone_ext
      where emp_no = $1
      ''', parameters: FirebirdStatementParameters.positional([2]));

    expect(updateResult.affectedRows, 1);
  });

  test(
    'fbdb prototype maps readCommitted to read consistency semantics',
    () async {
      if (Platform.environment['FIREBIRDPOD_RUN_FBDB_PROTOTYPE'] != '1') {
        return;
      }

      final endpoint = _buildEndpoint();
      final connection = await endpoint.connect();
      addTearDown(connection.close);

      final transaction = await connection.beginTransaction(
        settings: const FirebirdTransactionSettings(
          isolationLevel: FirebirdTransactionIsolationLevel.readCommitted,
        ),
      );
      addTearDown(transaction.close);

      final result = await transaction.execute('''
      select mon\$isolation_mode as isolation_mode
      from mon\$transactions
      where mon\$transaction_id = current_transaction
      ''');

      expect(_readIntValue(result.singleRow?['ISOLATION_MODE']), 4);

      await transaction.rollback();
    },
  );
}

FirebirdEndpoint _buildEndpoint() {
  return FirebirdEndpoint(
    client: FirebirdFbdbPrototypeClient(
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
}

int? _readInt(String? value) {
  if (value == null || value.isEmpty) return null;
  return int.parse(value);
}

int _readIntValue(Object? value) {
  if (value is int) return value;
  if (value is BigInt) return value.toInt();
  throw StateError('Unexpected integer value type: ${value.runtimeType}');
}
