import 'dart:io';

import 'package:firebirdpod/firebirdpod.dart';
import 'package:test/test.dart';

void main() {
  group('direct fbclient transactions', () {
    test('rollback discards an explicit transaction update', () async {
      if (Platform.environment['FIREBIRDPOD_RUN_FBCLIENT_DIRECT'] != '1') {
        return;
      }

      final endpoint = _buildEndpoint();
      final writer = await endpoint.connect();
      addTearDown(writer.close);

      final originalPhoneExt = await _readPhoneExt(endpoint, 2);
      final changedPhoneExt = originalPhoneExt == '9999' ? '9988' : '9999';

      final transaction = await writer.beginTransaction();
      addTearDown(transaction.close);

      await transaction.execute(
        r'''
        update employee
        set phone_ext = $1
        where emp_no = $2
        ''',
        parameters: FirebirdStatementParameters.positional([
          changedPhoneExt,
          2,
        ]),
      );

      await transaction.rollback();

      final phoneExtAfterRollback = await _readPhoneExt(endpoint, 2);
      expect(phoneExtAfterRollback, originalPhoneExt);
    });

    test('commit persists an explicit transaction update', () async {
      if (Platform.environment['FIREBIRDPOD_RUN_FBCLIENT_DIRECT'] != '1') {
        return;
      }

      final endpoint = _buildEndpoint();
      final writer = await endpoint.connect();
      addTearDown(writer.close);

      final originalPhoneExt = await _readPhoneExt(endpoint, 2);
      final changedPhoneExt = originalPhoneExt == '9999' ? '9988' : '9999';

      addTearDown(() async {
        final cleanupConnection = await endpoint.connect();
        try {
          await cleanupConnection.execute(
            r'''
            update employee
            set phone_ext = $1
            where emp_no = $2
            ''',
            parameters: FirebirdStatementParameters.positional([
              originalPhoneExt,
              2,
            ]),
          );
        } finally {
          await cleanupConnection.close();
        }
      });

      final transaction = await writer.beginTransaction();
      addTearDown(transaction.close);

      await transaction.execute(
        r'''
        update employee
        set phone_ext = $1
        where emp_no = $2
        ''',
        parameters: FirebirdStatementParameters.positional([
          changedPhoneExt,
          2,
        ]),
      );

      await transaction.commit();

      final phoneExtAfterCommit = await _readPhoneExt(endpoint, 2);
      expect(phoneExtAfterCommit, changedPhoneExt);
    });
  });
}

FirebirdEndpoint _buildEndpoint() {
  return FirebirdEndpoint(
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
}

Future<String?> _readPhoneExt(FirebirdEndpoint endpoint, int employeeNumber) async {
  final connection = await endpoint.connect();
  try {
    final result = await connection.execute(
      r'''
      select phone_ext
      from employee
      where emp_no = $1
      ''',
      parameters: FirebirdStatementParameters.positional([employeeNumber]),
    );
    final row = result.rows.single;
    return row['PHONE_EXT'] as String?;
  } finally {
    await connection.close();
  }
}

int? _readInt(String? value) {
  if (value == null || value.isEmpty) return null;
  return int.parse(value);
}
