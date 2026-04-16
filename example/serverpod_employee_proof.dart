import 'dart:io';

import 'package:firebirdpod/src/serverpod_proof/firebird_serverpod_employee_proof.dart';

Future<void> main() async {
  final databasePath =
      Platform.environment['FIREBIRDPOD_EXAMPLE_DATABASE'] ??
      defaultFirebirdEmployeeProofDatabasePath;
  final host = Platform.environment['FIREBIRDPOD_EXAMPLE_HOST'] ?? 'localhost';
  final portValue = Platform.environment['FIREBIRDPOD_EXAMPLE_PORT'];
  final port = portValue == null ? 3050 : int.parse(portValue);
  final user = Platform.environment['FIREBIRDPOD_EXAMPLE_USER'] ?? 'sysdba';
  final password =
      Platform.environment['FIREBIRDPOD_EXAMPLE_PASSWORD'] ?? 'masterkey';
  final fbClientLibraryPath =
      Platform.environment['FIREBIRDPOD_FBCLIENT_LIB'] ??
      defaultFirebirdClientLibraryPath;

  final app = FirebirdServerpodEmployeeProofApp.create(
    databasePath: databasePath,
    host: host,
    port: port,
    user: user,
    password: password,
    fbClientLibraryPath: fbClientLibraryPath,
  );

  try {
    final overview = await app.databaseOverview();
    final employees = await app.listEmployees(limit: 5);
    final firstEmployee = employees.first;
    final employeeLookup = await app.lookupEmployee(
      employeeNumber: firstEmployee['employeeNumber'] as int,
    );

    stdout.writeln('Minimal Firebird-backed Serverpod app proof');
    stdout.writeln('Database: ${app.databaseConfig.name}');
    stdout.writeln(
      'Startup complete flag: ${app.pod.isStartupComplete} '
      '(expected false because this proof does not call pod.start())',
    );
    stdout.writeln(
      'Overview: employeeCount=${overview['employeeCount']}, '
      'countryCount=${overview['countryCount']}',
    );
    stdout.writeln('Sample employees:');
    for (final employee in employees) {
      stdout.writeln(
        '  #${employee['employeeNumber']} '
        '${employee['firstName']} ${employee['lastName']} '
        '(${employee['jobCountry']})',
      );
    }
    stdout.writeln('Lookup of first sample employee: $employeeLookup');
    stdout.writeln(
      'Note: this proof uses a real Serverpod object, session, and endpoint '
      'dispatch against employee.fdb. Full pod.start() against the sample '
      'database still belongs to the later Serverpod-owned schema bootstrap '
      'work.',
    );
  } finally {
    await app.close();
  }
}
