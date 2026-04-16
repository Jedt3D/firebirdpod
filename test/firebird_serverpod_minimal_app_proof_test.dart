import 'package:firebirdpod/src/serverpod_proof/firebird_serverpod_employee_proof.dart';
import 'package:serverpod/serverpod.dart';
import 'package:test/test.dart';

import 'firebird_test_support.dart';

void main() {
  group('Firebird Serverpod minimal app proof', () {
    late FirebirdServerpodEmployeeProofApp app;

    setUpAll(() {
      if (!shouldRunDirectIntegrationTests()) {
        return;
      }

      app = FirebirdServerpodEmployeeProofApp.create(
        databasePath: firebirdTestDatabasePath(),
        user: firebirdTestUser(),
        password: firebirdTestPassword(),
        fbClientLibraryPath: firebirdClientLibraryPath(),
      );
    });

    tearDownAll(() async {
      if (!shouldRunDirectIntegrationTests()) {
        return;
      }

      await app.close();
    });

    test(
      'dispatches a basic endpoint against employee.fdb',
      () async {
        if (!shouldRunDirectIntegrationTests()) {
          return;
        }

        final overview = await app.databaseOverview();
        final employees = await app.listEmployees(limit: 3);
        final lookup = await app.lookupEmployee(
          employeeNumber: employees.first['employeeNumber']! as int,
        );

        expect(app.pod, isA<Serverpod>());
        expect(app.pod.isStartupComplete, isFalse);
        expect(overview['employeeCount'], greaterThan(0));
        expect(overview['countryCount'], greaterThan(0));
        expect(overview['sampleEmployee'], isA<Map<String, Object?>>());
        expect(employees, hasLength(3));
        expect(
          employees.first.keys,
          containsAll([
            'employeeNumber',
            'firstName',
            'lastName',
            'phoneExtension',
            'jobCountry',
            'hireDate',
          ]),
        );
        expect(lookup, isNotNull);
        expect(
          lookup!['employeeNumber'],
          equals(employees.first['employeeNumber']),
        );
      },
      skip: shouldRunDirectIntegrationTests()
          ? false
          : 'set FIREBIRDPOD_RUN_FBCLIENT_DIRECT=1 to run live Firebird proofs',
    );

    test(
      'rejects invalid endpoint parameters through the proof app',
      () async {
        if (!shouldRunDirectIntegrationTests()) {
          return;
        }

        expect(app.listEmployees(limit: 0), throwsA(isA<RangeError>()));
      },
      skip: shouldRunDirectIntegrationTests()
          ? false
          : 'set FIREBIRDPOD_RUN_FBCLIENT_DIRECT=1 to run live Firebird proofs',
    );

    test(
      'starts a real Serverpod after bootstrapping the Firebird runtime table',
      () async {
        if (!shouldRunDirectIntegrationTests()) {
          return;
        }

        final startedApp = FirebirdServerpodEmployeeProofApp.create(
          databasePath: firebirdTestDatabasePath(),
          user: firebirdTestUser(),
          password: firebirdTestPassword(),
          fbClientLibraryPath: firebirdClientLibraryPath(),
        );

        try {
          await startedApp.startWithBootstrap();

          expect(startedApp.pod, isA<Serverpod>());
          expect(startedApp.pod.isStartupComplete, isTrue);
          expect(await startedApp.runtimeSettingsRowCount(), greaterThan(0));

          final overview = await startedApp.databaseOverview();
          expect(overview['employeeCount'], greaterThan(0));
        } finally {
          await startedApp.close();
        }
      },
      skip: shouldRunDirectIntegrationTests()
          ? false
          : 'set FIREBIRDPOD_RUN_FBCLIENT_DIRECT=1 to run live Firebird proofs',
    );
  });
}
