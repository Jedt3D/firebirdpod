import 'package:firebirdpod/firebirdpod.dart';
import 'package:test/test.dart';

import 'firebird_test_support.dart';

void main() {
  group('Firebird query plans', () {
    test(
      'captures detailed and legacy plans for a parameterized lookup',
      () async {
        if (!shouldRunDirectIntegrationTests()) {
          markTestSkipped(
            'set FIREBIRDPOD_RUN_FBCLIENT_DIRECT=1 to run live Firebird query-plan tests',
          );
          return;
        }

        final connection = await buildDirectEndpoint(
          database: firebirdTestDatabasePath(),
        ).connect();
        addTearDown(connection.close);

        final detailedPlan = await connection.queryPlans.explain(
          'select first_name from employee where emp_no = @empNo',
        );
        final legacyPlan = await connection.queryPlans.explain(
          'select first_name from employee where emp_no = @empNo',
          detailed: false,
        );

        expect(
          detailedPlan.normalizedSql,
          'select first_name from employee where emp_no = ?',
        );
        expect(detailedPlan.parameterStyle, FirebirdParameterStyle.named);
        expect(detailedPlan.parameterCount, 1);
        expect(detailedPlan.detailed, isTrue);
        expect(detailedPlan.plan, isNotEmpty);
        expect(detailedPlan.lines, isNotEmpty);
        expect(detailedPlan.plan.toUpperCase(), contains('EMPLOYEE'));

        expect(legacyPlan.detailed, isFalse);
        expect(legacyPlan.plan, isNotEmpty);
        expect(legacyPlan.plan.toUpperCase(), contains('PLAN'));
        expect(legacyPlan.plan.toUpperCase(), contains('EMPLOYEE'));
      },
    );

    test('captures a multi-source explained plan for a join query', () async {
      if (!shouldRunDirectIntegrationTests()) {
        markTestSkipped(
          'set FIREBIRDPOD_RUN_FBCLIENT_DIRECT=1 to run live Firebird query-plan tests',
        );
        return;
      }

      final connection = await buildDirectEndpoint(
        database: firebirdTestDatabasePath(),
      ).connect();
      addTearDown(connection.close);

      final plan = await connection.queryPlans.explain('''
        select e.emp_no, p.proj_id
        from employee e
        join employee_project ep on ep.emp_no = e.emp_no
        join project p on p.proj_id = ep.proj_id
        where e.emp_no = 107
        ''');
      final normalizedPlan = plan.plan.toUpperCase();

      expect(plan.detailed, isTrue);
      expect(plan.lines.length, greaterThan(1));
      expect(normalizedPlan, contains('EMPLOYEE'));
      expect(normalizedPlan, contains('EMPLOYEE_PROJECT'));
      expect(normalizedPlan, contains('PROJECT'));
    });
  });
}
