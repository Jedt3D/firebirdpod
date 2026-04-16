import 'package:firebirdpod/firebirdpod.dart';
import 'package:test/test.dart';

void main() {
  group('parseFirebirdSql', () {
    test('parses once and binds many times for named placeholders', () {
      final prepared = parseFirebirdSql(
        r'SELECT @tenant_id, @id, @tenant_id FROM account WHERE id = @id',
      );

      expect(prepared.sql, 'SELECT ?, ?, ? FROM account WHERE id = ?');
      expect(prepared.parameterStyle, FirebirdParameterStyle.named);
      expect(prepared.parameterCount, 4);

      final first = prepared.bind(
        FirebirdStatementParameters.named({'tenant_id': 10, 'id': 99}),
      );
      final second = prepared.bind(
        FirebirdStatementParameters.named({'tenant_id': 20, 'id': 77}),
      );

      expect(first.values, [10, 99, 10, 99]);
      expect(second.values, [20, 77, 20, 77]);
    });

    test('parses once and binds many times for positional placeholders', () {
      final prepared = parseFirebirdSql(
        r'SELECT $2, $1 FROM account WHERE group_id = $2',
      );

      expect(prepared.sql, 'SELECT ?, ? FROM account WHERE group_id = ?');
      expect(prepared.parameterStyle, FirebirdParameterStyle.positional);
      expect(prepared.parameterCount, 3);

      final first = prepared.bind(
        FirebirdStatementParameters.positional(['first', 'second']),
      );
      final second = prepared.bind(
        FirebirdStatementParameters.positional(['left', 'right']),
      );

      expect(first.values, ['second', 'first', 'second']);
      expect(second.values, ['right', 'left', 'right']);
    });

    test(
      'rejects mixed positional and named placeholders in one statement',
      () {
        expect(
          () => parseFirebirdSql(
            r'SELECT * FROM account WHERE id = $1 AND tenant_id = @tenant_id',
          ),
          throwsA(isA<FirebirdMixedParameterStyleException>()),
        );
      },
    );

    test('rejects binding named parameters to a positional statement', () {
      final prepared = parseFirebirdSql(r'SELECT * FROM account WHERE id = $1');

      expect(
        () => prepared.bind(FirebirdStatementParameters.named({'id': 1})),
        throwsA(
          isA<FirebirdParameterStyleMismatchException>()
              .having(
                (error) => error.expected,
                'expected',
                FirebirdParameterStyle.positional,
              )
              .having(
                (error) => error.provided,
                'provided',
                FirebirdParameterStyle.named,
              ),
        ),
      );
    });

    test('requires parameters when the prepared SQL expects placeholders', () {
      final prepared = parseFirebirdSql(r'SELECT * FROM account WHERE id = $1');

      expect(
        () => prepared.bind(null),
        throwsA(isA<FirebirdMissingStatementParametersException>()),
      );
    });
  });
}
