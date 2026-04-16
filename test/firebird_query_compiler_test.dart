import 'package:firebirdpod/firebirdpod.dart';
import 'package:test/test.dart';

void main() {
  group('compileFirebirdSql', () {
    test('leaves SQL unchanged when no parameters are supplied', () {
      const sql = r"SELECT * FROM example WHERE id = $1 AND note = '@tag'";

      final compiled = compileFirebirdSql(sql, null);

      expect(compiled.sql, sql);
      expect(compiled.values, isEmpty);
    });

    test('rewrites positional placeholders to question marks', () {
      final compiled = compileFirebirdSql(
        r'SELECT * FROM example WHERE id = $1 AND status = $2',
        FirebirdStatementParameters.positional([42, 'open']),
      );

      expect(compiled.sql, 'SELECT * FROM example WHERE id = ? AND status = ?');
      expect(compiled.values, [42, 'open']);
    });

    test(
      'reorders positional values by placeholder occurrence instead of input order',
      () {
        final compiled = compileFirebirdSql(
          r'SELECT $2, $1, $2',
          FirebirdStatementParameters.positional(['first', 'second']),
        );

        expect(compiled.sql, 'SELECT ?, ?, ?');
        expect(compiled.values, ['second', 'first', 'second']);
      },
    );

    test(
      'does not rewrite placeholder-looking text inside strings comments or quoted identifiers',
      () {
        final compiled = compileFirebirdSql(
          r'''SELECT $1, "keep$2"
FROM example
WHERE note = 'ignore $2'
  AND code = $2
  -- preserve $1 here
  /* preserve @name here */''',
          FirebirdStatementParameters.positional(['left', 'right']),
        );

        expect(compiled.sql, r'''SELECT ?, "keep$2"
FROM example
WHERE note = 'ignore $2'
  AND code = ?
  -- preserve $1 here
  /* preserve @name here */''');
        expect(compiled.values, ['left', 'right']);
      },
    );

    test('does not treat dollar signs inside identifiers as placeholders', () {
      final compiled = compileFirebirdSql(
        r'SELECT foo$1, bar FROM example WHERE id = $1',
        FirebirdStatementParameters.positional([99]),
      );

      expect(compiled.sql, 'SELECT foo\$1, bar FROM example WHERE id = ?');
      expect(compiled.values, [99]);
    });

    test(
      'throws when a positional placeholder references an unbound index',
      () {
        expect(
          () => compileFirebirdSql(
            r'SELECT * FROM example WHERE id = $2',
            FirebirdStatementParameters.positional([1]),
          ),
          throwsA(
            isA<FirebirdPositionalParameterOutOfRangeException>()
                .having((error) => error.index, 'index', 2)
                .having((error) => error.boundCount, 'boundCount', 1),
          ),
        );
      },
    );

    test('throws when a positional placeholder starts at zero', () {
      expect(
        () => compileFirebirdSql(
          r'SELECT * FROM example WHERE id = $0',
          FirebirdStatementParameters.positional([1]),
        ),
        throwsA(
          isA<FirebirdQueryCompileException>().having(
            (error) => error.message,
            'message',
            'Positional parameters must start at \$1.',
          ),
        ),
      );
    });

    test(
      'rewrites named placeholders and preserves first-appearance binding order',
      () {
        final compiled = compileFirebirdSql(
          r'SELECT @status, @id, @status',
          FirebirdStatementParameters.named({'id': 7, 'status': 'ready'}),
        );

        expect(compiled.sql, 'SELECT ?, ?, ?');
        expect(compiled.values, ['ready', 7, 'ready']);
      },
    );

    test('throws when a named placeholder has no supplied value', () {
      expect(
        () => compileFirebirdSql(
          r'SELECT * FROM example WHERE id = @id AND tenant = @tenant',
          FirebirdStatementParameters.named({'id': 1}),
        ),
        throwsA(
          isA<FirebirdMissingNamedParameterException>().having(
            (error) => error.name,
            'name',
            'tenant',
          ),
        ),
      );
    });

    test(
      'does not rewrite named placeholders inside strings comments or quoted identifiers',
      () {
        final compiled = compileFirebirdSql(
          r'''SELECT @id, "@tenant"
FROM example
WHERE note = 'ignore @tenant'
  AND tenant_id = @tenant
  -- keep @audit_tag
  /* keep @comment_tag */''',
          FirebirdStatementParameters.named({'id': 10, 'tenant': 20}),
        );

        expect(compiled.sql, r'''SELECT ?, "@tenant"
FROM example
WHERE note = 'ignore @tenant'
  AND tenant_id = ?
  -- keep @audit_tag
  /* keep @comment_tag */''');
        expect(compiled.values, [10, 20]);
      },
    );
  });
}
