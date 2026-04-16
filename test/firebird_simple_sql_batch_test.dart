import 'package:firebirdpod/firebirdpod.dart';
import 'package:test/test.dart';

void main() {
  group('splitFirebirdSimpleSqlBatch', () {
    test('splits ordinary top-level statements', () {
      expect(
        splitFirebirdSimpleSqlBatch('select 1; select 2;'),
        equals(['select 1;', 'select 2;']),
      );
    });

    test('keeps execute block as one statement', () {
      expect(
        splitFirebirdSimpleSqlBatch('''
execute block as
begin
  execute statement 'insert into test_table(id) values (1)';
  execute statement 'insert into test_table(id) values (2)';
end;
select 1 from rdb\$database;
'''),
        equals([
          '''
execute block as
begin
  execute statement 'insert into test_table(id) values (1)';
  execute statement 'insert into test_table(id) values (2)';
end;
'''
              .trim(),
          'select 1 from rdb\$database;',
        ]),
      );
    });

    test('ignores semicolons inside strings identifiers and comments', () {
      expect(
        splitFirebirdSimpleSqlBatch('''
select ';' as "semi;colon" from rdb\$database;
-- select ';' from nowhere;
/* keep; comment; together */
select 'done';
'''),
        equals([
          'select \';\' as "semi;colon" from rdb\$database;',
          '-- select \';\' from nowhere;\n/* keep; comment; together */\nselect \'done\';',
        ]),
      );
    });
  });
}
