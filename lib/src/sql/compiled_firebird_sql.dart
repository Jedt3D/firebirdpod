import 'package:meta/meta.dart';

@immutable
class CompiledFirebirdSql {
  const CompiledFirebirdSql({required this.sql, required this.values});

  final String sql;
  final List<Object?> values;
}
