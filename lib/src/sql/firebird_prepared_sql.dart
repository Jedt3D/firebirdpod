import 'package:meta/meta.dart';

import 'compiled_firebird_sql.dart';
import 'firebird_parameter_style.dart';
import 'firebird_query_compile_exception.dart';
import 'firebird_statement_parameters.dart';

@immutable
class FirebirdPreparedSql {
  const FirebirdPreparedSql.internal({
    required this.sourceSql,
    required this.sql,
    required this.parameterStyle,
    required List<Object> parameterLayout,
  }) : _parameterLayout = parameterLayout;

  final String sourceSql;
  final String sql;
  final FirebirdParameterStyle parameterStyle;
  final List<Object> _parameterLayout;

  int get parameterCount => _parameterLayout.length;

  CompiledFirebirdSql bind(FirebirdStatementParameters? parameters) {
    if (_parameterLayout.isEmpty) {
      return CompiledFirebirdSql(sql: sql, values: const []);
    }

    if (parameters == null) {
      throw FirebirdMissingStatementParametersException(
        query: sourceSql,
        offset: 0,
      );
    }

    return switch (parameters) {
      FirebirdPositionalParameters(:final parameters) => _bindPositional(
        parameters,
      ),
      FirebirdNamedParameters(:final parameters) => _bindNamed(parameters),
    };
  }

  CompiledFirebirdSql _bindPositional(List<Object?> parameters) {
    if (parameterStyle != FirebirdParameterStyle.positional) {
      throw FirebirdParameterStyleMismatchException(
        expected: parameterStyle,
        provided: FirebirdParameterStyle.positional,
        query: sourceSql,
        offset: 0,
      );
    }

    final values = <Object?>[];
    for (final entry in _parameterLayout) {
      final index = entry as int;
      if (index > parameters.length) {
        throw FirebirdPositionalParameterOutOfRangeException(
          index: index,
          boundCount: parameters.length,
          query: sourceSql,
          offset: 0,
        );
      }
      values.add(parameters[index - 1]);
    }

    return CompiledFirebirdSql(sql: sql, values: values);
  }

  CompiledFirebirdSql _bindNamed(Map<String, Object?> parameters) {
    if (parameterStyle != FirebirdParameterStyle.named) {
      throw FirebirdParameterStyleMismatchException(
        expected: parameterStyle,
        provided: FirebirdParameterStyle.named,
        query: sourceSql,
        offset: 0,
      );
    }

    final values = <Object?>[];
    for (final entry in _parameterLayout) {
      final name = entry as String;
      if (!parameters.containsKey(name)) {
        throw FirebirdMissingNamedParameterException(
          name: name,
          query: sourceSql,
          offset: 0,
        );
      }
      values.add(parameters[name]);
    }

    return CompiledFirebirdSql(sql: sql, values: values);
  }
}
