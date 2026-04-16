import 'package:meta/meta.dart';

import 'firebird_parameter_style.dart';

@immutable
class FirebirdQueryCompileException implements Exception {
  const FirebirdQueryCompileException(
    this.message, {
    required this.query,
    required this.offset,
  });

  final String message;
  final String query;
  final int offset;

  @override
  String toString() {
    return 'FirebirdQueryCompileException('
        'message: $message, '
        'offset: $offset, '
        'query: $query'
        ')';
  }
}

@immutable
class FirebirdMissingNamedParameterException
    extends FirebirdQueryCompileException {
  FirebirdMissingNamedParameterException({
    required this.name,
    required super.query,
    required super.offset,
  }) : super('Missing value for named parameter @$name.');

  final String name;
}

@immutable
class FirebirdPositionalParameterOutOfRangeException
    extends FirebirdQueryCompileException {
  FirebirdPositionalParameterOutOfRangeException({
    required this.index,
    required this.boundCount,
    required super.query,
    required super.offset,
  }) : super(
         'Positional parameter \$$index is outside the bound value count '
         '($boundCount).',
       );

  final int index;
  final int boundCount;
}

@immutable
class FirebirdMissingStatementParametersException
    extends FirebirdQueryCompileException {
  FirebirdMissingStatementParametersException({
    required super.query,
    required super.offset,
  }) : super('The SQL statement expects parameters, but none were provided.');
}

@immutable
class FirebirdMixedParameterStyleException
    extends FirebirdQueryCompileException {
  FirebirdMixedParameterStyleException({
    required super.query,
    required super.offset,
  }) : super(
         'Do not mix positional and named parameters in the same SQL statement.',
       );
}

@immutable
class FirebirdParameterStyleMismatchException
    extends FirebirdQueryCompileException {
  FirebirdParameterStyleMismatchException({
    required this.expected,
    required this.provided,
    required super.query,
    required super.offset,
  }) : super(
         'The prepared SQL expects ${expected.name} parameters, '
         'but ${provided.name} parameters were provided.',
       );

  final FirebirdParameterStyle expected;
  final FirebirdParameterStyle provided;
}
