import 'compiled_firebird_sql.dart';
import 'firebird_parameter_style.dart';
import 'firebird_prepared_sql.dart';
import 'firebird_query_compile_exception.dart';
import 'firebird_statement_parameters.dart';

typedef _LiteralOrComment = ({String text, int end});

CompiledFirebirdSql compileFirebirdSql(
  String query,
  FirebirdStatementParameters? parameters,
) {
  if (parameters == null) {
    return CompiledFirebirdSql(sql: query, values: const []);
  }

  return parseFirebirdSql(query).bind(parameters);
}

FirebirdPreparedSql parseFirebirdSql(String query) {
  final parameterLayout = <Object>[];
  var parameterStyle = FirebirdParameterStyle.none;

  final sql = _rewriteExecutableSql(
    query,
    (codeSlice, sliceStart) => _rewriteCodeSlice(
      codeSlice,
      sliceStart,
      query,
      parameterLayout,
      (style, offset) {
        if (parameterStyle == FirebirdParameterStyle.none) {
          parameterStyle = style;
          return;
        }

        if (parameterStyle != style) {
          throw FirebirdMixedParameterStyleException(
            query: query,
            offset: offset,
          );
        }
      },
    ),
  );

  return FirebirdPreparedSql.internal(
    sourceSql: query,
    sql: sql,
    parameterStyle: parameterStyle,
    parameterLayout: parameterLayout,
  );
}

String _rewriteExecutableSql(
  String query,
  String Function(String codeSlice, int sliceStart) rewriteCodeSlice,
) {
  final out = StringBuffer();
  var index = 0;
  while (index < query.length) {
    final literal = _tryReadQuotedLiteralOrComment(query, index);
    if (literal != null) {
      out.write(literal.text);
      index = literal.end;
      continue;
    }

    final end = _indexOfNextQuotedLiteralOrComment(query, index);
    out.write(rewriteCodeSlice(query.substring(index, end), index));
    index = end;
  }
  return out.toString();
}

String _rewriteCodeSlice(
  String codeSlice,
  int sliceStart,
  String query,
  List<Object> parameterLayout,
  void Function(FirebirdParameterStyle style, int offset) registerStyle,
) {
  final out = StringBuffer();
  var index = 0;

  while (index < codeSlice.length) {
    final codeUnit = codeSlice.codeUnitAt(index);

    if (codeUnit == 0x24 &&
        _hasPlaceholderLeftBoundary(codeSlice, index) &&
        index + 1 < codeSlice.length &&
        _isDigit(codeSlice.codeUnitAt(index + 1))) {
      var digitEnd = index + 1;
      while (digitEnd < codeSlice.length &&
          _isDigit(codeSlice.codeUnitAt(digitEnd))) {
        digitEnd++;
      }

      if (_hasIdentifierCharOnRight(codeSlice, digitEnd)) {
        out.writeCharCode(codeUnit);
        index++;
        continue;
      }

      final parameterIndex = int.parse(
        codeSlice.substring(index + 1, digitEnd),
      );
      if (parameterIndex < 1) {
        throw FirebirdQueryCompileException(
          'Positional parameters must start at \$1.',
          query: query,
          offset: sliceStart + index,
        );
      }

      registerStyle(FirebirdParameterStyle.positional, sliceStart + index);
      parameterLayout.add(parameterIndex);
      out.write('?');
      index = digitEnd;
      continue;
    }

    if (codeUnit == 0x40 && _hasPlaceholderLeftBoundary(codeSlice, index)) {
      final parameterName = _tryReadNamedParameter(codeSlice, index);
      if (parameterName != null) {
        registerStyle(FirebirdParameterStyle.named, sliceStart + index);
        parameterLayout.add(parameterName);
        out.write('?');
        index += parameterName.length + 1;
        continue;
      }
    }

    out.writeCharCode(codeUnit);
    index++;
  }

  return out.toString();
}

String? _tryReadNamedParameter(String codeSlice, int atIndex) {
  final start = atIndex + 1;
  if (start >= codeSlice.length) return null;
  if (!_isNamedParameterStartChar(codeSlice.codeUnitAt(start))) return null;

  var end = start + 1;
  while (end < codeSlice.length &&
      _isNamedParameterContinuationChar(codeSlice.codeUnitAt(end))) {
    end++;
  }

  return codeSlice.substring(start, end);
}

bool _hasPlaceholderLeftBoundary(String codeSlice, int index) {
  if (index == 0) return true;
  return !_isIdentifierChar(codeSlice.codeUnitAt(index - 1));
}

bool _hasIdentifierCharOnRight(String codeSlice, int index) {
  if (index >= codeSlice.length) return false;
  return _isIdentifierChar(codeSlice.codeUnitAt(index));
}

bool _isIdentifierChar(int codeUnit) {
  return _isAsciiLetter(codeUnit) ||
      _isDigit(codeUnit) ||
      codeUnit == 0x5F ||
      codeUnit == 0x24;
}

bool _isNamedParameterStartChar(int codeUnit) {
  return _isAsciiLetter(codeUnit) || codeUnit == 0x5F;
}

bool _isNamedParameterContinuationChar(int codeUnit) {
  return _isAsciiLetter(codeUnit) || _isDigit(codeUnit) || codeUnit == 0x5F;
}

bool _isAsciiLetter(int codeUnit) {
  return (codeUnit >= 0x41 && codeUnit <= 0x5A) ||
      (codeUnit >= 0x61 && codeUnit <= 0x7A);
}

bool _isDigit(int codeUnit) => codeUnit >= 0x30 && codeUnit <= 0x39;

_LiteralOrComment? _tryReadQuotedLiteralOrComment(String sql, int index) {
  if (index >= sql.length) return null;
  switch (sql.codeUnitAt(index)) {
    case 0x27:
      final end = _skipSingleQuotedString(sql, index);
      return (text: sql.substring(index, end), end: end);
    case 0x22:
      final end = _skipDoubleQuotedIdentifier(sql, index);
      return (text: sql.substring(index, end), end: end);
    case 0x2D:
      if (index + 1 < sql.length && sql.codeUnitAt(index + 1) == 0x2D) {
        var end = index + 2;
        while (end < sql.length) {
          final codeUnit = sql.codeUnitAt(end);
          if (codeUnit == 0x0A || codeUnit == 0x0D) break;
          end++;
        }
        return (text: sql.substring(index, end), end: end);
      }
      return null;
    case 0x2F:
      if (index + 1 < sql.length && sql.codeUnitAt(index + 1) == 0x2A) {
        var end = index + 2;
        while (end < sql.length) {
          if (sql.codeUnitAt(end) == 0x2A &&
              end + 1 < sql.length &&
              sql.codeUnitAt(end + 1) == 0x2F) {
            end += 2;
            break;
          }
          end++;
        }
        return (text: sql.substring(index, end), end: end);
      }
      return null;
    default:
      return null;
  }
}

int _indexOfNextQuotedLiteralOrComment(String sql, int index) {
  var current = index;
  while (current < sql.length) {
    final codeUnit = sql.codeUnitAt(current);
    if (codeUnit == 0x27 || codeUnit == 0x22) return current;
    if (codeUnit == 0x2D &&
        current + 1 < sql.length &&
        sql.codeUnitAt(current + 1) == 0x2D) {
      return current;
    }
    if (codeUnit == 0x2F &&
        current + 1 < sql.length &&
        sql.codeUnitAt(current + 1) == 0x2A) {
      return current;
    }
    current++;
  }
  return current;
}

int _skipSingleQuotedString(String sql, int openQuoteIndex) {
  var index = openQuoteIndex + 1;
  while (index < sql.length) {
    if (sql.codeUnitAt(index) == 0x27) {
      if (index + 1 < sql.length && sql.codeUnitAt(index + 1) == 0x27) {
        index += 2;
        continue;
      }
      return index + 1;
    }
    index++;
  }
  return index;
}

int _skipDoubleQuotedIdentifier(String sql, int openQuoteIndex) {
  var index = openQuoteIndex + 1;
  while (index < sql.length) {
    if (sql.codeUnitAt(index) == 0x22) {
      if (index + 1 < sql.length && sql.codeUnitAt(index + 1) == 0x22) {
        index += 2;
        continue;
      }
      return index + 1;
    }
    index++;
  }
  return index;
}
