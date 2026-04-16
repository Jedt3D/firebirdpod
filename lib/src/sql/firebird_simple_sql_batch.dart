/// Splits Firebird simple-SQL batches into executable top-level statements.
///
/// The current implementation is intentionally focused on the statements we
/// need for schema and migration work:
///
/// - ordinary SQL statements separated by `;`
/// - `EXECUTE BLOCK` statements with nested `BEGIN ... END` and inner `;`
/// - quoted strings and identifiers
/// - line and block comments
///
/// It is not a full Firebird parser, but it is deterministic and sufficient
/// for the migration batches used by `firebirdpod`.
List<String> splitFirebirdSimpleSqlBatch(String sql) {
  var current = StringBuffer();
  var token = StringBuffer();
  final statements = <String>[];

  var inSingleQuote = false;
  var inDoubleQuote = false;
  var inLineComment = false;
  var inBlockComment = false;

  var pendingExecute = false;
  var inExecuteBlock = false;
  var executeBlockDepth = 0;
  String? lastKeyword;

  void emitCurrentStatement() {
    final statement = current.toString().trim();
    if (statement.isNotEmpty) {
      statements.add(statement);
    }
    current = StringBuffer();
    lastKeyword = null;
  }

  void finalizeToken() {
    if (token.isEmpty) return;

    final upper = token.toString().toUpperCase();
    token = StringBuffer();

    if (pendingExecute && !inExecuteBlock) {
      inExecuteBlock = upper == 'BLOCK';
      pendingExecute = false;
    } else if (upper == 'EXECUTE') {
      pendingExecute = true;
    } else {
      pendingExecute = false;
    }

    if (inExecuteBlock) {
      if (upper == 'BEGIN') {
        executeBlockDepth++;
      } else if (upper == 'END' && executeBlockDepth > 0) {
        executeBlockDepth--;
      }
    }

    lastKeyword = upper;
  }

  bool isWordChar(String char) {
    final codeUnit = char.codeUnitAt(0);
    return (codeUnit >= 48 && codeUnit <= 57) ||
        (codeUnit >= 65 && codeUnit <= 90) ||
        (codeUnit >= 97 && codeUnit <= 122) ||
        char == '_' ||
        char == '\$';
  }

  for (var index = 0; index < sql.length; index++) {
    final char = sql[index];
    final next = index + 1 < sql.length ? sql[index + 1] : null;

    if (inLineComment) {
      current.write(char);
      if (char == '\n') {
        inLineComment = false;
      }
      continue;
    }

    if (inBlockComment) {
      current.write(char);
      if (char == '*' && next == '/') {
        current.write(next);
        inBlockComment = false;
        index++;
      }
      continue;
    }

    if (inSingleQuote) {
      current.write(char);
      if (char == "'" && next == "'") {
        current.write(next);
        index++;
        continue;
      }
      if (char == "'") {
        inSingleQuote = false;
      }
      continue;
    }

    if (inDoubleQuote) {
      current.write(char);
      if (char == '"' && next == '"') {
        current.write(next);
        index++;
        continue;
      }
      if (char == '"') {
        inDoubleQuote = false;
      }
      continue;
    }

    if (char == '-' && next == '-') {
      finalizeToken();
      current
        ..write(char)
        ..write(next);
      inLineComment = true;
      index++;
      continue;
    }

    if (char == '/' && next == '*') {
      finalizeToken();
      current
        ..write(char)
        ..write(next);
      inBlockComment = true;
      index++;
      continue;
    }

    if (char == "'") {
      finalizeToken();
      current.write(char);
      inSingleQuote = true;
      continue;
    }

    if (char == '"') {
      finalizeToken();
      current.write(char);
      inDoubleQuote = true;
      continue;
    }

    if (isWordChar(char)) {
      current.write(char);
      token.write(char);
      continue;
    }

    finalizeToken();
    current.write(char);

    if (char == ';') {
      if (inExecuteBlock) {
        if (executeBlockDepth == 0 && lastKeyword == 'END') {
          emitCurrentStatement();
          inExecuteBlock = false;
          pendingExecute = false;
        }
      } else {
        emitCurrentStatement();
        pendingExecute = false;
      }
    }
  }

  finalizeToken();
  emitCurrentStatement();

  return statements;
}
