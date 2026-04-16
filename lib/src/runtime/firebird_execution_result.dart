import 'package:meta/meta.dart';

@immutable
class FirebirdExecutionResult {
  const FirebirdExecutionResult({this.affectedRows, this.rows = const []});

  final int? affectedRows;
  final List<Map<String, Object?>> rows;

  Map<String, Object?>? get firstRow => rows.isEmpty ? null : rows.first;

  Map<String, Object?>? get singleRow {
    if (rows.isEmpty) return null;
    if (rows.length != 1) {
      throw StateError(
        'Expected exactly one row in the result, got ${rows.length}.',
      );
    }
    return rows.single;
  }

  Object? generatedId({String? columnName}) {
    final row = singleRow;
    if (row == null) return null;
    if (columnName case final name?) {
      return row[name];
    }
    if (row.length == 1) {
      return row.values.single;
    }
    return row[row.keys.first];
  }
}
