import 'package:serverpod_database/serverpod_database.dart';

/// Firebird default-value conversion helpers for Serverpod schema generation.
extension FirebirdColumnTypeDefault on ColumnType {
  /// Converts a dialect-neutral Serverpod default into Firebird SQL.
  String? getFirebirdColumnDefault(dynamic defaultValue) {
    if (defaultValue == null) return null;

    if ((this == ColumnType.integer || this == ColumnType.bigint) &&
        defaultValue == defaultIntSerial) {
      return null;
    }

    switch (this) {
      case ColumnType.timestampWithoutTimeZone:
        if (defaultValue is! String) {
          throw StateError('Invalid DateTime default value: $defaultValue');
        }
        if (defaultValue == defaultDateTimeValueNow) {
          return 'CURRENT_TIMESTAMP';
        }

        final dateTime = DateTime.parse(defaultValue).toUtc();
        final formatted = dateTime
            .toIso8601String()
            .replaceFirst('T', ' ')
            .replaceFirst('Z', '');
        return "TIMESTAMP '$formatted'";
      case ColumnType.boolean:
        if (defaultValue == defaultBooleanTrue) return 'TRUE';
        if (defaultValue == defaultBooleanFalse) return 'FALSE';
        return '$defaultValue';
      case ColumnType.integer:
      case ColumnType.doublePrecision:
      case ColumnType.bigint:
      case ColumnType.text:
      case ColumnType.json:
        return defaultValue.toString();
      case ColumnType.uuid:
        if (defaultValue is! String) {
          throw StateError('Invalid UUID default value: $defaultValue');
        }

        return switch (defaultValue) {
          defaultUuidValueRandom || defaultUuidValueRandomV7 => null,
          _ => defaultValue,
        };
      case ColumnType.bytea:
        throw UnsupportedError(
          'Firebird schema generation does not support binary column defaults '
          'yet.',
        );
      case ColumnType.vector:
      case ColumnType.halfvec:
      case ColumnType.sparsevec:
      case ColumnType.bit:
      case ColumnType.unknown:
        throw UnsupportedError(
          'Firebird schema generation does not support defaults for column '
          'type $this.',
        );
    }
  }
}
