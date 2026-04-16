import 'dart:convert';
import 'dart:typed_data';

import 'package:serverpod_database/serverpod_database.dart';

/// Conservative Firebird SQL literal encoder for the first Serverpod dialect
/// slices. Complex values should move to prepared parameters instead of relying
/// on this literal path.
class FirebirdServerpodValueEncoder implements ValueEncoder {
  const FirebirdServerpodValueEncoder();

  @override
  String convert(
    Object? input, {
    bool escapeStrings = true,
    bool hasDefaults = false,
  }) {
    if (input == null) {
      return hasDefaults ? 'DEFAULT' : 'NULL';
    }
    if (input is bool) return input ? 'TRUE' : 'FALSE';
    if (input is int || input is BigInt) return input.toString();
    if (input is double) {
      if (input.isNaN) return 'NULL';
      if (input.isInfinite) {
        throw UnsupportedError(
          'Firebird does not support infinite floating-point literals.',
        );
      }
      return input.toString();
    }
    if (input is DateTime) return "'${input.toIso8601String()}'";
    if (input is Duration) return input.inMicroseconds.toString();
    if (input is Uri) return "'${_escapeString(input.toString())}'";
    if (input is String) {
      if (!escapeStrings) return input;
      return "'${_escapeString(input)}'";
    }
    if (input is ByteData) {
      final bytes = Uint8List.view(
        input.buffer,
        input.offsetInBytes,
        input.lengthInBytes,
      );
      return "'${base64Encode(bytes)}'";
    }

    return "'${_escapeString(jsonEncode(input))}'";
  }

  @override
  String? tryConvert(Object? input, {bool escapeStrings = false}) {
    try {
      return convert(input, escapeStrings: escapeStrings);
    } catch (_) {
      return null;
    }
  }

  static String _escapeString(String value) {
    return value.replaceAll("'", "''");
  }
}
