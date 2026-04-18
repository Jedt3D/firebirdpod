import 'package:firebirdpod/firebirdpod.dart';
import 'package:serverpod_serialization/serverpod_serialization.dart';
import 'package:test/test.dart';

void main() {
  const encoder = FirebirdServerpodValueEncoder();

  group('Firebird Serverpod value encoder', () {
    test('renders UUID values as Firebird string literals', () {
      expect(
        encoder.convert(
          UuidValue.withValidation('550e8400-e29b-41d4-a716-446655440000'),
        ),
        "'550e8400-e29b-41d4-a716-446655440000'",
      );
    });

    test('serializes sets through the Serverpod serializer', () {
      expect(
        encoder.convert({'profile', 'admin'}),
        anyOf("'[\"profile\",\"admin\"]'", "'[\"admin\",\"profile\"]'"),
      );
    });
  });
}
