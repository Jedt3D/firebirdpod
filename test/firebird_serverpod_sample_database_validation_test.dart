import 'package:firebirdpod/firebirdpod.dart';
import 'package:test/test.dart';

import 'firebird_test_support.dart';

void main() {
  group('Phase 03 sample database validation', () {
    test('converted sample databases stay within the current validation budget', () async {
      if (!shouldRunDirectIntegrationTests()) {
        markTestSkipped(
          'set FIREBIRDPOD_RUN_FBCLIENT_DIRECT=1 to run live Firebird sample validation tests',
        );
        return;
      }

      for (final target in firebirdSampleDatabaseTargets.where(
        (target) => target.kind == FirebirdSampleDatabaseKind.converted,
      )) {
        final result = await validateSampleDatabase(target);
        final expected = _expectedInventory[target.name]!;

        expect(result.tableCount, expected.tableCount, reason: target.label);
        expect(result.viewCount, expected.viewCount, reason: target.label);
        expect(
          result.triggerCount,
          expected.triggerCount,
          reason: target.label,
        );
        expect(
          result.procedureCount,
          expected.procedureCount,
          reason: target.label,
        );
        expect(
          result.sequenceCount,
          expected.sequenceCount,
          reason: target.label,
        );
        expect(result.columnCount, expected.columnCount, reason: target.label);
        expect(
          result.passesZeroGapBaseline,
          isTrue,
          reason: '${target.label}: '
              'unknown=${result.unknownColumns.map((issue) => issue.displayLocation).toList()} '
              'defaults=${result.unresolvedDefaults.map((issue) => issue.displayLocation).toList()} '
              'generator=${result.generatorError}',
        );
      }
    });

    test('curated native sample databases stay at zero-gap baseline', () async {
      if (!shouldRunDirectIntegrationTests()) {
        markTestSkipped(
          'set FIREBIRDPOD_RUN_FBCLIENT_DIRECT=1 to run live Firebird sample validation tests',
        );
        return;
      }

      for (final target in firebirdSampleDatabaseTargets.where(
        (target) => target.kind == FirebirdSampleDatabaseKind.curatedNative,
      )) {
        final result = await validateSampleDatabase(target);
        final expected = _expectedInventory[target.name]!;

        expect(result.tableCount, expected.tableCount, reason: target.label);
        expect(result.viewCount, expected.viewCount, reason: target.label);
        expect(
          result.triggerCount,
          expected.triggerCount,
          reason: target.label,
        );
        expect(
          result.procedureCount,
          expected.procedureCount,
          reason: target.label,
        );
        expect(
          result.sequenceCount,
          expected.sequenceCount,
          reason: target.label,
        );
        expect(result.columnCount, expected.columnCount, reason: target.label);
        expect(
          result.passesZeroGapBaseline,
          isTrue,
          reason: '${target.label}: '
              'unknown=${result.unknownColumns.map((issue) => issue.displayLocation).toList()} '
              'defaults=${result.unresolvedDefaults.map((issue) => issue.displayLocation).toList()} '
              'generator=${result.generatorError}',
        );
      }
    });
  });
}

const _expectedInventory = <String, _ExpectedInventory>{
  'car_database': _ExpectedInventory(
    tableCount: 10,
    viewCount: 0,
    triggerCount: 0,
    procedureCount: 0,
    sequenceCount: 0,
    columnCount: 49,
  ),
  'chinook': _ExpectedInventory(
    tableCount: 11,
    viewCount: 0,
    triggerCount: 0,
    procedureCount: 0,
    sequenceCount: 0,
    columnCount: 64,
  ),
  'northwind': _ExpectedInventory(
    tableCount: 13,
    viewCount: 0,
    triggerCount: 0,
    procedureCount: 0,
    sequenceCount: 0,
    columnCount: 88,
  ),
  'sakila_master': _ExpectedInventory(
    tableCount: 16,
    viewCount: 0,
    triggerCount: 0,
    procedureCount: 0,
    sequenceCount: 0,
    columnCount: 89,
  ),
  'car_database_native': _ExpectedInventory(
    tableCount: 10,
    viewCount: 0,
    triggerCount: 8,
    procedureCount: 1,
    sequenceCount: 8,
    columnCount: 50,
  ),
  'chinook_native': _ExpectedInventory(
    tableCount: 11,
    viewCount: 1,
    triggerCount: 10,
    procedureCount: 1,
    sequenceCount: 10,
    columnCount: 64,
  ),
  'northwind_native': _ExpectedInventory(
    tableCount: 13,
    viewCount: 1,
    triggerCount: 10,
    procedureCount: 1,
    sequenceCount: 10,
    columnCount: 91,
  ),
  'sakila_master_native': _ExpectedInventory(
    tableCount: 16,
    viewCount: 1,
    triggerCount: 14,
    procedureCount: 1,
    sequenceCount: 14,
    columnCount: 89,
  ),
};

class _ExpectedInventory {
  const _ExpectedInventory({
    required this.tableCount,
    required this.viewCount,
    required this.triggerCount,
    required this.procedureCount,
    required this.sequenceCount,
    required this.columnCount,
  });

  final int tableCount;
  final int viewCount;
  final int triggerCount;
  final int procedureCount;
  final int sequenceCount;
  final int columnCount;
}
