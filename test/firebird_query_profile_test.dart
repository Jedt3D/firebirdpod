import 'package:firebirdpod/firebirdpod.dart';
import 'package:test/test.dart';

void main() {
  test('computes attachment statistics deltas and sorts hot tables', () {
    final before = FirebirdMonitoringStatisticsSnapshot(
      ownerType: FirebirdMonitoringStatOwnerType.attachment,
      ownerId: 42,
      statId: 100,
      io: const FirebirdMonitoredIoStatistics(
        pageReads: 2,
        pageFetches: 10,
      ),
      records: const FirebirdMonitoredRecordStatistics(
        sequentialReads: 4,
        indexedReads: 6,
      ),
      memory: const FirebirdMonitoredMemoryUsage(
        used: 100,
        allocated: 200,
      ),
      tables: const <FirebirdMonitoredTableStatistics>[
        FirebirdMonitoredTableStatistics(
          tableName: 'ALBUMS',
          records: FirebirdMonitoredRecordStatistics(indexedReads: 3),
        ),
        FirebirdMonitoredTableStatistics(
          tableName: 'TRACKS',
          records: FirebirdMonitoredRecordStatistics(sequentialReads: 5),
        ),
      ],
    );

    final after = FirebirdMonitoringStatisticsSnapshot(
      ownerType: FirebirdMonitoringStatOwnerType.attachment,
      ownerId: 42,
      statId: 100,
      io: const FirebirdMonitoredIoStatistics(
        pageReads: 4,
        pageFetches: 18,
        pageMarks: 1,
      ),
      records: const FirebirdMonitoredRecordStatistics(
        sequentialReads: 9,
        indexedReads: 10,
        updates: 1,
      ),
      memory: const FirebirdMonitoredMemoryUsage(
        used: 120,
        allocated: 240,
        maxUsed: 150,
      ),
      tables: const <FirebirdMonitoredTableStatistics>[
        FirebirdMonitoredTableStatistics(
          tableName: 'ALBUMS',
          records: FirebirdMonitoredRecordStatistics(indexedReads: 7),
        ),
        FirebirdMonitoredTableStatistics(
          tableName: 'TRACKS',
          records: FirebirdMonitoredRecordStatistics(
            sequentialReads: 11,
            updates: 1,
          ),
        ),
      ],
    );

    final delta = after.deltaFrom(before);

    expect(delta.ownerId, 42);
    expect(delta.io.pageReads, 2);
    expect(delta.io.pageFetches, 8);
    expect(delta.io.pageMarks, 1);
    expect(delta.records.sequentialReads, 5);
    expect(delta.records.indexedReads, 4);
    expect(delta.records.updates, 1);
    expect(delta.memory.used, 20);
    expect(delta.memory.allocated, 40);
    expect(delta.memory.maxUsed, 150);
    expect(delta.tables, hasLength(2));
    expect(
      delta.tablesByReadOperations().map((table) => table.tableName),
      <String>['TRACKS', 'ALBUMS'],
    );
    expect(
      delta.tablesByReadOperations().first.records.totalReadOperations,
      6,
    );
  });
}
