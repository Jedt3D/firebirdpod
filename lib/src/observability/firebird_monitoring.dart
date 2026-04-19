import 'package:meta/meta.dart';

import '../runtime/firebird_connection.dart';
import '../sql/firebird_statement_parameters.dart';

/// Reads Firebird monitoring tables through an ordinary SQL attachment.
class FirebirdMonitoring {
  const FirebirdMonitoring(this._connection);

  final FirebirdConnection _connection;
  static const int _attachmentStatGroup = 1;
  static const int _statementStatGroup = 3;

  Future<int> currentAttachmentId() async {
    final result = await _connection.execute(
      'select current_connection as ATTACHMENT_ID from rdb\$database',
    );
    return _requireInt(
      result.singleRow?['ATTACHMENT_ID'],
      fieldName: 'ATTACHMENT_ID',
    );
  }

  Future<List<FirebirdMonitoredAttachment>> listAttachments({
    int? attachmentId,
    bool includeCurrentAttachment = true,
  }) async {
    final filter = _buildAttachmentFilter(
      attachmentId: attachmentId,
      includeCurrentAttachment: includeCurrentAttachment,
    );
    final result = await _connection.execute(
      '''
      select
        mon\$attachment_id as ATTACHMENT_ID,
        mon\$timestamp as ATTACHED_AT,
        mon\$state as STATE,
        mon\$system_flag as SYSTEM_FLAG,
        mon\$server_pid as SERVER_PID,
        mon\$user as USER_NAME,
        mon\$role as ROLE_NAME,
        mon\$remote_protocol as REMOTE_PROTOCOL,
        mon\$remote_address as REMOTE_ADDRESS,
        mon\$remote_process as REMOTE_PROCESS
      from mon\$attachments
      ${filter.sql}
      order by mon\$attachment_id
      ''',
      parameters: FirebirdStatementParameters.positional(filter.parameters),
    );

    return result.rows
        .map(
          (row) => FirebirdMonitoredAttachment(
            id: _requireInt(row['ATTACHMENT_ID'], fieldName: 'ATTACHMENT_ID'),
            attachedAt: _asDateTime(row['ATTACHED_AT']),
            state: _asInt(row['STATE']),
            isSystem: _asBool(row['SYSTEM_FLAG']),
            serverPid: _asInt(row['SERVER_PID']),
            user: _asTrimmedString(row['USER_NAME']),
            role: _asTrimmedString(row['ROLE_NAME']),
            remoteProtocol: _asTrimmedString(row['REMOTE_PROTOCOL']),
            remoteAddress: _asTrimmedString(row['REMOTE_ADDRESS']),
            remoteProcess: _asTrimmedString(row['REMOTE_PROCESS']),
          ),
        )
        .toList(growable: false);
  }

  Future<List<FirebirdMonitoredTransaction>> listTransactions({
    int? attachmentId,
    bool includeCurrentAttachment = true,
  }) async {
    final filter = _buildAttachmentFilter(
      attachmentId: attachmentId,
      includeCurrentAttachment: includeCurrentAttachment,
    );
    final result = await _connection.execute(
      '''
      select
        mon\$transaction_id as TRANSACTION_ID,
        mon\$attachment_id as ATTACHMENT_ID,
        mon\$timestamp as STARTED_AT,
        mon\$state as STATE,
        mon\$top_transaction as TOP_TRANSACTION,
        mon\$oldest_transaction as OLDEST_TRANSACTION,
        mon\$oldest_active as OLDEST_ACTIVE,
        mon\$isolation_mode as ISOLATION_MODE,
        mon\$lock_timeout as LOCK_TIMEOUT_SECONDS,
        mon\$read_only as READ_ONLY_FLAG,
        mon\$auto_commit as AUTO_COMMIT_FLAG,
        mon\$auto_undo as AUTO_UNDO_FLAG
      from mon\$transactions
      ${filter.sql}
      order by mon\$transaction_id
      ''',
      parameters: FirebirdStatementParameters.positional(filter.parameters),
    );

    return result.rows
        .map(
          (row) => FirebirdMonitoredTransaction(
            id: _requireInt(row['TRANSACTION_ID'], fieldName: 'TRANSACTION_ID'),
            attachmentId: _requireInt(
              row['ATTACHMENT_ID'],
              fieldName: 'ATTACHMENT_ID',
            ),
            startedAt: _asDateTime(row['STARTED_AT']),
            state: _asInt(row['STATE']),
            topTransactionId: _asInt(row['TOP_TRANSACTION']),
            oldestTransactionId: _asInt(row['OLDEST_TRANSACTION']),
            oldestActiveId: _asInt(row['OLDEST_ACTIVE']),
            isolationMode: _asInt(row['ISOLATION_MODE']),
            lockTimeoutSeconds: _asInt(row['LOCK_TIMEOUT_SECONDS']),
            isReadOnly: _asBool(row['READ_ONLY_FLAG']),
            isAutoCommit: _asBool(row['AUTO_COMMIT_FLAG']),
            isAutoUndo: _asBool(row['AUTO_UNDO_FLAG']),
          ),
        )
        .toList(growable: false);
  }

  Future<List<FirebirdMonitoredStatement>> listStatements({
    int? attachmentId,
    bool includeCurrentAttachment = true,
  }) async {
    final filter = _buildAttachmentFilter(
      attachmentId: attachmentId,
      includeCurrentAttachment: includeCurrentAttachment,
    );
    final result = await _connection.execute(
      '''
      select
        mon\$statement_id as STATEMENT_ID,
        mon\$attachment_id as ATTACHMENT_ID,
        mon\$transaction_id as TRANSACTION_ID,
        mon\$timestamp as STARTED_AT,
        mon\$state as STATE,
        mon\$sql_text as SQL_TEXT
      from mon\$statements
      ${filter.sql}
      order by mon\$statement_id
      ''',
      parameters: FirebirdStatementParameters.positional(filter.parameters),
    );

    return result.rows
        .map(
          (row) => FirebirdMonitoredStatement(
            id: _requireInt(row['STATEMENT_ID'], fieldName: 'STATEMENT_ID'),
            attachmentId: _requireInt(
              row['ATTACHMENT_ID'],
              fieldName: 'ATTACHMENT_ID',
            ),
            transactionId: _asInt(row['TRANSACTION_ID']),
            startedAt: _asDateTime(row['STARTED_AT']),
            state: _asInt(row['STATE']),
            sqlText: _asTrimmedString(row['SQL_TEXT']),
          ),
        )
        .toList(growable: false);
  }

  Future<FirebirdMonitoringSnapshot> captureSnapshot({
    bool includeCurrentAttachment = true,
  }) async {
    final attachmentsFuture = listAttachments(
      includeCurrentAttachment: includeCurrentAttachment,
    );
    final transactionsFuture = listTransactions(
      includeCurrentAttachment: includeCurrentAttachment,
    );
    final statementsFuture = listStatements(
      includeCurrentAttachment: includeCurrentAttachment,
    );

    return FirebirdMonitoringSnapshot(
      attachments: await attachmentsFuture,
      transactions: await transactionsFuture,
      statements: await statementsFuture,
    );
  }

  Future<FirebirdMonitoringSnapshot> captureExternalSnapshot() {
    return captureSnapshot(includeCurrentAttachment: false);
  }

  Future<FirebirdAttachmentMonitoringSnapshot>
  captureCurrentAttachmentSnapshot() async {
    return captureAttachmentSnapshot(await currentAttachmentId());
  }

  Future<FirebirdAttachmentMonitoringSnapshot> captureAttachmentSnapshot(
    int attachmentId,
  ) async {
    final attachmentsFuture = listAttachments(attachmentId: attachmentId);
    final transactionsFuture = listTransactions(attachmentId: attachmentId);
    final statementsFuture = listStatements(attachmentId: attachmentId);

    return FirebirdAttachmentMonitoringSnapshot(
      attachmentId: attachmentId,
      attachments: await attachmentsFuture,
      transactions: await transactionsFuture,
      statements: await statementsFuture,
    );
  }

  Future<FirebirdMonitoringStatisticsSnapshot>
  captureCurrentAttachmentStatistics() async {
    return captureAttachmentStatistics(await currentAttachmentId());
  }

  Future<FirebirdMonitoringStatisticsSnapshot> captureAttachmentStatistics(
    int attachmentId,
  ) async {
    return _captureStatisticsSnapshot(
      ownerType: FirebirdMonitoringStatOwnerType.attachment,
      ownerId: attachmentId,
      statGroup: _attachmentStatGroup,
      sourceTable: 'mon\$attachments',
      sourceIdColumn: 'mon\$attachment_id',
    );
  }

  Future<FirebirdMonitoringStatisticsSnapshot> captureStatementStatistics(
    int statementId,
  ) async {
    return _captureStatisticsSnapshot(
      ownerType: FirebirdMonitoringStatOwnerType.statement,
      ownerId: statementId,
      statGroup: _statementStatGroup,
      sourceTable: 'mon\$statements',
      sourceIdColumn: 'mon\$statement_id',
    );
  }

  Future<FirebirdMonitoringStatisticsSnapshot> _captureStatisticsSnapshot({
    required FirebirdMonitoringStatOwnerType ownerType,
    required int ownerId,
    required int statGroup,
    required String sourceTable,
    required String sourceIdColumn,
  }) async {
    final attachmentResult = await _connection.execute(
      '''
      select
        source.mon\$stat_id as STAT_ID,
        io.mon\$page_reads as PAGE_READS,
        io.mon\$page_writes as PAGE_WRITES,
        io.mon\$page_fetches as PAGE_FETCHES,
        io.mon\$page_marks as PAGE_MARKS,
        rs.mon\$record_seq_reads as RECORD_SEQ_READS,
        rs.mon\$record_idx_reads as RECORD_IDX_READS,
        rs.mon\$record_inserts as RECORD_INSERTS,
        rs.mon\$record_updates as RECORD_UPDATES,
        rs.mon\$record_deletes as RECORD_DELETES,
        rs.mon\$record_backouts as RECORD_BACKOUTS,
        rs.mon\$record_purges as RECORD_PURGES,
        rs.mon\$record_expunges as RECORD_EXPUNGES,
        rs.mon\$record_locks as RECORD_LOCKS,
        rs.mon\$record_waits as RECORD_WAITS,
        rs.mon\$record_conflicts as RECORD_CONFLICTS,
        rs.mon\$backversion_reads as BACKVERSION_READS,
        rs.mon\$fragment_reads as FRAGMENT_READS,
        rs.mon\$record_rpt_reads as RECORD_RPT_READS,
        rs.mon\$record_imgc as RECORD_IMGC,
        mu.mon\$memory_used as MEMORY_USED,
        mu.mon\$memory_allocated as MEMORY_ALLOCATED,
        mu.mon\$max_memory_used as MAX_MEMORY_USED,
        mu.mon\$max_memory_allocated as MAX_MEMORY_ALLOCATED
      from $sourceTable source
      left join mon\$io_stats io
        on io.mon\$stat_id = source.mon\$stat_id
       and io.mon\$stat_group = $statGroup
      left join mon\$record_stats rs
        on rs.mon\$stat_id = source.mon\$stat_id
       and rs.mon\$stat_group = $statGroup
      left join mon\$memory_usage mu
        on mu.mon\$stat_id = source.mon\$stat_id
       and mu.mon\$stat_group = $statGroup
      where source.$sourceIdColumn = \$1
      ''',
      parameters: FirebirdStatementParameters.positional([ownerId]),
    );
    final attachmentRow = attachmentResult.singleRow;
    if (attachmentRow == null) {
      throw StateError(
        'No monitoring statistics were found for $ownerType $ownerId.',
      );
    }

    final statId = _requireInt(attachmentRow['STAT_ID'], fieldName: 'STAT_ID');
    final tableResult = await _connection.execute(
      '''
      select
        ts.mon\$table_name as TABLE_NAME,
        rs.mon\$record_seq_reads as RECORD_SEQ_READS,
        rs.mon\$record_idx_reads as RECORD_IDX_READS,
        rs.mon\$record_inserts as RECORD_INSERTS,
        rs.mon\$record_updates as RECORD_UPDATES,
        rs.mon\$record_deletes as RECORD_DELETES,
        rs.mon\$record_backouts as RECORD_BACKOUTS,
        rs.mon\$record_purges as RECORD_PURGES,
        rs.mon\$record_expunges as RECORD_EXPUNGES,
        rs.mon\$record_locks as RECORD_LOCKS,
        rs.mon\$record_waits as RECORD_WAITS,
        rs.mon\$record_conflicts as RECORD_CONFLICTS,
        rs.mon\$backversion_reads as BACKVERSION_READS,
        rs.mon\$fragment_reads as FRAGMENT_READS,
        rs.mon\$record_rpt_reads as RECORD_RPT_READS,
        rs.mon\$record_imgc as RECORD_IMGC
      from mon\$table_stats ts
      join mon\$record_stats rs
        on rs.mon\$stat_id = ts.mon\$record_stat_id
       and rs.mon\$stat_group = $statGroup
      where ts.mon\$stat_id = \$1
        and ts.mon\$stat_group = $statGroup
      order by ts.mon\$table_name
      ''',
      parameters: FirebirdStatementParameters.positional([statId]),
    );

    return FirebirdMonitoringStatisticsSnapshot(
      ownerType: ownerType,
      ownerId: ownerId,
      statId: statId,
      io: FirebirdMonitoredIoStatistics(
        pageReads: _asInt(attachmentRow['PAGE_READS']) ?? 0,
        pageWrites: _asInt(attachmentRow['PAGE_WRITES']) ?? 0,
        pageFetches: _asInt(attachmentRow['PAGE_FETCHES']) ?? 0,
        pageMarks: _asInt(attachmentRow['PAGE_MARKS']) ?? 0,
      ),
      records: FirebirdMonitoredRecordStatistics(
        sequentialReads: _asInt(attachmentRow['RECORD_SEQ_READS']) ?? 0,
        indexedReads: _asInt(attachmentRow['RECORD_IDX_READS']) ?? 0,
        inserts: _asInt(attachmentRow['RECORD_INSERTS']) ?? 0,
        updates: _asInt(attachmentRow['RECORD_UPDATES']) ?? 0,
        deletes: _asInt(attachmentRow['RECORD_DELETES']) ?? 0,
        backouts: _asInt(attachmentRow['RECORD_BACKOUTS']) ?? 0,
        purges: _asInt(attachmentRow['RECORD_PURGES']) ?? 0,
        expunges: _asInt(attachmentRow['RECORD_EXPUNGES']) ?? 0,
        locks: _asInt(attachmentRow['RECORD_LOCKS']) ?? 0,
        waits: _asInt(attachmentRow['RECORD_WAITS']) ?? 0,
        conflicts: _asInt(attachmentRow['RECORD_CONFLICTS']) ?? 0,
        backversionReads: _asInt(attachmentRow['BACKVERSION_READS']) ?? 0,
        fragmentReads: _asInt(attachmentRow['FRAGMENT_READS']) ?? 0,
        repeatedReads: _asInt(attachmentRow['RECORD_RPT_READS']) ?? 0,
        recordImages: _asInt(attachmentRow['RECORD_IMGC']) ?? 0,
      ),
      memory: FirebirdMonitoredMemoryUsage(
        used: _asInt(attachmentRow['MEMORY_USED']) ?? 0,
        allocated: _asInt(attachmentRow['MEMORY_ALLOCATED']) ?? 0,
        maxUsed: _asInt(attachmentRow['MAX_MEMORY_USED']) ?? 0,
        maxAllocated: _asInt(attachmentRow['MAX_MEMORY_ALLOCATED']) ?? 0,
      ),
      tables: tableResult.rows
          .map(
            (row) => FirebirdMonitoredTableStatistics(
              tableName:
                  _asTrimmedString(row['TABLE_NAME']) ??
                  (throw StateError(
                    'Expected TABLE_NAME in MON\$TABLE_STATS result.',
                  )),
              records: FirebirdMonitoredRecordStatistics(
                sequentialReads: _asInt(row['RECORD_SEQ_READS']) ?? 0,
                indexedReads: _asInt(row['RECORD_IDX_READS']) ?? 0,
                inserts: _asInt(row['RECORD_INSERTS']) ?? 0,
                updates: _asInt(row['RECORD_UPDATES']) ?? 0,
                deletes: _asInt(row['RECORD_DELETES']) ?? 0,
                backouts: _asInt(row['RECORD_BACKOUTS']) ?? 0,
                purges: _asInt(row['RECORD_PURGES']) ?? 0,
                expunges: _asInt(row['RECORD_EXPUNGES']) ?? 0,
                locks: _asInt(row['RECORD_LOCKS']) ?? 0,
                waits: _asInt(row['RECORD_WAITS']) ?? 0,
                conflicts: _asInt(row['RECORD_CONFLICTS']) ?? 0,
                backversionReads: _asInt(row['BACKVERSION_READS']) ?? 0,
                fragmentReads: _asInt(row['FRAGMENT_READS']) ?? 0,
                repeatedReads: _asInt(row['RECORD_RPT_READS']) ?? 0,
                recordImages: _asInt(row['RECORD_IMGC']) ?? 0,
              ),
            ),
          )
          .toList(growable: false),
    );
  }
}

extension FirebirdConnectionMonitoring on FirebirdConnection {
  FirebirdMonitoring get monitoring => FirebirdMonitoring(this);
}

@immutable
class FirebirdMonitoredAttachment {
  const FirebirdMonitoredAttachment({
    required this.id,
    this.attachedAt,
    this.state,
    this.isSystem,
    this.serverPid,
    this.user,
    this.role,
    this.remoteProtocol,
    this.remoteAddress,
    this.remoteProcess,
  });

  final int id;
  final DateTime? attachedAt;
  final int? state;
  final bool? isSystem;
  final int? serverPid;
  final String? user;
  final String? role;
  final String? remoteProtocol;
  final String? remoteAddress;
  final String? remoteProcess;
}

@immutable
class FirebirdMonitoredTransaction {
  const FirebirdMonitoredTransaction({
    required this.id,
    required this.attachmentId,
    this.startedAt,
    this.state,
    this.topTransactionId,
    this.oldestTransactionId,
    this.oldestActiveId,
    this.isolationMode,
    this.lockTimeoutSeconds,
    this.isReadOnly,
    this.isAutoCommit,
    this.isAutoUndo,
  });

  final int id;
  final int attachmentId;
  final DateTime? startedAt;
  final int? state;
  final int? topTransactionId;
  final int? oldestTransactionId;
  final int? oldestActiveId;
  final int? isolationMode;
  final int? lockTimeoutSeconds;
  final bool? isReadOnly;
  final bool? isAutoCommit;
  final bool? isAutoUndo;
}

@immutable
class FirebirdMonitoredStatement {
  const FirebirdMonitoredStatement({
    required this.id,
    required this.attachmentId,
    this.transactionId,
    this.startedAt,
    this.state,
    this.sqlText,
  });

  final int id;
  final int attachmentId;
  final int? transactionId;
  final DateTime? startedAt;
  final int? state;
  final String? sqlText;
}

@immutable
class FirebirdMonitoringSnapshot {
  FirebirdMonitoringSnapshot({
    required List<FirebirdMonitoredAttachment> attachments,
    required List<FirebirdMonitoredTransaction> transactions,
    required List<FirebirdMonitoredStatement> statements,
  }) : attachments = List.unmodifiable(attachments),
       transactions = List.unmodifiable(transactions),
       statements = List.unmodifiable(statements);

  final List<FirebirdMonitoredAttachment> attachments;
  final List<FirebirdMonitoredTransaction> transactions;
  final List<FirebirdMonitoredStatement> statements;

  int get attachmentCount => attachments.length;
  int get transactionCount => transactions.length;
  int get statementCount => statements.length;

  List<int> get attachmentIds =>
      attachments.map((attachment) => attachment.id).toList(growable: false);

  List<int> get transactionIds =>
      transactions.map((transaction) => transaction.id).toList(growable: false);

  List<int> get statementIds =>
      statements.map((statement) => statement.id).toList(growable: false);
}

@immutable
class FirebirdAttachmentMonitoringSnapshot extends FirebirdMonitoringSnapshot {
  FirebirdAttachmentMonitoringSnapshot({
    required this.attachmentId,
    required super.attachments,
    required super.transactions,
    required super.statements,
  });

  final int attachmentId;
}

enum FirebirdMonitoringStatOwnerType { attachment, statement }

@immutable
class FirebirdMonitoredIoStatistics {
  const FirebirdMonitoredIoStatistics({
    this.pageReads = 0,
    this.pageWrites = 0,
    this.pageFetches = 0,
    this.pageMarks = 0,
  });

  final int pageReads;
  final int pageWrites;
  final int pageFetches;
  final int pageMarks;

  int get totalActivity => pageReads + pageWrites + pageFetches + pageMarks;
  bool get isZero => totalActivity == 0;

  FirebirdMonitoredIoStatistics difference(
    FirebirdMonitoredIoStatistics baseline,
  ) {
    return FirebirdMonitoredIoStatistics(
      pageReads: pageReads - baseline.pageReads,
      pageWrites: pageWrites - baseline.pageWrites,
      pageFetches: pageFetches - baseline.pageFetches,
      pageMarks: pageMarks - baseline.pageMarks,
    );
  }
}

@immutable
class FirebirdMonitoredRecordStatistics {
  const FirebirdMonitoredRecordStatistics({
    this.sequentialReads = 0,
    this.indexedReads = 0,
    this.inserts = 0,
    this.updates = 0,
    this.deletes = 0,
    this.backouts = 0,
    this.purges = 0,
    this.expunges = 0,
    this.locks = 0,
    this.waits = 0,
    this.conflicts = 0,
    this.backversionReads = 0,
    this.fragmentReads = 0,
    this.repeatedReads = 0,
    this.recordImages = 0,
  });

  final int sequentialReads;
  final int indexedReads;
  final int inserts;
  final int updates;
  final int deletes;
  final int backouts;
  final int purges;
  final int expunges;
  final int locks;
  final int waits;
  final int conflicts;
  final int backversionReads;
  final int fragmentReads;
  final int repeatedReads;
  final int recordImages;

  int get totalReadOperations =>
      sequentialReads +
      indexedReads +
      backversionReads +
      fragmentReads +
      repeatedReads;

  int get totalWriteOperations =>
      inserts + updates + deletes + backouts + purges + expunges;

  int get totalLockOperations => locks + waits + conflicts;

  bool get isZero =>
      totalReadOperations == 0 &&
      totalWriteOperations == 0 &&
      totalLockOperations == 0 &&
      recordImages == 0;

  FirebirdMonitoredRecordStatistics difference(
    FirebirdMonitoredRecordStatistics baseline,
  ) {
    return FirebirdMonitoredRecordStatistics(
      sequentialReads: sequentialReads - baseline.sequentialReads,
      indexedReads: indexedReads - baseline.indexedReads,
      inserts: inserts - baseline.inserts,
      updates: updates - baseline.updates,
      deletes: deletes - baseline.deletes,
      backouts: backouts - baseline.backouts,
      purges: purges - baseline.purges,
      expunges: expunges - baseline.expunges,
      locks: locks - baseline.locks,
      waits: waits - baseline.waits,
      conflicts: conflicts - baseline.conflicts,
      backversionReads: backversionReads - baseline.backversionReads,
      fragmentReads: fragmentReads - baseline.fragmentReads,
      repeatedReads: repeatedReads - baseline.repeatedReads,
      recordImages: recordImages - baseline.recordImages,
    );
  }
}

@immutable
class FirebirdMonitoredMemoryUsage {
  const FirebirdMonitoredMemoryUsage({
    this.used = 0,
    this.allocated = 0,
    this.maxUsed = 0,
    this.maxAllocated = 0,
  });

  final int used;
  final int allocated;
  final int maxUsed;
  final int maxAllocated;

  bool get isZero =>
      used == 0 && allocated == 0 && maxUsed == 0 && maxAllocated == 0;

  FirebirdMonitoredMemoryUsage difference(FirebirdMonitoredMemoryUsage baseline) {
    return FirebirdMonitoredMemoryUsage(
      used: used - baseline.used,
      allocated: allocated - baseline.allocated,
      maxUsed: maxUsed - baseline.maxUsed,
      maxAllocated: maxAllocated - baseline.maxAllocated,
    );
  }
}

@immutable
class FirebirdMonitoredTableStatistics {
  const FirebirdMonitoredTableStatistics({
    required this.tableName,
    required this.records,
  });

  final String tableName;
  final FirebirdMonitoredRecordStatistics records;

  int get totalReadOperations => records.totalReadOperations;
  int get totalWriteOperations => records.totalWriteOperations;
  bool get isZero => records.isZero;

  FirebirdMonitoredTableStatistics difference(
    FirebirdMonitoredTableStatistics baseline,
  ) {
    return FirebirdMonitoredTableStatistics(
      tableName: tableName,
      records: records.difference(baseline.records),
    );
  }
}

@immutable
class FirebirdMonitoringStatisticsSnapshot {
  FirebirdMonitoringStatisticsSnapshot({
    required this.ownerType,
    required this.ownerId,
    required this.statId,
    required this.io,
    required this.records,
    required this.memory,
    required List<FirebirdMonitoredTableStatistics> tables,
  }) : tables = List.unmodifiable(tables);

  final FirebirdMonitoringStatOwnerType ownerType;
  final int ownerId;
  final int statId;
  final FirebirdMonitoredIoStatistics io;
  final FirebirdMonitoredRecordStatistics records;
  final FirebirdMonitoredMemoryUsage memory;
  final List<FirebirdMonitoredTableStatistics> tables;

  FirebirdMonitoringStatisticsDelta deltaFrom(
    FirebirdMonitoringStatisticsSnapshot baseline,
  ) {
    final baselineTables = <String, FirebirdMonitoredTableStatistics>{
      for (final table in baseline.tables) table.tableName: table,
    };
    final currentTables = <String, FirebirdMonitoredTableStatistics>{
      for (final table in tables) table.tableName: table,
    };
    final tableNames = <String>{
      ...baselineTables.keys,
      ...currentTables.keys,
    }.toList()..sort();

    final tableDeltas = tableNames
        .map((tableName) {
          final current = currentTables[tableName];
          final previous = baselineTables[tableName];
          if (current == null) {
            return FirebirdMonitoredTableStatistics(
              tableName: tableName,
              records: const FirebirdMonitoredRecordStatistics(),
            ).difference(previous!);
          }
          if (previous == null) {
            return current.difference(
              FirebirdMonitoredTableStatistics(
                tableName: tableName,
                records: const FirebirdMonitoredRecordStatistics(),
              ),
            );
          }
          return current.difference(previous);
        })
        .where((table) => !table.isZero)
        .toList(growable: false);

    return FirebirdMonitoringStatisticsDelta(
      ownerType: ownerType,
      ownerId: ownerId,
      io: io.difference(baseline.io),
      records: records.difference(baseline.records),
      memory: memory.difference(baseline.memory),
      tables: tableDeltas,
    );
  }
}

@immutable
class FirebirdMonitoringStatisticsDelta {
  FirebirdMonitoringStatisticsDelta({
    required this.ownerType,
    required this.ownerId,
    required this.io,
    required this.records,
    required this.memory,
    required List<FirebirdMonitoredTableStatistics> tables,
  }) : tables = List.unmodifiable(tables);

  final FirebirdMonitoringStatOwnerType ownerType;
  final int ownerId;
  final FirebirdMonitoredIoStatistics io;
  final FirebirdMonitoredRecordStatistics records;
  final FirebirdMonitoredMemoryUsage memory;
  final List<FirebirdMonitoredTableStatistics> tables;

  bool get isZero =>
      io.isZero && records.isZero && memory.isZero && tables.isEmpty;

  List<FirebirdMonitoredTableStatistics> tablesByReadOperations({
    int? limit,
  }) {
    final sorted = [...tables]
      ..sort(
        (left, right) =>
            right.totalReadOperations.compareTo(left.totalReadOperations),
      );
    if (limit == null || limit >= sorted.length) {
      return List<FirebirdMonitoredTableStatistics>.unmodifiable(sorted);
    }
    return List<FirebirdMonitoredTableStatistics>.unmodifiable(
      sorted.take(limit),
    );
  }
}

class _MonitoringFilter {
  const _MonitoringFilter({required this.sql, required this.parameters});

  final String sql;
  final List<Object?> parameters;
}

_MonitoringFilter _buildAttachmentFilter({
  int? attachmentId,
  required bool includeCurrentAttachment,
}) {
  if (attachmentId != null) {
    return _MonitoringFilter(
      sql: 'where mon\$attachment_id = \$1',
      parameters: [attachmentId],
    );
  }
  if (includeCurrentAttachment) {
    return const _MonitoringFilter(sql: '', parameters: <Object?>[]);
  }
  return const _MonitoringFilter(
    sql: 'where mon\$attachment_id <> current_connection',
    parameters: <Object?>[],
  );
}

String? _asTrimmedString(Object? value) {
  if (value == null) return null;
  final text = value.toString().trim();
  return text.isEmpty ? null : text;
}

DateTime? _asDateTime(Object? value) {
  return value is DateTime ? value : null;
}

int _requireInt(Object? value, {required String fieldName}) {
  final intValue = _asInt(value);
  if (intValue == null) {
    throw StateError('Expected $fieldName to be an integer, got $value.');
  }
  return intValue;
}

int? _asInt(Object? value) {
  switch (value) {
    case null:
      return null;
    case int intValue:
      return intValue;
    case BigInt bigIntValue:
      return bigIntValue.toInt();
    case num numericValue:
      return numericValue.toInt();
    case String textValue:
      return int.tryParse(textValue.trim());
    default:
      return null;
  }
}

bool? _asBool(Object? value) {
  switch (value) {
    case null:
      return null;
    case bool boolValue:
      return boolValue;
    case num numericValue:
      return numericValue != 0;
    case String textValue:
      final normalized = textValue.trim().toLowerCase();
      if (normalized.isEmpty) return null;
      if (normalized == '1' || normalized == 'true') return true;
      if (normalized == '0' || normalized == 'false') return false;
      return null;
    default:
      return null;
  }
}
