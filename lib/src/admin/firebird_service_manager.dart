import 'dart:ffi';

import 'package:fbdb/fbclient.dart' as fbclient;
import 'package:ffi/ffi.dart';
import 'package:meta/meta.dart';

import '../runtime/firebird_connection_options.dart';
import '../runtime/firebird_error_mapper.dart';

/// Low-level Firebird service-manager entry point for maintenance operations
/// that are not available through ordinary SQL attachments.
class FirebirdServiceManager {
  const FirebirdServiceManager({this.fbClientLibraryPath});

  final String? fbClientLibraryPath;

  Future<FirebirdServiceManagerConnection> attach(
    FirebirdConnectionOptions options,
  ) async {
    final client = fbclient.FbClient(fbClientLibraryPath);
    final master = client.fbGetMasterInterface();
    final status = master.getStatus();
    final provider = master.getDispatcher();
    final util = master.getUtilInterface();
    fbclient.IXpbBuilder? spb;

    try {
      _safeStatusInit(status);
      spb = util.getXpbBuilder(status, fbclient.IXpbBuilder.spbAttach);

      final host = _trimOrNull(options.host);
      if (host != null) {
        spb.insertString(status, fbclient.FbConsts.isc_spb_host_name, host);
      }

      spb.insertString(
        status,
        fbclient.FbConsts.isc_spb_user_name,
        options.user,
      );
      spb.insertString(
        status,
        fbclient.FbConsts.isc_spb_password,
        options.password,
      );
      if (options.role case final role? when role.isNotEmpty) {
        spb.insertString(status, fbclient.FbConsts.isc_spb_sql_role_name, role);
      }
      spb.insertString(
        status,
        fbclient.FbConsts.isc_spb_expected_db,
        options.database,
      );

      final service = provider.attachServiceManager(
        status,
        _serviceManagerAttachmentString(options),
        spb.getBufferLength(status),
        spb.getBuffer(status),
      );

      return FirebirdServiceManagerConnection._(
        client: client,
        status: status,
        provider: provider,
        util: util,
        service: service,
        options: options,
      );
    } catch (error) {
      final mappedError = error is fbclient.FbStatusException
          ? mapFbStatusException(
              exception: error,
              util: util,
              operation: 'attach service manager',
            )
          : error;
      _safeStatusInit(status);
      try {
        provider.release();
      } catch (_) {}
      try {
        status.dispose();
      } catch (_) {}
      client.close();
      throw mappedError;
    } finally {
      spb?.dispose();
    }
  }
}

class FirebirdServiceManagerConnection {
  FirebirdServiceManagerConnection._({
    required this.client,
    required this.status,
    required this.provider,
    required this.util,
    required fbclient.IService service,
    required FirebirdConnectionOptions options,
  }) : _service = service,
       _options = options;

  static const _serviceQueryBufferSize = 8192;

  final fbclient.FbClient client;
  final fbclient.IStatus status;
  final fbclient.IProvider provider;
  final fbclient.IUtil util;
  final FirebirdConnectionOptions _options;

  fbclient.IService? _service;
  bool _isClosed = false;

  Future<void> close() async {
    if (_isClosed) return;
    _isClosed = true;

    Object? firstError;
    StackTrace? firstStackTrace;

    void recordError(Object error, StackTrace stackTrace) {
      firstError ??= error is fbclient.FbStatusException
          ? mapFbStatusException(
              exception: error,
              util: util,
              operation: 'close service manager connection',
            )
          : error;
      firstStackTrace ??= stackTrace;
    }

    final service = _service;
    _service = null;
    if (service != null) {
      _safeStatusInit(status);
      try {
        service.detach(status);
      } catch (error, stackTrace) {
        recordError(error, stackTrace);
        try {
          service.release();
        } catch (releaseError, releaseStackTrace) {
          recordError(releaseError, releaseStackTrace);
        }
      }
    }

    try {
      provider.release();
    } catch (error, stackTrace) {
      recordError(error, stackTrace);
    }

    try {
      status.dispose();
    } catch (error, stackTrace) {
      recordError(error, stackTrace);
    }

    try {
      client.close();
    } catch (error, stackTrace) {
      recordError(error, stackTrace);
    }

    if (firstError != null) {
      Error.throwWithStackTrace(firstError!, firstStackTrace!);
    }
  }

  Future<String> queryServerVersion() async {
    final values = _queryServiceTextItems(
      requestedItem: fbclient.FbConsts.isc_info_svc_server_version,
      operation: 'query server version',
    );

    if (values.isEmpty) {
      throw StateError('Firebird service manager returned no server version.');
    }

    return values.first.trim();
  }

  Future<FirebirdServiceManagerReport> getDatabaseStatistics({
    String? database,
    FirebirdDatabaseStatisticsOptions options =
        const FirebirdDatabaseStatisticsOptions(),
  }) async {
    final targetDatabase = database ?? _options.database;
    final lines = _runLineService(
      operation: 'database statistics',
      configure: (spb) {
        spb.insertTag(status, fbclient.FbConsts.isc_action_svc_db_stats);
        spb.insertString(
          status,
          fbclient.FbConsts.isc_spb_dbname,
          targetDatabase,
        );
        spb.insertInt(
          status,
          fbclient.FbConsts.isc_spb_options,
          options.serviceOptions,
        );
      },
    );

    return FirebirdServiceManagerReport(
      operation: 'database statistics',
      database: targetDatabase,
      lines: lines,
    );
  }

  Future<FirebirdServiceManagerReport> validateDatabase({
    String? database,
  }) async {
    final targetDatabase = database ?? _options.database;
    final lines = _runLineService(
      operation: 'validate database',
      configure: (spb) {
        spb.insertTag(status, fbclient.FbConsts.isc_action_svc_repair);
        spb.insertString(
          status,
          fbclient.FbConsts.isc_spb_dbname,
          targetDatabase,
        );
        spb.insertInt(
          status,
          fbclient.FbConsts.isc_spb_options,
          fbclient.FbConsts.isc_spb_rpr_validate_db,
        );
      },
    );

    return FirebirdServiceManagerReport(
      operation: 'validate database',
      database: targetDatabase,
      lines: lines,
    );
  }

  Future<FirebirdServiceManagerReport> sweepDatabase({String? database}) async {
    final targetDatabase = database ?? _options.database;
    final lines = _runLineService(
      operation: 'sweep database',
      configure: (spb) {
        spb.insertTag(status, fbclient.FbConsts.isc_action_svc_repair);
        spb.insertString(
          status,
          fbclient.FbConsts.isc_spb_dbname,
          targetDatabase,
        );
        spb.insertInt(
          status,
          fbclient.FbConsts.isc_spb_options,
          fbclient.FbConsts.isc_spb_rpr_sweep_db,
        );
      },
    );

    return FirebirdServiceManagerReport(
      operation: 'sweep database',
      database: targetDatabase,
      lines: lines,
    );
  }

  Future<FirebirdBackupReport> backupDatabase({
    String? database,
    required String backupFile,
    FirebirdBackupOptions options = const FirebirdBackupOptions(),
  }) async {
    final targetDatabase = database ?? _options.database;
    final lines = _runLineService(
      operation: 'backup database',
      configure: (spb) {
        spb.insertTag(status, fbclient.FbConsts.isc_action_svc_backup);
        spb.insertString(
          status,
          fbclient.FbConsts.isc_spb_dbname,
          targetDatabase,
        );
        spb.insertString(
          status,
          fbclient.FbConsts.isc_spb_bkp_file,
          backupFile,
        );
        final serviceOptions = options.serviceOptions;
        if (serviceOptions != 0) {
          spb.insertInt(
            status,
            fbclient.FbConsts.isc_spb_options,
            serviceOptions,
          );
        }
      },
    );

    return FirebirdBackupReport(
      database: targetDatabase,
      backupFile: backupFile,
      lines: lines,
    );
  }

  Future<FirebirdRestoreReport> restoreDatabase({
    required String backupFile,
    required String database,
    FirebirdRestoreOptions options = const FirebirdRestoreOptions(),
  }) async {
    final lines = _runLineService(
      operation: 'restore database',
      configure: (spb) {
        spb.insertTag(status, fbclient.FbConsts.isc_action_svc_restore);
        spb.insertString(
          status,
          fbclient.FbConsts.isc_spb_bkp_file,
          backupFile,
        );
        spb.insertString(status, fbclient.FbConsts.isc_spb_dbname, database);
        final serviceOptions = options.serviceOptions;
        if (serviceOptions != 0) {
          spb.insertInt(
            status,
            fbclient.FbConsts.isc_spb_options,
            serviceOptions,
          );
        }
        if (options.pageBuffers case final pageBuffers?) {
          spb.insertInt(
            status,
            fbclient.FbConsts.isc_spb_res_buffers,
            pageBuffers,
          );
        }
        if (options.pageSize case final pageSize?) {
          spb.insertInt(
            status,
            fbclient.FbConsts.isc_spb_res_page_size,
            pageSize,
          );
        }
        if (options.readOnly case final readOnly?) {
          spb.insertInt(
            status,
            fbclient.FbConsts.isc_spb_res_access_mode,
            readOnly
                ? fbclient.FbConsts.isc_spb_res_am_readonly
                : fbclient.FbConsts.isc_spb_res_am_readwrite,
          );
        }
      },
    );

    return FirebirdRestoreReport(
      database: database,
      backupFile: backupFile,
      lines: lines,
    );
  }

  Future<FirebirdServiceManagerReport> shutdownDatabase({
    String? database,
    FirebirdDatabaseShutdownOptions options =
        const FirebirdDatabaseShutdownOptions(),
  }) async {
    final targetDatabase = database ?? _options.database;
    final lines = _runLineService(
      operation: 'shutdown database',
      configure: (spb) {
        spb.insertTag(status, fbclient.FbConsts.isc_action_svc_properties);
        spb.insertString(
          status,
          fbclient.FbConsts.isc_spb_dbname,
          targetDatabase,
        );
        _insertSingleByteInt(
          spb,
          status: status,
          tag: fbclient.FbConsts.isc_spb_prp_shutdown_mode,
          value: options.mode.serviceValue,
        );
        spb.insertInt(
          status,
          options.method.serviceTag,
          options.timeoutSeconds,
        );
      },
    );

    return FirebirdServiceManagerReport(
      operation: 'shutdown database',
      database: targetDatabase,
      lines: lines,
    );
  }

  Future<FirebirdServiceManagerReport> bringDatabaseOnline({
    String? database,
    FirebirdDatabaseOnlineMode mode = FirebirdDatabaseOnlineMode.normal,
  }) async {
    final targetDatabase = database ?? _options.database;
    final lines = _runLineService(
      operation: 'bring database online',
      configure: (spb) {
        spb.insertTag(status, fbclient.FbConsts.isc_action_svc_properties);
        spb.insertString(
          status,
          fbclient.FbConsts.isc_spb_dbname,
          targetDatabase,
        );
        _insertSingleByteInt(
          spb,
          status: status,
          tag: fbclient.FbConsts.isc_spb_prp_online_mode,
          value: mode.serviceValue,
        );
      },
    );

    return FirebirdServiceManagerReport(
      operation: 'bring database online',
      database: targetDatabase,
      lines: lines,
    );
  }

  List<String> _queryServiceTextItems({
    required int requestedItem,
    required String operation,
  }) {
    _ensureOpen();
    final service = _serviceOrThrow();
    final receiveItems = calloc<Uint8>(1);
    final responseBuffer = calloc<Uint8>(_serviceQueryBufferSize);

    try {
      receiveItems[0] = requestedItem;
      _safeStatusInit(status);
      service.query(
        status,
        0,
        nullptr,
        1,
        receiveItems,
        _serviceQueryBufferSize,
        responseBuffer,
      );
      final chunk = _decodeServiceQueryChunk(
        buffer: responseBuffer,
        bufferLength: _serviceQueryBufferSize,
      );
      return chunk.values;
    } on fbclient.FbStatusException catch (error) {
      throw mapFbStatusException(
        exception: error,
        util: util,
        operation: operation,
      );
    } finally {
      calloc.free(responseBuffer);
      calloc.free(receiveItems);
    }
  }

  List<String> _runLineService({
    required String operation,
    required void Function(fbclient.IXpbBuilder spb) configure,
  }) {
    _ensureOpen();
    final service = _serviceOrThrow();
    fbclient.IXpbBuilder? spb;
    final receiveItems = calloc<Uint8>(1);
    final responseBuffer = calloc<Uint8>(_serviceQueryBufferSize);

    try {
      _safeStatusInit(status);
      spb = util.getXpbBuilder(status, fbclient.IXpbBuilder.spbStart);
      configure(spb);

      service.start(status, spb.getBufferLength(status), spb.getBuffer(status));

      receiveItems[0] = fbclient.FbConsts.isc_info_svc_line;

      final lines = <String>[];
      while (true) {
        responseBuffer.setAllBytes(_serviceQueryBufferSize, 0);
        _safeStatusInit(status);
        service.query(
          status,
          0,
          nullptr,
          1,
          receiveItems,
          _serviceQueryBufferSize,
          responseBuffer,
        );

        final chunk = _decodeServiceQueryChunk(
          buffer: responseBuffer,
          bufferLength: _serviceQueryBufferSize,
        );
        lines.addAll(chunk.values.where((line) => line.isNotEmpty));
        if (!chunk.hasMore) {
          return lines;
        }
      }
    } on fbclient.FbStatusException catch (error) {
      throw mapFbStatusException(
        exception: error,
        util: util,
        operation: operation,
      );
    } finally {
      spb?.dispose();
      calloc.free(responseBuffer);
      calloc.free(receiveItems);
    }
  }

  void _ensureOpen() {
    if (_isClosed) {
      throw StateError('Firebird service manager connection is closed.');
    }
  }

  fbclient.IService _serviceOrThrow() {
    final service = _service;
    if (service == null) {
      throw StateError('Firebird service manager connection is closed.');
    }
    return service;
  }
}

@immutable
class FirebirdDatabaseStatisticsOptions {
  const FirebirdDatabaseStatisticsOptions({
    this.includeDataPages = false,
    this.includeDatabaseLog = false,
    this.includeHeaderPages = true,
    this.includeIndexPages = false,
    this.includeSystemRelations = false,
    this.includeRecordVersions = false,
    this.excludeCreationDate = true,
    this.includeEncryption = false,
  }) : assert(
         !includeHeaderPages ||
             (!includeDataPages &&
                 !includeIndexPages &&
                 !includeSystemRelations &&
                 !includeRecordVersions),
         'Header statistics cannot be combined with data, index, system, or '
         'record-version statistics.',
       );

  final bool includeDataPages;
  final bool includeDatabaseLog;
  final bool includeHeaderPages;
  final bool includeIndexPages;
  final bool includeSystemRelations;
  final bool includeRecordVersions;
  final bool excludeCreationDate;
  final bool includeEncryption;

  int get serviceOptions {
    var options = 0;
    if (includeDataPages) {
      options |= fbclient.FbConsts.isc_spb_sts_data_pages;
    }
    if (includeDatabaseLog) {
      options |= fbclient.FbConsts.isc_spb_sts_db_log;
    }
    if (includeHeaderPages) {
      options |= fbclient.FbConsts.isc_spb_sts_hdr_pages;
    }
    if (includeIndexPages) {
      options |= fbclient.FbConsts.isc_spb_sts_idx_pages;
    }
    if (includeSystemRelations) {
      options |= fbclient.FbConsts.isc_spb_sts_sys_relations;
    }
    if (includeRecordVersions) {
      options |= fbclient.FbConsts.isc_spb_sts_record_versions;
    }
    if (excludeCreationDate) {
      options |= fbclient.FbConsts.isc_spb_sts_nocreation;
    }
    if (includeEncryption) {
      options |= fbclient.FbConsts.isc_spb_sts_encryption;
    }
    return options;
  }
}

@immutable
class FirebirdBackupOptions {
  const FirebirdBackupOptions({
    this.ignoreChecksums = false,
    this.ignoreLimboTransactions = false,
    this.metadataOnly = false,
    this.noGarbageCollect = false,
    this.nonTransportable = false,
    this.convertExternalTables = false,
    this.expand = false,
    this.noTriggers = false,
    this.zip = false,
  });

  final bool ignoreChecksums;
  final bool ignoreLimboTransactions;
  final bool metadataOnly;
  final bool noGarbageCollect;
  final bool nonTransportable;
  final bool convertExternalTables;
  final bool expand;
  final bool noTriggers;
  final bool zip;

  int get serviceOptions {
    var options = 0;
    if (ignoreChecksums) {
      options |= fbclient.FbConsts.isc_spb_bkp_ignore_checksums;
    }
    if (ignoreLimboTransactions) {
      options |= fbclient.FbConsts.isc_spb_bkp_ignore_limbo;
    }
    if (metadataOnly) {
      options |= fbclient.FbConsts.isc_spb_bkp_metadata_only;
    }
    if (noGarbageCollect) {
      options |= fbclient.FbConsts.isc_spb_bkp_no_garbage_collect;
    }
    if (nonTransportable) {
      options |= fbclient.FbConsts.isc_spb_bkp_non_transportable;
    }
    if (convertExternalTables) {
      options |= fbclient.FbConsts.isc_spb_bkp_convert;
    }
    if (expand) {
      options |= fbclient.FbConsts.isc_spb_bkp_expand;
    }
    if (noTriggers) {
      options |= fbclient.FbConsts.isc_spb_bkp_no_triggers;
    }
    if (zip) {
      options |= fbclient.FbConsts.isc_spb_bkp_zip;
    }
    return options;
  }
}

@immutable
class FirebirdRestoreOptions {
  const FirebirdRestoreOptions({
    this.createDatabase = true,
    this.replaceExisting = false,
    this.metadataOnly = false,
    this.deactivateIndexes = false,
    this.noShadow = false,
    this.noValidity = false,
    this.oneAtATime = false,
    this.useAllSpace = false,
    this.readOnly,
    this.pageBuffers,
    this.pageSize,
  }) : assert(
         !(createDatabase && replaceExisting),
         'Restore cannot request both create and replace.',
       ),
       assert(
         pageBuffers == null || pageBuffers > 0,
         'Restore pageBuffers must be positive when provided.',
       ),
       assert(
         pageSize == null || pageSize > 0,
         'Restore pageSize must be positive when provided.',
       );

  final bool createDatabase;
  final bool replaceExisting;
  final bool metadataOnly;
  final bool deactivateIndexes;
  final bool noShadow;
  final bool noValidity;
  final bool oneAtATime;
  final bool useAllSpace;
  final bool? readOnly;
  final int? pageBuffers;
  final int? pageSize;

  int get serviceOptions {
    var options = 0;
    if (metadataOnly) {
      options |= fbclient.FbConsts.isc_spb_res_metadata_only;
    }
    if (deactivateIndexes) {
      options |= fbclient.FbConsts.isc_spb_res_deactivate_idx;
    }
    if (noShadow) {
      options |= fbclient.FbConsts.isc_spb_res_no_shadow;
    }
    if (noValidity) {
      options |= fbclient.FbConsts.isc_spb_res_no_validity;
    }
    if (oneAtATime) {
      options |= fbclient.FbConsts.isc_spb_res_one_at_a_time;
    }
    if (replaceExisting) {
      options |= fbclient.FbConsts.isc_spb_res_replace;
    }
    if (createDatabase) {
      options |= fbclient.FbConsts.isc_spb_res_create;
    }
    if (useAllSpace) {
      options |= fbclient.FbConsts.isc_spb_res_use_all_space;
    }
    return options;
  }
}

enum FirebirdDatabaseShutdownMode {
  multi,
  single,
  full;

  int get serviceValue => switch (this) {
    FirebirdDatabaseShutdownMode.multi =>
      fbclient.FbConsts.isc_spb_prp_sm_multi,
    FirebirdDatabaseShutdownMode.single =>
      fbclient.FbConsts.isc_spb_prp_sm_single,
    FirebirdDatabaseShutdownMode.full => fbclient.FbConsts.isc_spb_prp_sm_full,
  };
}

enum FirebirdDatabaseShutdownMethod {
  force,
  attachments,
  transactions;

  int get serviceTag => switch (this) {
    FirebirdDatabaseShutdownMethod.force =>
      fbclient.FbConsts.isc_spb_prp_force_shutdown,
    FirebirdDatabaseShutdownMethod.attachments =>
      fbclient.FbConsts.isc_spb_prp_attachments_shutdown,
    FirebirdDatabaseShutdownMethod.transactions =>
      fbclient.FbConsts.isc_spb_prp_transactions_shutdown,
  };
}

enum FirebirdDatabaseOnlineMode {
  normal,
  multi,
  single;

  int get serviceValue => switch (this) {
    FirebirdDatabaseOnlineMode.normal =>
      fbclient.FbConsts.isc_spb_prp_sm_normal,
    FirebirdDatabaseOnlineMode.multi => fbclient.FbConsts.isc_spb_prp_sm_multi,
    FirebirdDatabaseOnlineMode.single =>
      fbclient.FbConsts.isc_spb_prp_sm_single,
  };
}

@immutable
class FirebirdDatabaseShutdownOptions {
  const FirebirdDatabaseShutdownOptions({
    this.mode = FirebirdDatabaseShutdownMode.full,
    this.method = FirebirdDatabaseShutdownMethod.force,
    this.timeoutSeconds = 0,
  }) : assert(
         timeoutSeconds >= 0,
         'Shutdown timeoutSeconds must be non-negative.',
       );

  final FirebirdDatabaseShutdownMode mode;
  final FirebirdDatabaseShutdownMethod method;
  final int timeoutSeconds;
}

@immutable
class FirebirdServiceManagerReport {
  FirebirdServiceManagerReport({
    required this.operation,
    required this.database,
    required List<String> lines,
  }) : lines = List.unmodifiable(lines);

  final String operation;
  final String database;
  final List<String> lines;
}

@immutable
class FirebirdBackupReport {
  FirebirdBackupReport({
    required this.database,
    required this.backupFile,
    required List<String> lines,
  }) : lines = List.unmodifiable(lines);

  final String database;
  final String backupFile;
  final List<String> lines;
}

@immutable
class FirebirdRestoreReport {
  FirebirdRestoreReport({
    required this.database,
    required this.backupFile,
    required List<String> lines,
  }) : lines = List.unmodifiable(lines);

  final String database;
  final String backupFile;
  final List<String> lines;
}

String _serviceManagerAttachmentString(FirebirdConnectionOptions options) {
  final host = _trimOrNull(options.host);
  if (host == null) return 'service_mgr';
  if (options.port case final port?) {
    return '$host/$port:service_mgr';
  }
  return '$host:service_mgr';
}

String? _trimOrNull(String? value) {
  if (value == null) return null;
  final trimmed = value.trim();
  return trimmed.isEmpty ? null : trimmed;
}

({List<String> values, bool hasMore}) _decodeServiceQueryChunk({
  required Pointer<Uint8> buffer,
  required int bufferLength,
}) {
  final values = <String>[];
  var hasMore = false;
  var offset = 0;

  while (offset < bufferLength) {
    final parameterCode = buffer[offset];
    if (parameterCode == fbclient.FbConsts.isc_info_end) {
      break;
    }

    offset++;
    switch (parameterCode) {
      case fbclient.FbConsts.isc_info_svc_server_version:
      case fbclient.FbConsts.isc_info_svc_line:
        final result = _readServiceString(
          buffer: buffer,
          offset: offset,
          bufferLength: bufferLength,
        );
        offset = result.offset;
        values.add(result.value);
        hasMore = hasMore || result.value.isNotEmpty;
      case fbclient.FbConsts.isc_info_truncated:
      case fbclient.FbConsts.isc_info_svc_timeout:
      case fbclient.FbConsts.isc_info_data_not_ready:
        hasMore = true;
      default:
        throw StateError(
          'Unsupported Firebird service info item: 0x'
          '${parameterCode.toRadixString(16).padLeft(2, '0')}',
        );
    }
  }

  return (values: values, hasMore: hasMore);
}

({int offset, String value}) _readServiceString({
  required Pointer<Uint8> buffer,
  required int offset,
  required int bufferLength,
}) {
  if (offset + 2 > bufferLength) {
    throw StateError('Malformed Firebird service response.');
  }

  final length = buffer.readVaxInt16(offset);
  final valueOffset = offset + 2;
  if (valueOffset + length > bufferLength) {
    throw StateError('Malformed Firebird service response.');
  }

  return (
    offset: valueOffset + length,
    value: buffer.readString(valueOffset, length),
  );
}

void _safeStatusInit(fbclient.IStatus status) {
  try {
    status.init();
  } catch (_) {}
}

void _insertSingleByteInt(
  fbclient.IXpbBuilder spb, {
  required fbclient.IStatus status,
  required int tag,
  required int value,
}) {
  final bytes = calloc<Uint8>(1);
  try {
    bytes[0] = value;
    spb.insertBytes(status, tag, bytes, 1);
  } finally {
    calloc.free(bytes);
  }
}
