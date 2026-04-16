import 'dart:convert';
import 'dart:ffi';
import 'dart:math';
import 'dart:typed_data';

import 'package:fbdb/fbclient.dart' as fbclient;
import 'package:ffi/ffi.dart';

import '../firebird_cancel_mode.dart';
import '../firebird_connection_options.dart';
import '../firebird_error_mapper.dart';
import '../firebird_execution_result.dart';
import '../firebird_native_client.dart';
import '../firebird_transaction_settings.dart';
import '../firebird_value_types.dart';

/// First owned low-level transport that talks to Firebird through the
/// `fbclient` OO API directly.
///
/// This slice intentionally supports a narrow but useful subset:
/// - attach using DPB credentials
/// - keep one default transaction per connection
/// - prepare and execute simple statements
/// - decode common scalar result types
///
/// The current implementation is synchronous under the hood because `fbclient`
/// calls are blocking FFI calls. The async surface exists to keep the seam
/// aligned with the rest of `firebirdpod`.
class FirebirdFbClientNativeClient implements FirebirdNativeClient {
  const FirebirdFbClientNativeClient({this.fbClientLibraryPath});

  final String? fbClientLibraryPath;

  @override
  Future<FirebirdNativeConnection> attach(
    FirebirdConnectionOptions options,
  ) async {
    final client = fbclient.FbClient(fbClientLibraryPath);
    final master = client.fbGetMasterInterface();
    final status = master.getStatus();
    final provider = master.getDispatcher();
    final util = master.getUtilInterface();
    fbclient.IXpbBuilder? dpb;

    try {
      status.init();
      dpb = util.getXpbBuilder(status, fbclient.IXpbBuilder.dpb);
      dpb.insertString(
        status,
        fbclient.FbConsts.isc_dpb_user_name,
        options.user,
      );
      dpb.insertString(
        status,
        fbclient.FbConsts.isc_dpb_password,
        options.password,
      );
      dpb.insertString(
        status,
        fbclient.FbConsts.isc_dpb_lc_ctype,
        options.charset,
      );
      if (options.role case final role? when role.isNotEmpty) {
        dpb.insertString(status, fbclient.FbConsts.isc_dpb_sql_role_name, role);
      }

      final attachment = provider.attachDatabase(
        status,
        options.attachmentString,
        dpb.getBufferLength(status),
        dpb.getBuffer(status),
      );
      final retainedTransaction = _startTransaction(
        attachment: attachment,
        status: status,
        util: util,
        readOnly: false,
        settings: const FirebirdTransactionSettings(),
      );

      return _FirebirdFbClientNativeConnection(
        client: client,
        status: status,
        provider: provider,
        util: util,
        attachment: attachment,
        retainedTransaction: retainedTransaction,
      );
    } catch (error) {
      final mappedError = error is fbclient.FbStatusException
          ? mapFbStatusException(
              exception: error,
              util: util,
              operation: 'attach',
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
      dpb?.dispose();
    }
  }
}

class _FirebirdFbClientNativeConnection implements FirebirdNativeConnection {
  static const _retainedTransactionSettings = FirebirdTransactionSettings();

  _FirebirdFbClientNativeConnection({
    required this.client,
    required this.status,
    required this.provider,
    required this.util,
    required this.attachment,
    required fbclient.ITransaction retainedTransaction,
  }) : _retainedTransaction = retainedTransaction;

  final fbclient.FbClient client;
  final fbclient.IStatus status;
  final fbclient.IProvider provider;
  final fbclient.IUtil util;
  final fbclient.IAttachment attachment;
  fbclient.ITransaction? _retainedTransaction;

  bool _isClosed = false;

  @override
  Future<void> cancelOperation(FirebirdCancelMode mode) async {
    _ensureOpen();
    try {
      _safeStatusInit(status);
      attachment.cancelOperation(status, mode.wireValue);
    } on fbclient.FbStatusException catch (error) {
      throw mapFbStatusException(
        exception: error,
        util: util,
        operation: 'cancel operation',
      );
    }
  }

  @override
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
              operation: 'close connection',
            )
          : error;
      firstStackTrace ??= stackTrace;
    }

    final retainedTransaction = _retainedTransaction;
    _retainedTransaction = null;
    if (retainedTransaction != null) {
      _safeStatusInit(status);
      try {
        retainedTransaction.rollback(status);
      } catch (error, stackTrace) {
        recordError(error, stackTrace);
      }

      try {
        retainedTransaction.release();
      } catch (error, stackTrace) {
        recordError(error, stackTrace);
      }
    }

    _safeStatusInit(status);
    try {
      attachment.detach(status);
    } catch (error, stackTrace) {
      recordError(error, stackTrace);
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

  @override
  Future<FirebirdNativeStatement> prepareStatement(String sql) async {
    _ensureOpen();
    final retainedTransaction = _retainedTransactionOrThrow();
    try {
      return await _prepareStatement(
        sql,
        transaction: retainedTransaction,
        retainedTransactionMode: true,
      );
    } on fbclient.FbStatusException catch (error) {
      throw mapFbStatusException(
        exception: error,
        util: util,
        operation: 'prepare',
      );
    }
  }

  @override
  Future<void> resetRetainedState() async {
    _ensureOpen();

    Object? firstError;
    StackTrace? firstStackTrace;

    void recordError(Object error, StackTrace stackTrace) {
      firstError ??= error is fbclient.FbStatusException
          ? mapFbStatusException(
              exception: error,
              util: util,
              operation: 'reset retained transaction',
            )
          : error;
      firstStackTrace ??= stackTrace;
    }

    final previousTransaction = _retainedTransaction;
    _retainedTransaction = null;

    if (previousTransaction != null) {
      _safeStatusInit(status);
      try {
        previousTransaction.rollback(status);
      } catch (error, stackTrace) {
        recordError(error, stackTrace);
      }

      try {
        previousTransaction.release();
      } catch (error, stackTrace) {
        recordError(error, stackTrace);
      }
    }

    try {
      _retainedTransaction = _startTransaction(
        attachment: attachment,
        status: status,
        util: util,
        readOnly: false,
        settings: _retainedTransactionSettings,
      );
    } catch (error, stackTrace) {
      recordError(error, stackTrace);
    }

    if (firstError != null) {
      Error.throwWithStackTrace(firstError!, firstStackTrace!);
    }
  }

  @override
  Future<FirebirdNativeTransaction> beginTransaction({
    bool readOnly = false,
    FirebirdTransactionSettings settings = const FirebirdTransactionSettings(),
  }) async {
    _ensureOpen();
    try {
      final transaction = _startTransaction(
        attachment: attachment,
        status: status,
        util: util,
        readOnly: readOnly,
        settings: settings,
      );
      return _FirebirdFbClientNativeTransaction(
        owner: this,
        transaction: transaction,
      );
    } on fbclient.FbStatusException catch (error) {
      throw mapFbStatusException(
        exception: error,
        util: util,
        operation: 'begin transaction',
      );
    }
  }

  @override
  Future<Duration?> getStatementTimeout() async {
    _ensureOpen();
    try {
      _safeStatusInit(status);
      final milliseconds = attachment.getStatementTimeout(status);
      return milliseconds == 0 ? null : Duration(milliseconds: milliseconds);
    } on fbclient.FbStatusException catch (error) {
      throw mapFbStatusException(
        exception: error,
        util: util,
        operation: 'get statement timeout',
      );
    }
  }

  @override
  Future<void> setStatementTimeout(Duration? timeout) async {
    _ensureOpen();
    final statement = await prepareStatement(_statementTimeoutSql(timeout));
    try {
      await statement.execute(const <Object?>[]);
    } finally {
      await statement.close();
    }
  }

  Future<FirebirdNativeStatement> _prepareStatement(
    String sql, {
    required fbclient.ITransaction transaction,
    required bool retainedTransactionMode,
  }) async {
    _ensureOpen();

    _safeStatusInit(status);
    final statement = attachment.prepare(
      status,
      transaction,
      sql,
      fbclient.FbConsts.sqlDialectCurrent,
      fbclient.IStatement.preparePrefetchMetadata,
    );

    fbclient.IMessageMetadata? inputMetadata;
    fbclient.IMessageMetadata? outputMetadata;
    try {
      final rawInputMetadata = statement.getInputMetadata(status);
      final inputFields = _describeFields(status, rawInputMetadata);
      final inputMessageLength = rawInputMetadata.getMessageLength(status);
      if (inputFields.isEmpty) {
        rawInputMetadata.release();
      } else {
        inputMetadata = rawInputMetadata;
      }

      final rawOutputMetadata = statement.getOutputMetadata(status);
      final outputFields = _describeFields(status, rawOutputMetadata);
      final outputMessageLength = rawOutputMetadata.getMessageLength(status);
      if (outputFields.isEmpty) {
        rawOutputMetadata.release();
      } else {
        outputMetadata = rawOutputMetadata;
      }

      final statementType = statement.getType(status);
      final flags = statement.getFlags(status);
      final hasCursor = (flags & fbclient.IStatement.flagHasCursor) != 0;

      return _FirebirdFbClientNativeStatement(
        owner: this,
        statement: statement,
        statementType: statementType,
        inputMetadata: inputMetadata,
        inputFields: inputFields,
        inputMessageLength: inputMetadata == null ? 0 : inputMessageLength,
        outputMetadata: outputMetadata,
        outputFields: outputFields,
        outputMessageLength: outputMetadata == null ? 0 : outputMessageLength,
        hasCursor: hasCursor,
        transaction: transaction,
        retainedTransactionMode: retainedTransactionMode,
      );
    } catch (_) {
      try {
        inputMetadata?.release();
      } catch (_) {}
      try {
        outputMetadata?.release();
      } catch (_) {}
      _safeStatusInit(status);
      try {
        statement.free(status);
      } catch (_) {
        try {
          statement.release();
        } catch (_) {}
      }
      rethrow;
    }
  }

  void commitRetaining() {
    _ensureOpen();
    _safeStatusInit(status);
    _retainedTransactionOrThrow().commitRetaining(status);
  }

  void rollbackRetaining() {
    if (_isClosed) return;
    _safeStatusInit(status);
    _retainedTransactionOrThrow().rollbackRetaining(status);
  }

  void _ensureOpen() {
    if (_isClosed) {
      throw StateError('The direct fbclient connection is already closed.');
    }
  }

  fbclient.ITransaction _retainedTransactionOrThrow() {
    final retainedTransaction = _retainedTransaction;
    if (retainedTransaction == null) {
      throw StateError(
        'The direct fbclient retained transaction is unavailable. '
        'Close and recreate the connection.',
      );
    }
    return retainedTransaction;
  }

  String _statementTimeoutSql(Duration? timeout) {
    final milliseconds = timeout?.inMilliseconds ?? 0;
    return 'set statement timeout $milliseconds millisecond';
  }
}

class _FirebirdFbClientNativeStatement implements FirebirdNativeStatement {
  _FirebirdFbClientNativeStatement({
    required _FirebirdFbClientNativeConnection owner,
    required fbclient.IStatement statement,
    required int statementType,
    required fbclient.IMessageMetadata? inputMetadata,
    required List<_FirebirdFieldDescriptor> inputFields,
    required int inputMessageLength,
    required fbclient.IMessageMetadata? outputMetadata,
    required List<_FirebirdFieldDescriptor> outputFields,
    required int outputMessageLength,
    required bool hasCursor,
    required fbclient.ITransaction transaction,
    required bool retainedTransactionMode,
  }) : _owner = owner,
       _statement = statement,
       _statementType = statementType,
       _inputMetadata = inputMetadata,
       _inputFields = inputFields,
       _inputMessageLength = inputMessageLength,
       _outputMetadata = outputMetadata,
       _outputFields = outputFields,
       _outputMessageLength = outputMessageLength,
       _hasCursor = hasCursor,
       _transaction = transaction,
       _retainedTransactionMode = retainedTransactionMode;

  final _FirebirdFbClientNativeConnection _owner;
  final fbclient.IStatement _statement;
  final int _statementType;
  final fbclient.IMessageMetadata? _inputMetadata;
  final List<_FirebirdFieldDescriptor> _inputFields;
  final int _inputMessageLength;
  final fbclient.IMessageMetadata? _outputMetadata;
  final List<_FirebirdFieldDescriptor> _outputFields;
  final int _outputMessageLength;
  final bool _hasCursor;
  final fbclient.ITransaction _transaction;
  final bool _retainedTransactionMode;

  bool _isClosed = false;

  @override
  Future<void> close() async {
    if (_isClosed) return;
    _isClosed = true;

    Object? firstError;
    StackTrace? firstStackTrace;

    void recordError(Object error, StackTrace stackTrace) {
      firstError ??= error is fbclient.FbStatusException
          ? mapFbStatusException(
              exception: error,
              util: _owner.util,
              operation: 'close statement',
            )
          : error;
      firstStackTrace ??= stackTrace;
    }

    try {
      _inputMetadata?.release();
    } catch (error, stackTrace) {
      recordError(error, stackTrace);
    }

    try {
      _outputMetadata?.release();
    } catch (error, stackTrace) {
      recordError(error, stackTrace);
    }

    _safeStatusInit(_owner.status);
    try {
      _statement.free(_owner.status);
    } catch (error, stackTrace) {
      recordError(error, stackTrace);
      try {
        _statement.release();
      } catch (_) {}
    }

    if (firstError != null) {
      Error.throwWithStackTrace(firstError!, firstStackTrace!);
    }
  }

  @override
  Future<Duration?> getTimeout() async {
    _ensureOpen();
    try {
      _safeStatusInit(_owner.status);
      final milliseconds = _statement.getTimeout(_owner.status);
      return milliseconds == 0 ? null : Duration(milliseconds: milliseconds);
    } on fbclient.FbStatusException catch (error) {
      throw mapFbStatusException(
        exception: error,
        util: _owner.util,
        operation: 'get statement timeout',
      );
    }
  }

  @override
  Future<void> setTimeout(Duration? timeout) async {
    _ensureOpen();
    try {
      _safeStatusInit(_owner.status);
      _statement.setTimeout(_owner.status, timeout?.inMilliseconds ?? 0);
    } on fbclient.FbStatusException catch (error) {
      throw mapFbStatusException(
        exception: error,
        util: _owner.util,
        operation: 'set statement timeout',
      );
    }
  }

  @override
  Future<FirebirdExecutionResult> execute(List<Object?> values) async {
    _ensureOpen();

    Pointer<Uint8>? inputBuffer;
    Pointer<Uint8>? outputBuffer;
    fbclient.IResultSet? cursor;

    try {
      inputBuffer = _encodeInputMessage(values);

      if (_hasCursor) {
        final outputMetadata = _outputMetadata;
        if (outputMetadata == null) {
          throw StateError(
            'The direct fbclient statement has a cursor but no output metadata.',
          );
        }

        _safeStatusInit(_owner.status);
        cursor = _statement.openCursor(
          _owner.status,
          _transaction,
          _inputMetadata,
          inputBuffer,
          outputMetadata,
        );

        outputBuffer = fbclient.mem.allocate<Uint8>(_outputMessageLength);
        outputBuffer.setAllBytes(_outputMessageLength, 0);

        final rows = <Map<String, Object?>>[];
        while (cursor.fetchNext(_owner.status, outputBuffer) ==
            fbclient.IStatus.resultOK) {
          rows.add(_decodeRow(outputBuffer));
        }

        _safeStatusInit(_owner.status);
        cursor.close(_owner.status);
        cursor = null;

        if (_retainedTransactionMode) {
          _owner.commitRetaining();
        }
        return FirebirdExecutionResult(rows: rows);
      }

      if (_outputMetadata != null) {
        outputBuffer = fbclient.mem.allocate<Uint8>(_outputMessageLength);
        outputBuffer.setAllBytes(_outputMessageLength, 0);
      }

      _safeStatusInit(_owner.status);
      _statement.execute(
        _owner.status,
        _transaction,
        _inputMetadata,
        inputBuffer,
        _outputMetadata,
        outputBuffer,
      );

      final rows = outputBuffer == null
          ? const <Map<String, Object?>>[]
          : <Map<String, Object?>>[_decodeRow(outputBuffer)];
      var affectedRows = _statement.getAffectedRecords(_owner.status);
      if (affectedRows == 0 &&
          rows.isNotEmpty &&
          _shouldUseOutputRowCountFallback(_statementType)) {
        affectedRows = rows.length;
      }

      if (_retainedTransactionMode) {
        _owner.commitRetaining();
      }
      return FirebirdExecutionResult(affectedRows: affectedRows, rows: rows);
    } on fbclient.FbStatusException catch (error) {
      final mappedError = mapFbStatusException(
        exception: error,
        util: _owner.util,
        operation: 'execute',
      );
      _safeErrorRecovery();
      throw mappedError;
    } catch (_) {
      _safeErrorRecovery();
      rethrow;
    } finally {
      if (cursor != null) {
        _safeStatusInit(_owner.status);
        try {
          cursor.close(_owner.status);
        } catch (_) {
          try {
            cursor.release();
          } catch (_) {}
        }
      }
      if (inputBuffer != null) {
        fbclient.mem.free(inputBuffer);
      }
      if (outputBuffer != null) {
        fbclient.mem.free(outputBuffer);
      }
    }
  }

  Pointer<Uint8>? _encodeInputMessage(List<Object?> values) {
    if (_inputFields.isEmpty) {
      if (values.isNotEmpty) {
        throw ArgumentError.value(
          values,
          'values',
          'This statement does not accept parameters.',
        );
      }
      return null;
    }

    if (values.length != _inputFields.length) {
      throw ArgumentError(
        'Expected ${_inputFields.length} bound values, got ${values.length}.',
      );
    }

    final message = fbclient.mem.allocate<Uint8>(_inputMessageLength);
    message.setAllBytes(_inputMessageLength, 0);
    try {
      for (var index = 0; index < _inputFields.length; index++) {
        _encodeField(message, _inputFields[index], values[index]);
      }
      return message;
    } catch (_) {
      fbclient.mem.free(message);
      rethrow;
    }
  }

  void _encodeField(
    Pointer<Uint8> message,
    _FirebirdFieldDescriptor field,
    Object? value,
  ) {
    if (value == null) {
      if (!field.nullable) {
        throw ArgumentError(
          'Parameter ${field.index + 1} (${field.name}) is not nullable.',
        );
      }
      message.writeUint16(field.nullOffset, 1);
      return;
    }

    message.writeUint16(field.nullOffset, 0);

    final fieldType = field.type & ~1;

    switch (fieldType) {
      case fbclient.FbConsts.SQL_TEXT:
        _writeFixedText(message, field, value);
        return;
      case fbclient.FbConsts.SQL_VARYING:
        message.writeVarchar(
          field.offset,
          _requireString(field, value),
          field.length + sizeOf<Uint16>(),
        );
        return;
      case fbclient.FbConsts.SQL_SHORT:
        message.writeInt16(field.offset, _encodeScaledInteger(field, value));
        return;
      case fbclient.FbConsts.SQL_LONG:
        message.writeInt32(field.offset, _encodeScaledInteger(field, value));
        return;
      case fbclient.FbConsts.SQL_INT64:
        message.writeInt64(field.offset, _encodeScaledInteger(field, value));
        return;
      case fbclient.FbConsts.SQL_FLOAT:
        message.writeFloat(field.offset, _requireNum(field, value).toDouble());
        return;
      case fbclient.FbConsts.SQL_DOUBLE:
        message.writeDouble(field.offset, _requireNum(field, value).toDouble());
        return;
      case fbclient.FbConsts.SQL_BOOLEAN:
        message.writeUint8(field.offset, _requireBool(field, value) ? 1 : 0);
        return;
      case fbclient.FbConsts.SQL_TYPE_DATE:
        final dateTime = _requireDateTime(field, value);
        message.writeInt32(
          field.offset,
          _owner.util.encodeDate(dateTime.year, dateTime.month, dateTime.day),
        );
        return;
      case fbclient.FbConsts.SQL_TYPE_TIME:
        final dateTime = _requireDateTime(field, value);
        message.writeUint32(
          field.offset,
          _owner.util.encodeTime(
            dateTime.hour,
            dateTime.minute,
            dateTime.second,
            dateTime.millisecond * 10 + dateTime.microsecond ~/ 100,
          ),
        );
        return;
      case fbclient.FbConsts.SQL_TIMESTAMP:
        final dateTime = _requireDateTime(field, value);
        final timestamp = (message + field.offset)
            .cast<fbclient.IscTimestamp>();
        timestamp.ref
          ..date = _owner.util.encodeDate(
            dateTime.year,
            dateTime.month,
            dateTime.day,
          )
          ..time = _owner.util.encodeTime(
            dateTime.hour,
            dateTime.minute,
            dateTime.second,
            dateTime.millisecond * 10 + dateTime.microsecond ~/ 100,
          );
        return;
      case fbclient.FbConsts.SQL_BLOB:
        _writeBlob(message, field, value);
        return;
      case fbclient.FbConsts.SQL_INT128:
        _writeInt128(message, field, value);
        return;
      case fbclient.FbConsts.SQL_DEC16:
        _writeDecFloat16(message, field, value);
        return;
      case fbclient.FbConsts.SQL_DEC34:
        _writeDecFloat34(message, field, value);
        return;
      case fbclient.FbConsts.SQL_TIMESTAMP_TZ:
      case fbclient.FbConsts.SQL_TIMESTAMP_TZ_EX:
        _writeTimestampWithTimeZone(message, field, value);
        return;
      case fbclient.FbConsts.SQL_TIME_TZ:
      case fbclient.FbConsts.SQL_TIME_TZ_EX:
        _writeTimeWithTimeZone(message, field, value);
        return;
      default:
        throw UnsupportedError(
          'Direct fbclient transport does not yet support input type '
          '${field.type} for ${field.name}.',
        );
    }
  }

  Map<String, Object?> _decodeRow(Pointer<Uint8> message) {
    final row = <String, Object?>{};
    for (final field in _outputFields) {
      row[field.name] = _decodeField(message, field);
    }
    return row;
  }

  Object? _decodeField(Pointer<Uint8> message, _FirebirdFieldDescriptor field) {
    if (message.readUint16(field.nullOffset) > 0) {
      return null;
    }

    final fieldType = field.type & ~1;

    switch (fieldType) {
      case fbclient.FbConsts.SQL_TEXT:
        final value = message.readString(field.offset, field.length);
        return field.charSet == 1 ? value : _trimTrailingSpaces(value);
      case fbclient.FbConsts.SQL_VARYING:
        return message.readVarchar(field.offset);
      case fbclient.FbConsts.SQL_SHORT:
        final value = message.readInt16(field.offset);
        return field.scale == 0
            ? value
            : _decodeScaledNumber(value, field.scale);
      case fbclient.FbConsts.SQL_LONG:
        final value = message.readInt32(field.offset);
        return field.scale == 0
            ? value
            : _decodeScaledNumber(value, field.scale);
      case fbclient.FbConsts.SQL_INT64:
        final value = message.readInt64(field.offset);
        return field.scale == 0
            ? value
            : _decodeScaledNumber(value, field.scale);
      case fbclient.FbConsts.SQL_FLOAT:
        return message.readFloat(field.offset);
      case fbclient.FbConsts.SQL_DOUBLE:
        return message.readDouble(field.offset);
      case fbclient.FbConsts.SQL_BOOLEAN:
        return message.readUint8(field.offset) != 0;
      case fbclient.FbConsts.SQL_TYPE_DATE:
        return _decodeDate(message.readInt32(field.offset));
      case fbclient.FbConsts.SQL_TYPE_TIME:
        return _decodeTime(message.readUint32(field.offset));
      case fbclient.FbConsts.SQL_TIMESTAMP:
        return _decodeTimestamp((message + field.offset).cast());
      case fbclient.FbConsts.SQL_BLOB:
        return _readBlob(message, field);
      case fbclient.FbConsts.SQL_INT128:
        return _decodeInt128(message, field);
      case fbclient.FbConsts.SQL_DEC16:
        return _decodeDecFloat16(message, field);
      case fbclient.FbConsts.SQL_DEC34:
        return _decodeDecFloat34(message, field);
      case fbclient.FbConsts.SQL_TIMESTAMP_TZ:
        return _decodeTimestampWithTimeZone(message, field);
      case fbclient.FbConsts.SQL_TIMESTAMP_TZ_EX:
        return _decodeTimestampWithTimeZoneExtended(message, field);
      case fbclient.FbConsts.SQL_TIME_TZ:
        return _decodeTimeWithTimeZone(message, field);
      case fbclient.FbConsts.SQL_TIME_TZ_EX:
        return _decodeTimeWithTimeZoneExtended(message, field);
      default:
        throw UnsupportedError(
          'Direct fbclient transport does not yet support output type '
          '${field.type} for ${field.name}.',
        );
    }
  }

  DateTime _decodeDate(int encodedDate) {
    final year = fbclient.mem.allocate<UnsignedInt>(sizeOf<UnsignedInt>());
    final month = fbclient.mem.allocate<UnsignedInt>(sizeOf<UnsignedInt>());
    final day = fbclient.mem.allocate<UnsignedInt>(sizeOf<UnsignedInt>());
    try {
      _owner.util.decodeDate(encodedDate, year, month, day);
      return DateTime(year.value, month.value, day.value);
    } finally {
      fbclient.mem.free(year);
      fbclient.mem.free(month);
      fbclient.mem.free(day);
    }
  }

  DateTime _decodeTime(int encodedTime) {
    final hours = fbclient.mem.allocate<UnsignedInt>(sizeOf<UnsignedInt>());
    final minutes = fbclient.mem.allocate<UnsignedInt>(sizeOf<UnsignedInt>());
    final seconds = fbclient.mem.allocate<UnsignedInt>(sizeOf<UnsignedInt>());
    final fractions = fbclient.mem.allocate<UnsignedInt>(sizeOf<UnsignedInt>());
    try {
      _owner.util.decodeTime(encodedTime, hours, minutes, seconds, fractions);
      return DateTime(
        1,
        1,
        1,
        hours.value,
        minutes.value,
        seconds.value,
        fractions.value ~/ 10,
        (fractions.value % 10) * 100,
      );
    } finally {
      fbclient.mem.free(hours);
      fbclient.mem.free(minutes);
      fbclient.mem.free(seconds);
      fbclient.mem.free(fractions);
    }
  }

  DateTime _decodeTimestamp(Pointer<fbclient.IscTimestamp> timestamp) {
    final date = _decodeDate(timestamp.ref.date);
    final time = _decodeTime(timestamp.ref.time);
    return DateTime(
      date.year,
      date.month,
      date.day,
      time.hour,
      time.minute,
      time.second,
      time.millisecond,
      time.microsecond,
    );
  }

  void _writeBlob(
    Pointer<Uint8> message,
    _FirebirdFieldDescriptor field,
    Object? value,
  ) {
    if (value is fbclient.FbBlobId) {
      value.storeInQuad((message + field.offset).cast());
      return;
    }

    final blobBytes = _asBlobBytes(field, value);
    final blobId = (message + field.offset).cast<fbclient.IscQuad>();

    fbclient.IBlob? blob;
    Pointer<Uint8>? segmentBuffer;
    try {
      _safeStatusInit(_owner.status);
      blob = _owner.attachment.createBlob(_owner.status, _transaction, blobId);
      if (blobBytes.isNotEmpty) {
        const segmentLimit = 65535;
        segmentBuffer = fbclient.mem.allocate<Uint8>(
          min(segmentLimit, blobBytes.length),
        );
        for (
          var offset = 0;
          offset < blobBytes.length;
          offset += segmentLimit
        ) {
          final end = min(offset + segmentLimit, blobBytes.length);
          final segment = blobBytes.sublist(offset, end);
          segmentBuffer.fromDartMem(segment, segment.length, 0, 0);
          _safeStatusInit(_owner.status);
          blob.putSegment(_owner.status, segment.length, segmentBuffer);
        }
      }
      _safeStatusInit(_owner.status);
      blob.close(_owner.status);
      blob = null;
    } on fbclient.FbStatusException catch (error) {
      try {
        _safeStatusInit(_owner.status);
        blob?.cancel(_owner.status);
      } catch (_) {}
      throw mapFbStatusException(
        exception: error,
        util: _owner.util,
        operation: 'write blob',
      );
    } finally {
      if (segmentBuffer != null) {
        fbclient.mem.free(segmentBuffer);
      }
      try {
        blob?.release();
      } catch (_) {}
    }
  }

  void _writeInt128(
    Pointer<Uint8> message,
    _FirebirdFieldDescriptor field,
    Object? value,
  ) {
    final text = _exactNumericText(field, value);
    final i128 = _owner.util.getInt128(_owner.status);
    final nativeValue = fbclient.mem.allocate<fbclient.FbI128>(
      sizeOf<fbclient.FbI128>(),
    );
    try {
      _safeStatusInit(_owner.status);
      i128.fromStr(_owner.status, field.scale, text, nativeValue);
      message.fromNativeMem(
        nativeValue,
        sizeOf<fbclient.FbI128>(),
        0,
        field.offset,
      );
    } on fbclient.FbStatusException catch (error) {
      throw mapFbStatusException(
        exception: error,
        util: _owner.util,
        operation: 'encode INT128',
      );
    } finally {
      fbclient.mem.free(nativeValue);
    }
  }

  void _writeDecFloat16(
    Pointer<Uint8> message,
    _FirebirdFieldDescriptor field,
    Object? value,
  ) {
    final text = _exactNumericText(field, value);
    final decFloat = _owner.util.getDecFloat16(_owner.status);
    final nativeValue = fbclient.mem.allocate<fbclient.FbDec16>(
      sizeOf<fbclient.FbDec16>(),
    );
    try {
      _safeStatusInit(_owner.status);
      decFloat.fromStr(_owner.status, text, nativeValue);
      message.fromNativeMem(
        nativeValue,
        sizeOf<fbclient.FbDec16>(),
        0,
        field.offset,
      );
    } on fbclient.FbStatusException catch (error) {
      throw mapFbStatusException(
        exception: error,
        util: _owner.util,
        operation: 'encode DECFLOAT(16)',
      );
    } finally {
      fbclient.mem.free(nativeValue);
    }
  }

  void _writeDecFloat34(
    Pointer<Uint8> message,
    _FirebirdFieldDescriptor field,
    Object? value,
  ) {
    final text = _exactNumericText(field, value);
    final decFloat = _owner.util.getDecFloat34(_owner.status);
    final nativeValue = fbclient.mem.allocate<fbclient.FbDec34>(
      sizeOf<fbclient.FbDec34>(),
    );
    try {
      _safeStatusInit(_owner.status);
      decFloat.fromStr(_owner.status, text, nativeValue);
      message.fromNativeMem(
        nativeValue,
        sizeOf<fbclient.FbDec34>(),
        0,
        field.offset,
      );
    } on fbclient.FbStatusException catch (error) {
      throw mapFbStatusException(
        exception: error,
        util: _owner.util,
        operation: 'encode DECFLOAT(34)',
      );
    } finally {
      fbclient.mem.free(nativeValue);
    }
  }

  void _writeTimestampWithTimeZone(
    Pointer<Uint8> message,
    _FirebirdFieldDescriptor field,
    Object? value,
  ) {
    final timestamp = _requireTimestampWithTimeZone(field, value);

    if ((field.type & ~1) == fbclient.FbConsts.SQL_TIMESTAMP_TZ_EX) {
      final nativeValue = fbclient.mem.allocate<fbclient.IscTimestampTzEx>(
        sizeOf<fbclient.IscTimestampTzEx>(),
      );
      try {
        _safeStatusInit(_owner.status);
        _owner.util.encodeTimeStampTz(
          _owner.status,
          nativeValue.cast(),
          timestamp.year,
          timestamp.month,
          timestamp.day,
          timestamp.hour,
          timestamp.minute,
          timestamp.second,
          timestamp.millisecond * 10 + timestamp.tenthMillisecond,
          timestamp.timeZoneName.isNotEmpty
              ? timestamp.timeZoneName
              : _formatTimeZoneOffset(timestamp.timeZoneOffset),
        );
        nativeValue.ref.extOffset = timestamp.timeZoneOffset.inMinutes;
        message.fromNativeMem(
          nativeValue,
          sizeOf<fbclient.IscTimestampTzEx>(),
          0,
          field.offset,
        );
      } on fbclient.FbStatusException catch (error) {
        throw mapFbStatusException(
          exception: error,
          util: _owner.util,
          operation: 'encode TIMESTAMP WITH TIME ZONE',
        );
      } finally {
        fbclient.mem.free(nativeValue);
      }
      return;
    }

    final nativeValue = fbclient.mem.allocate<fbclient.IscTimestampTz>(
      sizeOf<fbclient.IscTimestampTz>(),
    );
    try {
      _safeStatusInit(_owner.status);
      _owner.util.encodeTimeStampTz(
        _owner.status,
        nativeValue,
        timestamp.year,
        timestamp.month,
        timestamp.day,
        timestamp.hour,
        timestamp.minute,
        timestamp.second,
        timestamp.millisecond * 10 + timestamp.tenthMillisecond,
        timestamp.timeZoneName.isNotEmpty
            ? timestamp.timeZoneName
            : _formatTimeZoneOffset(timestamp.timeZoneOffset),
      );
      message.fromNativeMem(
        nativeValue,
        sizeOf<fbclient.IscTimestampTz>(),
        0,
        field.offset,
      );
    } on fbclient.FbStatusException catch (error) {
      throw mapFbStatusException(
        exception: error,
        util: _owner.util,
        operation: 'encode TIMESTAMP WITH TIME ZONE',
      );
    } finally {
      fbclient.mem.free(nativeValue);
    }
  }

  void _writeTimeWithTimeZone(
    Pointer<Uint8> message,
    _FirebirdFieldDescriptor field,
    Object? value,
  ) {
    final time = _requireTimeWithTimeZone(field, value);

    if ((field.type & ~1) == fbclient.FbConsts.SQL_TIME_TZ_EX) {
      final nativeValue = fbclient.mem.allocate<fbclient.IscTimeTzEx>(
        sizeOf<fbclient.IscTimeTzEx>(),
      );
      try {
        _safeStatusInit(_owner.status);
        _owner.util.encodeTimeTz(
          _owner.status,
          nativeValue.cast(),
          time.hour,
          time.minute,
          time.second,
          time.millisecond * 10 + time.tenthMillisecond,
          time.timeZoneName.isNotEmpty
              ? time.timeZoneName
              : _formatTimeZoneOffset(time.timeZoneOffset),
        );
        nativeValue.ref.extOffset = time.timeZoneOffset.inMinutes;
        message.fromNativeMem(
          nativeValue,
          sizeOf<fbclient.IscTimeTzEx>(),
          0,
          field.offset,
        );
      } on fbclient.FbStatusException catch (error) {
        throw mapFbStatusException(
          exception: error,
          util: _owner.util,
          operation: 'encode TIME WITH TIME ZONE',
        );
      } finally {
        fbclient.mem.free(nativeValue);
      }
      return;
    }

    final nativeValue = fbclient.mem.allocate<fbclient.IscTimeTz>(
      sizeOf<fbclient.IscTimeTz>(),
    );
    try {
      _safeStatusInit(_owner.status);
      _owner.util.encodeTimeTz(
        _owner.status,
        nativeValue,
        time.hour,
        time.minute,
        time.second,
        time.millisecond * 10 + time.tenthMillisecond,
        time.timeZoneName.isNotEmpty
            ? time.timeZoneName
            : _formatTimeZoneOffset(time.timeZoneOffset),
      );
      message.fromNativeMem(
        nativeValue,
        sizeOf<fbclient.IscTimeTz>(),
        0,
        field.offset,
      );
    } on fbclient.FbStatusException catch (error) {
      throw mapFbStatusException(
        exception: error,
        util: _owner.util,
        operation: 'encode TIME WITH TIME ZONE',
      );
    } finally {
      fbclient.mem.free(nativeValue);
    }
  }

  Object _decodeInt128(Pointer<Uint8> message, _FirebirdFieldDescriptor field) {
    final bytes = message.toDartMem(sizeOf<fbclient.FbI128>(), field.offset);
    final value = _decodeSignedLittleEndian(bytes);
    if (field.scale == 0) {
      return value;
    }
    return FirebirdDecimal(_formatScaledBigInt(value, field.scale));
  }

  FirebirdDecimal _decodeDecFloat16(
    Pointer<Uint8> message,
    _FirebirdFieldDescriptor field,
  ) {
    final decFloat = _owner.util.getDecFloat16(_owner.status);
    final nativeValue = fbclient.mem.allocate<fbclient.FbDec16>(
      sizeOf<fbclient.FbDec16>(),
    );
    try {
      message.toNativeMem(
        nativeValue,
        sizeOf<fbclient.FbDec16>(),
        field.offset,
      );
      _safeStatusInit(_owner.status);
      return FirebirdDecimal(decFloat.toStr(_owner.status, nativeValue).trim());
    } on fbclient.FbStatusException catch (error) {
      throw mapFbStatusException(
        exception: error,
        util: _owner.util,
        operation: 'decode DECFLOAT(16)',
      );
    } finally {
      fbclient.mem.free(nativeValue);
    }
  }

  FirebirdDecimal _decodeDecFloat34(
    Pointer<Uint8> message,
    _FirebirdFieldDescriptor field,
  ) {
    final decFloat = _owner.util.getDecFloat34(_owner.status);
    final nativeValue = fbclient.mem.allocate<fbclient.FbDec34>(
      sizeOf<fbclient.FbDec34>(),
    );
    try {
      message.toNativeMem(
        nativeValue,
        sizeOf<fbclient.FbDec34>(),
        field.offset,
      );
      _safeStatusInit(_owner.status);
      return FirebirdDecimal(decFloat.toStr(_owner.status, nativeValue).trim());
    } on fbclient.FbStatusException catch (error) {
      throw mapFbStatusException(
        exception: error,
        util: _owner.util,
        operation: 'decode DECFLOAT(34)',
      );
    } finally {
      fbclient.mem.free(nativeValue);
    }
  }

  Object _readBlob(Pointer<Uint8> message, _FirebirdFieldDescriptor field) {
    fbclient.IBlob? blob;
    Pointer<Uint8>? buffer;
    Pointer<UnsignedInt>? segmentLength;
    try {
      _safeStatusInit(_owner.status);
      blob = _owner.attachment.openBlob(
        _owner.status,
        _transaction,
        (message + field.offset).cast(),
      );
      const segmentSize = 8192;
      buffer = fbclient.mem.allocate<Uint8>(segmentSize);
      segmentLength = fbclient.mem.allocate<UnsignedInt>(sizeOf<UnsignedInt>());
      final bytes = <int>[];
      while (true) {
        _safeStatusInit(_owner.status);
        final result = blob.getSegment(
          _owner.status,
          segmentSize,
          buffer,
          segmentLength,
        );
        if (result != fbclient.IStatus.resultOK &&
            result != fbclient.IStatus.resultSegment) {
          break;
        }
        bytes.addAll(buffer.toDartMem(segmentLength.value));
      }
      _safeStatusInit(_owner.status);
      blob.close(_owner.status);
      blob = null;

      if (field.subType == fbclient.FbConsts.isc_blob_text) {
        return const Utf8Decoder(allowMalformed: true).convert(bytes);
      }
      return Uint8List.fromList(bytes);
    } on fbclient.FbStatusException catch (error) {
      throw mapFbStatusException(
        exception: error,
        util: _owner.util,
        operation: 'read blob',
      );
    } finally {
      if (buffer != null) {
        fbclient.mem.free(buffer);
      }
      if (segmentLength != null) {
        fbclient.mem.free(segmentLength);
      }
      try {
        blob?.release();
      } catch (_) {}
    }
  }

  FirebirdTimestampWithTimeZone _decodeTimestampWithTimeZone(
    Pointer<Uint8> message,
    _FirebirdFieldDescriptor field,
  ) {
    final parts = fbclient.mem.allocate<UnsignedInt>(sizeOf<UnsignedInt>() * 7);
    final zoneBuffer = fbclient.mem.allocate<Uint8>(128);
    try {
      zoneBuffer.setAllBytes(128, 0);
      _safeStatusInit(_owner.status);
      _owner.util.decodeTimeStampTz(
        _owner.status,
        (message + field.offset).cast(),
        parts,
        (parts + 1).cast(),
        (parts + 2).cast(),
        (parts + 3).cast(),
        (parts + 4).cast(),
        (parts + 5).cast(),
        (parts + 6).cast(),
        128,
        zoneBuffer.cast(),
      );
      return FirebirdTimestampWithTimeZone(
        year: parts[0],
        month: parts[1],
        day: parts[2],
        hour: parts[3],
        minute: parts[4],
        second: parts[5],
        millisecond: parts[6] ~/ 10,
        tenthMillisecond: parts[6] % 10,
        timeZoneName: zoneBuffer.cast<Utf8>().toDartString(),
      );
    } on fbclient.FbStatusException catch (error) {
      throw mapFbStatusException(
        exception: error,
        util: _owner.util,
        operation: 'decode TIMESTAMP WITH TIME ZONE',
      );
    } finally {
      fbclient.mem.free(parts);
      fbclient.mem.free(zoneBuffer);
    }
  }

  FirebirdTimestampWithTimeZone _decodeTimestampWithTimeZoneExtended(
    Pointer<Uint8> message,
    _FirebirdFieldDescriptor field,
  ) {
    final parts = fbclient.mem.allocate<UnsignedInt>(sizeOf<UnsignedInt>() * 7);
    final zoneBuffer = fbclient.mem.allocate<Uint8>(128);
    try {
      zoneBuffer.setAllBytes(128, 0);
      _safeStatusInit(_owner.status);
      _owner.util.decodeTimeStampTzEx(
        _owner.status,
        (message + field.offset).cast(),
        parts,
        (parts + 1).cast(),
        (parts + 2).cast(),
        (parts + 3).cast(),
        (parts + 4).cast(),
        (parts + 5).cast(),
        (parts + 6).cast(),
        128,
        zoneBuffer.cast(),
      );
      final nativeValue = (message + field.offset)
          .cast<fbclient.IscTimestampTzEx>();
      return FirebirdTimestampWithTimeZone(
        year: parts[0],
        month: parts[1],
        day: parts[2],
        hour: parts[3],
        minute: parts[4],
        second: parts[5],
        millisecond: parts[6] ~/ 10,
        tenthMillisecond: parts[6] % 10,
        timeZoneName: zoneBuffer.cast<Utf8>().toDartString(),
        timeZoneOffset: Duration(minutes: nativeValue.ref.extOffset),
      );
    } on fbclient.FbStatusException catch (error) {
      throw mapFbStatusException(
        exception: error,
        util: _owner.util,
        operation: 'decode TIMESTAMP WITH TIME ZONE',
      );
    } finally {
      fbclient.mem.free(parts);
      fbclient.mem.free(zoneBuffer);
    }
  }

  FirebirdTimeWithTimeZone _decodeTimeWithTimeZone(
    Pointer<Uint8> message,
    _FirebirdFieldDescriptor field,
  ) {
    final parts = fbclient.mem.allocate<UnsignedInt>(sizeOf<UnsignedInt>() * 4);
    final zoneBuffer = fbclient.mem.allocate<Uint8>(128);
    try {
      zoneBuffer.setAllBytes(128, 0);
      _safeStatusInit(_owner.status);
      _owner.util.decodeTimeTz(
        _owner.status,
        (message + field.offset).cast(),
        parts,
        (parts + 1).cast(),
        (parts + 2).cast(),
        (parts + 3).cast(),
        128,
        zoneBuffer.cast(),
      );
      return FirebirdTimeWithTimeZone(
        hour: parts[0],
        minute: parts[1],
        second: parts[2],
        millisecond: parts[3] ~/ 10,
        tenthMillisecond: parts[3] % 10,
        timeZoneName: zoneBuffer.cast<Utf8>().toDartString(),
      );
    } on fbclient.FbStatusException catch (error) {
      throw mapFbStatusException(
        exception: error,
        util: _owner.util,
        operation: 'decode TIME WITH TIME ZONE',
      );
    } finally {
      fbclient.mem.free(parts);
      fbclient.mem.free(zoneBuffer);
    }
  }

  FirebirdTimeWithTimeZone _decodeTimeWithTimeZoneExtended(
    Pointer<Uint8> message,
    _FirebirdFieldDescriptor field,
  ) {
    final parts = fbclient.mem.allocate<UnsignedInt>(sizeOf<UnsignedInt>() * 4);
    final zoneBuffer = fbclient.mem.allocate<Uint8>(128);
    try {
      zoneBuffer.setAllBytes(128, 0);
      _safeStatusInit(_owner.status);
      _owner.util.decodeTimeTzEx(
        _owner.status,
        (message + field.offset).cast(),
        parts,
        (parts + 1).cast(),
        (parts + 2).cast(),
        (parts + 3).cast(),
        128,
        zoneBuffer.cast(),
      );
      final nativeValue = (message + field.offset).cast<fbclient.IscTimeTzEx>();
      return FirebirdTimeWithTimeZone(
        hour: parts[0],
        minute: parts[1],
        second: parts[2],
        millisecond: parts[3] ~/ 10,
        tenthMillisecond: parts[3] % 10,
        timeZoneName: zoneBuffer.cast<Utf8>().toDartString(),
        timeZoneOffset: Duration(minutes: nativeValue.ref.extOffset),
      );
    } on fbclient.FbStatusException catch (error) {
      throw mapFbStatusException(
        exception: error,
        util: _owner.util,
        operation: 'decode TIME WITH TIME ZONE',
      );
    } finally {
      fbclient.mem.free(parts);
      fbclient.mem.free(zoneBuffer);
    }
  }

  void _safeErrorRecovery() {
    if (!_retainedTransactionMode) return;
    try {
      _owner.rollbackRetaining();
    } catch (_) {}
  }

  void _ensureOpen() {
    if (_isClosed) {
      throw StateError('The direct fbclient statement is already closed.');
    }
  }
}

class _FirebirdFbClientNativeTransaction implements FirebirdNativeTransaction {
  _FirebirdFbClientNativeTransaction({
    required _FirebirdFbClientNativeConnection owner,
    required fbclient.ITransaction transaction,
  }) : _owner = owner,
       _transaction = transaction;

  final _FirebirdFbClientNativeConnection _owner;
  final fbclient.ITransaction _transaction;

  bool _isClosed = false;
  bool _isReleased = false;

  @override
  Future<void> close() async {
    if (_isClosed) return;
    try {
      await rollback();
    } catch (_) {
      rethrow;
    }
  }

  @override
  Future<void> commit() async {
    _ensureOpen();
    try {
      _safeStatusInit(_owner.status);
      _transaction.commit(_owner.status);
    } on fbclient.FbStatusException catch (error) {
      throw mapFbStatusException(
        exception: error,
        util: _owner.util,
        operation: 'commit transaction',
      );
    } finally {
      _isClosed = true;
      _releaseHandle();
    }
  }

  @override
  Future<FirebirdNativeSavepoint> createSavepoint(String id) async {
    _ensureOpen();
    final normalizedId = _requireSqlIdentifier(id, context: 'savepoint id');
    await _executeControlStatement('SAVEPOINT $normalizedId');
    return _FirebirdFbClientNativeSavepoint(
      transaction: this,
      id: normalizedId,
    );
  }

  @override
  Future<void> rollback() async {
    if (_isClosed) return;
    try {
      _safeStatusInit(_owner.status);
      _transaction.rollback(_owner.status);
    } on fbclient.FbStatusException catch (error) {
      throw mapFbStatusException(
        exception: error,
        util: _owner.util,
        operation: 'rollback transaction',
      );
    } finally {
      _isClosed = true;
      _releaseHandle();
    }
  }

  @override
  Future<FirebirdNativeStatement> prepareStatement(String sql) async {
    _ensureOpen();
    try {
      return await _owner._prepareStatement(
        sql,
        transaction: _transaction,
        retainedTransactionMode: false,
      );
    } on fbclient.FbStatusException catch (error) {
      throw mapFbStatusException(
        exception: error,
        util: _owner.util,
        operation: 'prepare',
      );
    }
  }

  @override
  Future<void> setTransactionContext(String key, Object value) async {
    _ensureOpen();
    final normalizedKey = key.trim();
    if (normalizedKey.isEmpty) {
      throw ArgumentError.value(
        key,
        'key',
        'Transaction context keys must not be empty.',
      );
    }

    await _executeControlStatement(
      '''
      select rdb\$set_context('USER_TRANSACTION', ?, ?) as APPLIED
      from rdb\$database
      ''',
      values: [normalizedKey, value.toString()],
    );
  }

  Future<void> _executeControlStatement(
    String sql, {
    List<Object?> values = const [],
  }) async {
    final statement = await prepareStatement(sql);
    try {
      await statement.execute(values);
    } finally {
      await statement.close();
    }
  }

  void _ensureOpen() {
    if (_isClosed) {
      throw StateError('The direct fbclient transaction is already closed.');
    }
  }

  void _releaseHandle() {
    if (_isReleased) return;
    _isReleased = true;
    try {
      _transaction.release();
    } catch (_) {}
  }
}

class _FirebirdFbClientNativeSavepoint implements FirebirdNativeSavepoint {
  _FirebirdFbClientNativeSavepoint({
    required _FirebirdFbClientNativeTransaction transaction,
    required this.id,
  }) : _transaction = transaction;

  final _FirebirdFbClientNativeTransaction _transaction;

  @override
  final String id;

  bool _isClosed = false;

  @override
  Future<void> release() async {
    if (_isClosed) return;
    await _transaction._executeControlStatement('RELEASE SAVEPOINT $id ONLY');
    _isClosed = true;
  }

  @override
  Future<void> rollback() async {
    if (_isClosed) return;
    await _transaction._executeControlStatement('ROLLBACK TO SAVEPOINT $id');
  }
}

class _FirebirdFieldDescriptor {
  const _FirebirdFieldDescriptor({
    required this.index,
    required this.name,
    required this.type,
    required this.subType,
    required this.scale,
    required this.length,
    required this.charSet,
    required this.offset,
    required this.nullOffset,
    required this.nullable,
  });

  final int index;
  final String name;
  final int type;
  final int subType;
  final int scale;
  final int length;
  final int charSet;
  final int offset;
  final int nullOffset;
  final bool nullable;
}

List<_FirebirdFieldDescriptor> _describeFields(
  fbclient.IStatus status,
  fbclient.IMessageMetadata metadata,
) {
  final count = metadata.getCount(status);
  return List<_FirebirdFieldDescriptor>.generate(count, (index) {
    final alias = metadata.getAlias(status, index);
    final field = metadata.getField(status, index);
    return _FirebirdFieldDescriptor(
      index: index,
      name: alias.isEmpty ? field : alias,
      type: metadata.getType(status, index),
      subType: metadata.getSubType(status, index),
      scale: metadata.getScale(status, index),
      length: metadata.getLength(status, index),
      charSet: metadata.getCharSet(status, index),
      offset: metadata.getOffset(status, index),
      nullOffset: metadata.getNullOffset(status, index),
      nullable: metadata.isNullable(status, index),
    );
  });
}

void _writeFixedText(
  Pointer<Uint8> message,
  _FirebirdFieldDescriptor field,
  Object? value,
) {
  final text = _requireString(field, value);
  final encoded = utf8.encode(text);
  final padCode = field.charSet == 1 ? 0x00 : 0x20;
  final truncated = encoded.length > field.length
      ? encoded.sublist(0, field.length)
      : encoded;
  final padded = truncated.length < field.length
      ? Uint8List.fromList([
          ...truncated,
          ...List<int>.filled(field.length - truncated.length, padCode),
        ])
      : Uint8List.fromList(truncated);
  message.fromDartMem(padded, field.length, 0, field.offset);
}

String _requireString(_FirebirdFieldDescriptor field, Object? value) {
  if (value is String) return value;
  throw ArgumentError(
    'Expected a String for ${field.name}, got ${value.runtimeType}.',
  );
}

num _requireNum(_FirebirdFieldDescriptor field, Object? value) {
  if (value is num) return value;
  throw ArgumentError(
    'Expected a numeric value for ${field.name}, got ${value.runtimeType}.',
  );
}

bool _requireBool(_FirebirdFieldDescriptor field, Object? value) {
  if (value is bool) return value;
  throw ArgumentError(
    'Expected a bool for ${field.name}, got ${value.runtimeType}.',
  );
}

DateTime _requireDateTime(_FirebirdFieldDescriptor field, Object? value) {
  if (value is DateTime) return value;
  throw ArgumentError(
    'Expected a DateTime for ${field.name}, got ${value.runtimeType}.',
  );
}

int _encodeScaledInteger(_FirebirdFieldDescriptor field, Object? value) {
  final numericValue = _requireNum(field, value);
  if (field.scale == 0) {
    return numericValue.toInt();
  }

  final factor = pow(10, -field.scale);
  return (numericValue * factor).round();
}

Object _decodeScaledNumber(int value, int scale) {
  final factor = pow(10, -scale);
  return value / factor;
}

String _trimTrailingSpaces(String value) {
  var end = value.length;
  while (end > 0 && value.codeUnitAt(end - 1) == 0x20) {
    end--;
  }
  return value.substring(0, end);
}

Uint8List _asBlobBytes(_FirebirdFieldDescriptor field, Object? value) {
  if (value is Uint8List) return value;
  if (value is String) return Uint8List.fromList(utf8.encode(value));
  if (value is ByteBuffer) return value.asUint8List();
  if (value is TypedData) {
    return value.buffer.asUint8List(value.offsetInBytes, value.lengthInBytes);
  }
  throw ArgumentError(
    'Expected blob input for ${field.name} as String, ByteBuffer, or TypedData, '
    'got ${value.runtimeType}.',
  );
}

String _exactNumericText(_FirebirdFieldDescriptor field, Object? value) {
  if (value is FirebirdDecimal) return value.text;
  if (value is BigInt) return value.toString();
  if (value is num) return value.toString();
  if (value is String) return value;
  throw ArgumentError(
    'Expected an exact numeric value for ${field.name}, got ${value.runtimeType}.',
  );
}

FirebirdTimestampWithTimeZone _requireTimestampWithTimeZone(
  _FirebirdFieldDescriptor field,
  Object? value,
) {
  if (value is FirebirdTimestampWithTimeZone) return value;
  if (value is DateTime) {
    return FirebirdTimestampWithTimeZone(
      year: value.year,
      month: value.month,
      day: value.day,
      hour: value.hour,
      minute: value.minute,
      second: value.second,
      millisecond: value.millisecond,
      tenthMillisecond: value.microsecond % 1000 ~/ 100,
      timeZoneOffset: value.timeZoneOffset,
    );
  }
  throw ArgumentError(
    'Expected a timestamp-with-time-zone value for ${field.name}, '
    'got ${value.runtimeType}.',
  );
}

FirebirdTimeWithTimeZone _requireTimeWithTimeZone(
  _FirebirdFieldDescriptor field,
  Object? value,
) {
  if (value is FirebirdTimeWithTimeZone) return value;
  if (value is DateTime) {
    return FirebirdTimeWithTimeZone(
      hour: value.hour,
      minute: value.minute,
      second: value.second,
      millisecond: value.millisecond,
      tenthMillisecond: value.microsecond % 1000 ~/ 100,
      timeZoneOffset: value.timeZoneOffset,
    );
  }
  throw ArgumentError(
    'Expected a time-with-time-zone value for ${field.name}, '
    'got ${value.runtimeType}.',
  );
}

String _formatTimeZoneOffset(Duration offset) {
  final sign = offset.isNegative ? '-' : '+';
  final totalMinutes = offset.inMinutes.abs();
  final hours = totalMinutes ~/ 60;
  final minutes = totalMinutes % 60;
  return '$sign${hours.toString().padLeft(2, '0')}:'
      '${minutes.toString().padLeft(2, '0')}';
}

bool _shouldUseOutputRowCountFallback(int statementType) {
  return statementType != fbclient.FbConsts.isc_info_sql_stmt_select &&
      statementType != fbclient.FbConsts.isc_info_sql_stmt_select_for_upd;
}

BigInt _decodeSignedLittleEndian(Uint8List bytes) {
  var unsignedValue = BigInt.zero;
  for (var index = bytes.length - 1; index >= 0; index--) {
    unsignedValue = (unsignedValue << 8) | BigInt.from(bytes[index]);
  }

  final signBitSet = bytes.isNotEmpty && (bytes.last & 0x80) != 0;
  if (!signBitSet) {
    return unsignedValue;
  }

  return unsignedValue - (BigInt.one << (bytes.length * 8));
}

String _formatScaledBigInt(BigInt value, int scale) {
  if (scale == 0) {
    return value.toString();
  }

  final sign = value.isNegative ? '-' : '';
  final absoluteText = value.abs().toString();

  if (scale > 0) {
    final zeros = List<String>.filled(scale, '0').join();
    return '$sign$absoluteText$zeros';
  }

  final fractionDigits = -scale;
  final paddedText = absoluteText.padLeft(fractionDigits + 1, '0');
  final separator = paddedText.length - fractionDigits;
  return '$sign${paddedText.substring(0, separator)}.'
      '${paddedText.substring(separator)}';
}

void _safeStatusInit(fbclient.IStatus status) {
  try {
    status.init();
  } catch (_) {}
}

fbclient.ITransaction _startTransaction({
  required fbclient.IAttachment attachment,
  required fbclient.IStatus status,
  required fbclient.IUtil util,
  required bool readOnly,
  required FirebirdTransactionSettings settings,
}) {
  fbclient.IXpbBuilder? tpb;
  try {
    _safeStatusInit(status);
    tpb = util.getXpbBuilder(status, fbclient.IXpbBuilder.tpb);
    switch (settings.isolationLevel) {
      case FirebirdTransactionIsolationLevel.readUncommitted:
      case FirebirdTransactionIsolationLevel.readCommitted:
        tpb.insertTag(status, fbclient.FbConsts.isc_tpb_read_committed);
        tpb.insertTag(status, fbclient.FbConsts.isc_tpb_read_consistency);
        break;
      case FirebirdTransactionIsolationLevel.repeatableRead:
        tpb.insertTag(status, fbclient.FbConsts.isc_tpb_concurrency);
        break;
      case FirebirdTransactionIsolationLevel.serializable:
        tpb.insertTag(status, fbclient.FbConsts.isc_tpb_consistency);
        break;
    }
    tpb.insertTag(status, fbclient.FbConsts.isc_tpb_wait);
    tpb.insertTag(
      status,
      readOnly
          ? fbclient.FbConsts.isc_tpb_read
          : fbclient.FbConsts.isc_tpb_write,
    );
    return attachment.startTransaction(
      status,
      tpb.getBufferLength(status),
      tpb.getBuffer(status),
    );
  } finally {
    tpb?.dispose();
  }
}

String _requireSqlIdentifier(String value, {required String context}) {
  final normalized = value.trim();
  final pattern = RegExp(r'^[A-Za-z_][A-Za-z0-9_]*$');
  if (!pattern.hasMatch(normalized)) {
    throw ArgumentError.value(
      value,
      context,
      'Only simple SQL identifiers are supported here.',
    );
  }
  return normalized;
}
