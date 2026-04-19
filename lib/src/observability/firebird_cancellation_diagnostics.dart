import 'package:meta/meta.dart';

import '../runtime/firebird_cancel_mode.dart';
import '../runtime/firebird_connection.dart';
import '../runtime/firebird_database_exception.dart';

/// Attachment-cancellation capability inspection for the current adapter.
class FirebirdCancellationDiagnostics {
  const FirebirdCancellationDiagnostics(this._connection);

  final FirebirdConnection _connection;

  Future<FirebirdCancellationConnectionState> captureConnectionState() async {
    final configuredTimeout = await _connection.getStatementTimeout();
    return FirebirdCancellationConnectionState(
      attachmentId: await _readCurrentAttachmentId(),
      configuredTimeout: configuredTimeout,
    );
  }

  Future<FirebirdObservedCancelRequest> observeRequest({
    FirebirdCancelMode mode = FirebirdCancelMode.raise,
    bool probeConnectionAfterObservation = true,
  }) async {
    final connectionState = await captureConnectionState();

    final requestedAt = DateTime.now();
    final stopwatch = Stopwatch()..start();
    FirebirdDatabaseException? requestError;

    try {
      await _connection.cancelCurrentOperation(mode: mode);
    } on FirebirdDatabaseException catch (exception) {
      requestError = exception;
    } finally {
      stopwatch.stop();
    }

    int? probedAttachmentId;
    FirebirdDatabaseException? connectionProbeError;
    bool? connectionUsableAfterObservation;

    if (probeConnectionAfterObservation) {
      try {
        probedAttachmentId = await _readCurrentAttachmentId();
        connectionUsableAfterObservation = true;
      } on FirebirdDatabaseException catch (exception) {
        connectionUsableAfterObservation = false;
        connectionProbeError = exception;
      }
    }

    return FirebirdObservedCancelRequest(
      attachmentId: connectionState.attachmentId,
      connectionTimeout: connectionState.configuredTimeout,
      requestedMode: mode,
      requestedAt: requestedAt,
      finishedAt: requestedAt.add(stopwatch.elapsed),
      elapsed: stopwatch.elapsed,
      requestError: requestError,
      connectionUsableAfterObservation: connectionUsableAfterObservation,
      probedAttachmentId: probedAttachmentId,
      connectionProbeError: connectionProbeError,
    );
  }

  Future<int> _readCurrentAttachmentId() async {
    final result = await _connection.execute(
      'select current_connection as ATTACHMENT_ID from rdb\$database',
    );
    final row = result.singleRow;
    if (row == null) {
      throw StateError(
        'Firebird returned no row for the current attachment snapshot.',
      );
    }

    return _requireInt(row['ATTACHMENT_ID'], fieldName: 'ATTACHMENT_ID');
  }
}

extension FirebirdConnectionCancellationObserver on FirebirdConnection {
  FirebirdCancellationDiagnostics get cancellationDiagnostics =>
      FirebirdCancellationDiagnostics(this);
}

@immutable
class FirebirdCancellationConnectionState {
  const FirebirdCancellationConnectionState({
    required this.attachmentId,
    required this.configuredTimeout,
  });

  final int attachmentId;
  final Duration? configuredTimeout;
}

@immutable
class FirebirdObservedCancelRequest {
  const FirebirdObservedCancelRequest({
    required this.attachmentId,
    required this.connectionTimeout,
    required this.requestedMode,
    required this.requestedAt,
    required this.finishedAt,
    required this.elapsed,
    this.requestError,
    this.connectionUsableAfterObservation,
    this.probedAttachmentId,
    this.connectionProbeError,
  });

  final int attachmentId;
  final Duration? connectionTimeout;
  final FirebirdCancelMode requestedMode;
  final DateTime requestedAt;
  final DateTime finishedAt;
  final Duration elapsed;
  final FirebirdDatabaseException? requestError;
  final bool? connectionUsableAfterObservation;
  final int? probedAttachmentId;
  final FirebirdDatabaseException? connectionProbeError;

  bool get requestAccepted => requestError == null;
  bool get cancelled => requestError?.isCancelled ?? false;
  bool get timedOut => requestError?.isTimeout ?? false;
  bool get reportedNothingToCancel =>
      requestError?.hasErrorCode(_iscNothingToCancel) ?? false;
  bool get likelyForcedAbort =>
      requestedMode == FirebirdCancelMode.abort &&
      connectionUsableAfterObservation == false;
}

const _iscNothingToCancel = 335544933;

int _requireInt(Object? value, {required String fieldName}) {
  switch (value) {
    case int intValue:
      return intValue;
    case BigInt bigIntValue:
      return bigIntValue.toInt();
    case num numericValue:
      return numericValue.toInt();
    case String textValue:
      final parsed = int.tryParse(textValue.trim());
      if (parsed != null) return parsed;
    default:
      break;
  }

  throw StateError('Expected $fieldName to be an integer, got $value.');
}
