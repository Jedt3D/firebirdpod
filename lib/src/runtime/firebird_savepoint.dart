import 'firebird_native_client.dart';

class FirebirdSavepoint {
  FirebirdSavepoint.internal({
    required this.id,
    required FirebirdNativeSavepoint nativeSavepoint,
    required Future<void> Function(FirebirdSavepoint savepoint) onRelease,
  }) : _nativeSavepoint = nativeSavepoint,
       _onRelease = onRelease;

  final String id;
  final FirebirdNativeSavepoint _nativeSavepoint;
  final Future<void> Function(FirebirdSavepoint savepoint) _onRelease;

  bool _isReleased = false;

  bool get isReleased => _isReleased;

  Future<void> release() async {
    if (_isReleased) return;
    await _nativeSavepoint.release();
    _isReleased = true;
    await _onRelease(this);
  }

  Future<void> rollback() async {
    if (_isReleased) {
      throw StateError('The Firebird savepoint is already released.');
    }
    await _nativeSavepoint.rollback();
  }
}
