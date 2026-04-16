import 'firebird_connection.dart';
import 'firebird_connection_options.dart';
import 'firebird_native_client.dart';

class FirebirdEndpoint {
  const FirebirdEndpoint({
    required FirebirdNativeClient client,
    required this.options,
  }) : _client = client;

  final FirebirdNativeClient _client;
  final FirebirdConnectionOptions options;

  Future<FirebirdConnection> connect() async {
    final nativeConnection = await _client.attach(options);
    if (options.statementTimeout case final timeout?) {
      await nativeConnection.setStatementTimeout(timeout);
    }
    return FirebirdConnection.internal(
      nativeConnection,
      defaultStatementTimeout: options.statementTimeout,
    );
  }
}
