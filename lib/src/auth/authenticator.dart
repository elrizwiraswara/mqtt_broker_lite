import 'dart:typed_data';

import '../codec/packet_type.dart';

class MqttAuthResult {
  const MqttAuthResult.accept() : returnCode = MqttConnectReturnCode.accepted;
  const MqttAuthResult.reject(this.returnCode);
  final MqttConnectReturnCode returnCode;
  bool get accepted => returnCode == MqttConnectReturnCode.accepted;
}

/// Pluggable authentication hook. Return [MqttAuthResult.accept] to allow the
/// CONNECT, or one of the reject codes (e.g. [MqttConnectReturnCode.badUsernameOrPassword]).
abstract class MqttAuthenticator {
  Future<MqttAuthResult> authenticate({
    required String clientId,
    String? username,
    Uint8List? password,
  });
}

/// Default authenticator: accepts everyone.
class AllowAllAuthenticator extends MqttAuthenticator {
  @override
  Future<MqttAuthResult> authenticate({
    required String clientId,
    String? username,
    Uint8List? password,
  }) async {
    return const MqttAuthResult.accept();
  }
}
