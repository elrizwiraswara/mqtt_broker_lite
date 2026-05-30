import 'dart:typed_data';

import '../codec/packet_type.dart';

class MqttAuthResult {
  const MqttAuthResult.accept() : returnCode = MqttConnectReturnCode.accepted;
  const MqttAuthResult.reject(this.returnCode);

  final MqttConnectReturnCode returnCode;

  bool get accepted => returnCode == MqttConnectReturnCode.accepted;
}

abstract class MqttAuthenticator {
  Future<MqttAuthResult> authenticate({
    required String clientId,
    String? username,
    Uint8List? password,
  });
}

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
