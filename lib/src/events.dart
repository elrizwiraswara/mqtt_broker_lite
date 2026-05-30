import 'dart:typed_data';

import 'session/session.dart';

class MqttConnectEvent {
  MqttConnectEvent(this.session);
  final MqttSession session;
  String get clientId => session.clientId;
}

class MqttDisconnectEvent {
  MqttDisconnectEvent(this.session, {required this.graceful});
  final MqttSession session;
  final bool graceful;
  String get clientId => session.clientId;
}

class MqttSubscribeEvent {
  MqttSubscribeEvent({
    required this.session,
    required this.filter,
    required this.grantedQos,
  });
  final MqttSession session;
  final String filter;
  final int grantedQos;
  String get clientId => session.clientId;
}

class MqttUnsubscribeEvent {
  MqttUnsubscribeEvent({required this.session, required this.filter});
  final MqttSession session;
  final String filter;
  String get clientId => session.clientId;
}

class MqttPublishEvent {
  MqttPublishEvent({
    required this.topic,
    required this.payload,
    required this.qos,
    required this.retain,
    this.session,
  });

  /// The session that published the message. `null` when the broker itself
  /// published via `MqttBroker.publish`.
  final MqttSession? session;
  final String topic;
  final Uint8List payload;
  final int qos;
  final bool retain;
  String? get clientId => session?.clientId;
}
