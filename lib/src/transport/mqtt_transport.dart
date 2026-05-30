import 'dart:typed_data';

/// Transport-agnostic byte pipe between the broker and one client.
abstract class MqttTransport {
  Stream<Uint8List> get inbound;

  void send(List<int> bytes);

  Future<void> close();

  void destroy();

  String get remote;
}
