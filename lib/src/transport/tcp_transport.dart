import 'dart:io';
import 'dart:typed_data';

import 'mqtt_transport.dart';

class TcpTransport implements MqttTransport {
  TcpTransport(this._socket);

  final Socket _socket;

  @override
  Stream<Uint8List> get inbound => _socket;

  @override
  String get remote => '${_socket.remoteAddress.address}:${_socket.remotePort}';

  @override
  void send(List<int> bytes) => _socket.add(bytes);

  @override
  Future<void> close() async {
    try {
      await _socket.close();
    } catch (_) {}
  }

  @override
  void destroy() {
    try {
      _socket.destroy();
    } catch (_) {}
  }
}
