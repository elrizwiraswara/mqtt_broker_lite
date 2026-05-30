import 'dart:io';
import 'dart:typed_data';

import 'mqtt_transport.dart';

const List<String> mqttWebSocketSubprotocols = ['mqtt', 'mqttv3.1.1', 'mqttv3.1'];

String? selectMqttSubprotocol(List<String> offered) {
  for (final pref in mqttWebSocketSubprotocols) {
    if (offered.contains(pref)) return pref;
  }

  return null;
}

/// MQTT-over-WebSocket carries packets in binary frames; text frames are a
/// protocol violation and terminate the connection.
class WebSocketTransport implements MqttTransport {
  WebSocketTransport(this._ws, this._remote);

  final WebSocket _ws;
  final String _remote;

  @override
  Stream<Uint8List> get inbound => _ws.map((event) {
        if (event is String) {
          throw const FormatException('MQTT-over-WebSocket requires binary frames; received text');
        }

        return event is Uint8List ? event : Uint8List.fromList(event as List<int>);
      });

  @override
  String get remote => _remote;

  @override
  void send(List<int> bytes) => _ws.add(bytes);

  @override
  Future<void> close() async {
    try {
      await _ws.close();
    } catch (_) {}
  }

  @override
  void destroy() {
    // 1006 (abnormalClosure) is reserved and cannot be sent; goingAway is the
    // closest analog to a transport-level destroy.
    _ws.close(WebSocketStatus.goingAway).ignore();
  }
}
