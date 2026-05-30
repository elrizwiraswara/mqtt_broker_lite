import 'dart:typed_data';

import '../codec/packets.dart';
import '../transport/connection.dart';

class WillMessage {
  WillMessage({
    required this.topic,
    required this.payload,
    required this.qos,
    required this.retain,
  });

  final String topic;
  final Uint8List payload;
  final int qos;
  final bool retain;
}

/// Per-client state. When a client disconnects with cleanSession=false the
/// session stays in the store with [connection]=null and pending QoS 1/2
/// messages are replayed on reconnect.
class MqttSession {
  MqttSession({required this.clientId, required this.cleanSession});

  final String clientId;
  bool cleanSession;

  ClientConnection? connection;
  WillMessage? will;

  int _nextPacketId = 0;

  /// QoS 1 PUBLISH awaiting PUBACK, and QoS 2 PUBLISH awaiting PUBREC.
  final Map<int, PublishPacket> inflightPublish = {};

  /// QoS 2 packetIds for which PUBREL is sent and PUBCOMP is awaited.
  final Set<int> inflightPubrel = {};

  /// QoS 2 inbound: PUBLISH received, PUBREC sent, awaiting PUBREL.
  final Map<int, PublishPacket> inboundQos2 = {};

  /// Messages queued while the session is offline (cleanSession=false only).
  final List<PublishPacket> pendingPublishes = [];

  bool get isConnected => connection != null;

  int allocatePacketId() {
    for (var i = 0; i < 65536; i++) {
      _nextPacketId = (_nextPacketId % 65535) + 1;

      if (!_packetIdInUse(_nextPacketId)) return _nextPacketId;
    }

    throw StateError('Exhausted outbound packet identifiers');
  }

  bool _packetIdInUse(int id) {
    if (inflightPublish.containsKey(id)) return true;
    if (inflightPubrel.contains(id)) return true;

    for (final p in pendingPublishes) {
      if (p.packetId == id) return true;
    }

    return false;
  }

  /// Discards state that should not survive a take-over (will, in-flight
  /// inbound QoS 2).
  void resetTransient() {
    will = null;
    inboundQos2.clear();
  }

  void clear() {
    will = null;
    inflightPublish.clear();
    inflightPubrel.clear();
    inboundQos2.clear();
    pendingPublishes.clear();
  }
}
