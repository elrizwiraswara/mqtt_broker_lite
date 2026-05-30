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

/// Per-client state. Lives in the [SessionStore]. When a client disconnects
/// and cleanSession was false, the session stays in the store with
/// [connection] set to null; on reconnect, [connection] is reattached and
/// [pendingPublishes] are replayed.
class MqttSession {
  MqttSession({required this.clientId, required this.cleanSession});

  final String clientId;
  bool cleanSession;

  ClientConnection? connection;
  WillMessage? will;

  int _nextPacketId = 0;

  /// QoS 1 PUBLISH awaiting PUBACK, and QoS 2 PUBLISH awaiting PUBREC.
  final Map<int, PublishPacket> inflightPublish = {};

  /// QoS 2: packetIds for which we have sent PUBREL and await PUBCOMP.
  final Set<int> inflightPubrel = {};

  /// QoS 2 inbound: packetIds for which we received PUBLISH and sent PUBREC,
  /// awaiting PUBREL. Payload stored so we can deliver on PUBREL.
  final Map<int, PublishPacket> inboundQos2 = {};

  /// Messages queued while the session is offline (cleanSession=false only).
  final List<PublishPacket> pendingPublishes = [];

  bool get isConnected => connection != null;

  /// Returns the next outbound packet identifier in the 1..65535 cycle,
  /// skipping any that are currently in-flight or queued for offline delivery.
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

  /// Discards transient state used by a previous connection that we should
  /// not carry across a take-over (will message, ongoing QoS 2 inbound).
  void resetTransient() {
    will = null;
    inboundQos2.clear();
  }

  /// Wipes all state — called for clean-session disconnect or store removal.
  void clear() {
    will = null;
    inflightPublish.clear();
    inflightPubrel.clear();
    inboundQos2.clear();
    pendingPublishes.clear();
  }
}
