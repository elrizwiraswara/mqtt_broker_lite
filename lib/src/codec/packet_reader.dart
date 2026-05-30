import 'dart:typed_data';

import 'packet_type.dart';
import 'packets.dart';
import 'variable_byte_int.dart';

class RawPacket {
  const RawPacket(this.firstByte, this.body);
  final int firstByte;
  final Uint8List body;
  MqttPacketType get type => MqttPacketType.fromFirstByte(firstByte);
}

/// Streaming MQTT packet reassembler. Feed bytes via [addBytes] and call
/// [readPackets] to drain any complete packets. Handles fragmentation and
/// multiple packets per TCP read.
class PacketReader {
  final List<int> _buffer = <int>[];

  /// Maximum bytes allowed in the buffer before throwing. Defaults to ~10 MB.
  final int maxBufferedBytes;

  PacketReader({this.maxBufferedBytes = 10 * 1024 * 1024});

  void addBytes(List<int> bytes) {
    _buffer.addAll(bytes);
    if (_buffer.length > maxBufferedBytes) {
      throw StateError('MQTT input buffer exceeded $maxBufferedBytes bytes');
    }
  }

  /// Drains and returns all complete packets currently in the buffer.
  List<RawPacket> readPackets() {
    final out = <RawPacket>[];
    while (true) {
      if (_buffer.length < 2) break;

      final VariableByteIntResult? lenResult;
      try {
        lenResult = VariableByteInt.decode(_buffer, 1);
      } on FormatException {
        rethrow;
      }
      if (lenResult == null) break; // need more length bytes

      final headerLen = 1 + lenResult.bytesConsumed;
      final totalLen = headerLen + lenResult.value;
      if (_buffer.length < totalLen) break; // incomplete packet

      final firstByte = _buffer[0];
      final body = Uint8List.fromList(_buffer.sublist(headerLen, totalLen));
      _buffer.removeRange(0, totalLen);
      out.add(RawPacket(firstByte, body));
    }
    return out;
  }

  void clear() => _buffer.clear();
  int get bufferedBytes => _buffer.length;
}

/// Decodes a [RawPacket] into a typed [MqttPacket] for packet types the
/// broker is expected to receive from clients.
MqttPacket decodeInboundPacket(RawPacket raw) {
  switch (raw.type) {
    case MqttPacketType.connect:
      return ConnectPacket.decode(raw.firstByte, raw.body);
    case MqttPacketType.publish:
      return PublishPacket.decode(raw.firstByte, raw.body);
    case MqttPacketType.puback:
      return PubAckPacket.decode(raw.firstByte, raw.body);
    case MqttPacketType.pubrec:
      return PubRecPacket.decode(raw.firstByte, raw.body);
    case MqttPacketType.pubrel:
      return PubRelPacket.decode(raw.firstByte, raw.body);
    case MqttPacketType.pubcomp:
      return PubCompPacket.decode(raw.firstByte, raw.body);
    case MqttPacketType.subscribe:
      return SubscribePacket.decode(raw.firstByte, raw.body);
    case MqttPacketType.unsubscribe:
      return UnsubscribePacket.decode(raw.firstByte, raw.body);
    case MqttPacketType.pingreq:
      return PingReqPacket();
    case MqttPacketType.disconnect:
      return DisconnectPacket();
    default:
      throw FormatException('Unexpected inbound packet type: ${raw.type}');
  }
}
