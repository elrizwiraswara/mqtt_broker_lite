import 'dart:typed_data';

import 'package:dart_mqtt_broker/src/codec/packet_reader.dart';
import 'package:dart_mqtt_broker/src/codec/packet_type.dart';
import 'package:dart_mqtt_broker/src/codec/packets.dart';
import 'package:test/test.dart';

void main() {
  group('PacketReader', () {
    test('reassembles a fragmented packet', () {
      final pkt = PublishPacket(
        topic: 'a/b',
        payload: Uint8List.fromList(List<int>.generate(200, (i) => i & 0xFF)),
        qos: 0,
      ).encode();

      final reader = PacketReader();
      // Feed byte-by-byte.
      for (final b in pkt) {
        reader.addBytes([b]);
        // Most feeds yield nothing; only the last byte completes the packet.
      }
      final raws = reader.readPackets();
      expect(raws, hasLength(1));
      final decoded = PublishPacket.decode(raws.single.firstByte, raws.single.body);
      expect(decoded.topic, 'a/b');
      expect(decoded.payload.length, 200);
    });

    test('drains multiple packets from a single chunk', () {
      final builder = BytesBuilder();
      builder.add(PingReqPacket().encode());
      builder.add(PingReqPacket().encode());
      builder.add(DisconnectPacket().encode());

      final reader = PacketReader();
      reader.addBytes(builder.takeBytes());
      final raws = reader.readPackets();
      expect(raws.map((r) => r.type).toList(), [
        MqttPacketType.pingreq,
        MqttPacketType.pingreq,
        MqttPacketType.disconnect,
      ]);
    });

    test('handles two packets split across a chunk boundary', () {
      final first = PingReqPacket().encode();
      final second = DisconnectPacket().encode();
      final reader = PacketReader();

      // First chunk: first packet + 1 byte of second.
      reader.addBytes([...first, second[0]]);
      var raws = reader.readPackets();
      expect(raws, hasLength(1));

      // Second chunk: remaining byte of second.
      reader.addBytes([second[1]]);
      raws = reader.readPackets();
      expect(raws, hasLength(1));
    });

    test('rejects oversize buffer', () {
      final reader = PacketReader(maxBufferedBytes: 8);
      expect(() => reader.addBytes(List.filled(16, 0)), throwsStateError);
    });
  });
}
