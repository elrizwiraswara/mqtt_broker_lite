import 'dart:typed_data';

import 'package:mqtt_broker_lite/src/codec/packet_reader.dart';
import 'package:mqtt_broker_lite/src/codec/packet_type.dart';
import 'package:mqtt_broker_lite/src/codec/packets.dart';
import 'package:test/test.dart';

/// Encodes a packet, runs it through a [PacketReader], and returns the
/// resulting [RawPacket].
RawPacket _toRaw(Uint8List encoded) {
  final reader = PacketReader();
  reader.addBytes(encoded);

  final raws = reader.readPackets();
  expect(raws, hasLength(1));

  return raws.single;
}

void main() {
  group('CONNECT', () {
    test('round-trips MQTT 3.1.1 with all options', () {
      final pkt = ConnectPacket(
        protocolName: 'MQTT',
        protocolLevel: 4,
        clientId: 'client-42',
        cleanSession: true,
        keepAlive: 60,
        username: 'alice',
        password: Uint8List.fromList([0xDE, 0xAD]),
        willTopic: 'farewell',
        willPayload: Uint8List.fromList([0xBE, 0xEF]),
        willQos: 1,
        willRetain: true,
      );

      final raw = _toRaw(pkt.encode());
      final decoded = ConnectPacket.decode(raw.firstByte, raw.body);

      expect(decoded.protocolName, 'MQTT');
      expect(decoded.protocolLevel, 4);
      expect(decoded.clientId, 'client-42');
      expect(decoded.cleanSession, isTrue);
      expect(decoded.keepAlive, 60);
      expect(decoded.username, 'alice');
      expect(decoded.password, [0xDE, 0xAD]);
      expect(decoded.willTopic, 'farewell');
      expect(decoded.willPayload, [0xBE, 0xEF]);
      expect(decoded.willQos, 1);
      expect(decoded.willRetain, isTrue);
    });
  });

  group('PUBLISH', () {
    test('round-trips QoS 0 with retain', () {
      final pkt = PublishPacket(
        topic: 'a/b',
        payload: Uint8List.fromList([1, 2, 3]),
        qos: 0,
        retain: true,
      );

      final raw = _toRaw(pkt.encode());
      expect(raw.firstByte & 0xF0, 0x30);
      expect(raw.firstByte & 0x01, 0x01);

      final decoded = PublishPacket.decode(raw.firstByte, raw.body);
      expect(decoded.topic, 'a/b');
      expect(decoded.qos, 0);
      expect(decoded.retain, isTrue);
      expect(decoded.payload, [1, 2, 3]);
    });

    test('round-trips QoS 2 with DUP', () {
      final pkt = PublishPacket(
        topic: 'x/y',
        payload: Uint8List.fromList([0xAA]),
        qos: 2,
        dup: true,
        packetId: 7,
      );

      final raw = _toRaw(pkt.encode());
      final decoded = PublishPacket.decode(raw.firstByte, raw.body);

      expect(decoded.qos, 2);
      expect(decoded.dup, isTrue);
      expect(decoded.packetId, 7);
    });
  });

  group('Ack-only packets', () {
    test('PUBACK / PUBREC / PUBREL / PUBCOMP', () {
      for (final cons in <MqttPacket Function(int)>[
        PubAckPacket.new,
        PubRecPacket.new,
        PubRelPacket.new,
        PubCompPacket.new,
      ]) {
        final pkt = cons(0x1234);
        final raw = _toRaw(pkt.encode());

        switch (pkt) {
          case PubAckPacket _:
            expect(PubAckPacket.decode(raw.firstByte, raw.body).packetId, 0x1234);
          case PubRecPacket _:
            expect(PubRecPacket.decode(raw.firstByte, raw.body).packetId, 0x1234);
          case PubRelPacket _:
            expect(PubRelPacket.decode(raw.firstByte, raw.body).packetId, 0x1234);
          case PubCompPacket _:
            expect(PubCompPacket.decode(raw.firstByte, raw.body).packetId, 0x1234);
        }
      }
    });

    test('PUBREL flags must be 0010', () {
      expect(
        () => PubRelPacket.decode(0x60, Uint8List.fromList([0x00, 0x01])),
        throwsA(isA<FormatException>()),
      );
    });
  });

  group('SUBSCRIBE / SUBACK / UNSUBSCRIBE / UNSUBACK', () {
    test('SUBSCRIBE round-trip', () {
      final pkt = SubscribePacket(
        packetId: 9,
        topics: [
          const SubscribeTopic('a/b', 0),
          const SubscribeTopic('c/#', 2),
        ],
      );

      final raw = _toRaw(pkt.encode());
      final decoded = SubscribePacket.decode(raw.firstByte, raw.body);

      expect(decoded.packetId, 9);
      expect(decoded.topics.map((t) => t.filter), ['a/b', 'c/#']);
      expect(decoded.topics.map((t) => t.qos), [0, 2]);
    });

    test('SUBACK encodes return codes', () {
      final pkt = SubAckPacket(packetId: 9, returnCodes: [0, 1, 2, subAckFailure]);
      final raw = _toRaw(pkt.encode());

      expect(raw.firstByte, 0x90);
      expect(raw.body, [0x00, 0x09, 0, 1, 2, subAckFailure]);
    });

    test('UNSUBSCRIBE round-trip', () {
      final pkt = UnsubscribePacket(packetId: 5, topics: ['a/b', 'c/#']);
      final raw = _toRaw(pkt.encode());
      final decoded = UnsubscribePacket.decode(raw.firstByte, raw.body);

      expect(decoded.packetId, 5);
      expect(decoded.topics, ['a/b', 'c/#']);
    });
  });

  test('PINGREQ / PINGRESP / DISCONNECT zero-byte bodies', () {
    expect(PingReqPacket().encode(), [0xC0, 0x00]);
    expect(PingRespPacket().encode(), [0xD0, 0x00]);
    expect(DisconnectPacket().encode(), [0xE0, 0x00]);
  });

  group('packet-type detection', () {
    test('masks the high nibble', () {
      for (final firstByte in [0x30, 0x31, 0x32, 0x33, 0x38, 0x39, 0x3A, 0x3B]) {
        expect(MqttPacketType.fromFirstByte(firstByte), MqttPacketType.publish);
      }

      expect(MqttPacketType.fromFirstByte(0xA2), MqttPacketType.unsubscribe);
      expect(MqttPacketType.fromFirstByte(0x82), MqttPacketType.subscribe);
    });
  });
}
