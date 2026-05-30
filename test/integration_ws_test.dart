@Timeout(Duration(seconds: 30))
library;

import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'package:mqtt_broker_lite/mqtt_broker_lite.dart';
import 'package:mqtt_broker_lite/src/codec/packet_reader.dart';
import 'package:mqtt_broker_lite/src/codec/packets.dart';
import 'package:test/test.dart';

Future<int> _freePort() async {
  final s = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
  final port = s.port;
  await s.close();

  return port;
}

int _uint16(Uint8List b, int o) => (b[o] << 8) | b[o + 1];

/// Decoder for packet types the broker sends to clients.
MqttPacket decodeInboundReply(RawPacket raw) {
  switch (raw.firstByte & 0xF0) {
    case 0x20:
      return _ConnAckDecoded(raw.body);
    case 0x30:
      return PublishPacket.decode(raw.firstByte, raw.body);
    case 0x40:
      return PubAckPacket.decode(raw.firstByte, raw.body);
    case 0x50:
      return PubRecPacket.decode(raw.firstByte, raw.body);
    case 0x60:
      return PubRelPacket.decode(raw.firstByte, raw.body);
    case 0x70:
      return PubCompPacket.decode(raw.firstByte, raw.body);
    case 0x90:
      return _SubAckDecoded(raw.body);
    case 0xB0:
      return UnsubAckPacket(_uint16(raw.body, 0));
    case 0xD0:
      return PingRespPacket();
    default:
      throw FormatException(
        'Unexpected reply packet type: 0x${raw.firstByte.toRadixString(16)}',
      );
  }
}

class _ConnAckDecoded extends MqttPacket {
  _ConnAckDecoded(Uint8List body)
      : sessionPresent = (body[0] & 0x01) != 0,
        returnCode = body[1];

  final bool sessionPresent;
  final int returnCode;

  @override
  Uint8List encode() => throw UnimplementedError();

  @override
  get type => throw UnimplementedError();
}

class _SubAckDecoded extends MqttPacket {
  _SubAckDecoded(Uint8List body)
      : packetId = (body[0] << 8) | body[1],
        returnCodes = List<int>.unmodifiable(body.sublist(2));

  final int packetId;
  final List<int> returnCodes;

  @override
  Uint8List encode() => throw UnimplementedError();

  @override
  get type => throw UnimplementedError();
}

/// Minimal WebSocket-backed MQTT client built on the broker's own codec.
class _WsMqttClient {
  _WsMqttClient(this.ws) {
    _packetReader = PacketReader();

    ws.listen(
      (event) {
        if (event is! List<int>) return;

        _packetReader.addBytes(event);

        for (final raw in _packetReader.readPackets()) {
          packets.add(decodeInboundReply(raw));
        }
      },
      onDone: () {
        if (!_done.isCompleted) _done.complete();
      },
    );
  }

  static Future<_WsMqttClient> connect(int port, {String path = '/mqtt'}) async {
    final ws = await WebSocket.connect(
      'ws://127.0.0.1:$port$path',
      protocols: ['mqtt'],
    );

    return _WsMqttClient(ws);
  }

  final WebSocket ws;
  late final PacketReader _packetReader;
  final StreamController<MqttPacket> packets = StreamController.broadcast();
  final Completer<void> _done = Completer<void>();

  Future<void> get done => _done.future;

  void send(MqttPacket p) => ws.add(p.encode());

  Future<T> waitFor<T extends MqttPacket>() {
    return packets.stream.firstWhere((p) => p is T).then((p) => p as T);
  }

  Future<void> close() async {
    await ws.close();
  }
}

void main() {
  late MqttBroker broker;
  late int port;

  setUp(() async {
    port = await _freePort();
    broker = MqttBroker.ws(
      address: '127.0.0.1',
      port: port,
      logger: SilentMqttLogger(),
    );
    await broker.start();
  });

  tearDown(() async {
    await broker.stop();
  });

  test('client connects over WebSocket and receives CONNACK', () async {
    final c = await _WsMqttClient.connect(port);
    final waitConnAck = c.waitFor<_ConnAckDecoded>();

    c.send(ConnectPacket(
      protocolName: 'MQTT',
      protocolLevel: 4,
      clientId: 'ws-1',
      cleanSession: true,
      keepAlive: 0,
    ));

    final connAck = await waitConnAck.timeout(const Duration(seconds: 5));
    expect(connAck.returnCode, 0);
    expect(connAck.sessionPresent, isFalse);

    await c.close();
  });

  test('subscribe + publish round-trip over WebSocket', () async {
    final sub = await _WsMqttClient.connect(port);
    final pub = await _WsMqttClient.connect(port);

    for (final c in [sub, pub]) {
      final waitAck = c.waitFor<_ConnAckDecoded>();

      c.send(ConnectPacket(
        protocolName: 'MQTT',
        protocolLevel: 4,
        clientId: c == sub ? 'ws-sub' : 'ws-pub',
        cleanSession: true,
        keepAlive: 0,
      ));

      await waitAck.timeout(const Duration(seconds: 5));
    }

    final waitSubAck = sub.waitFor<_SubAckDecoded>();

    sub.send(SubscribePacket(
      packetId: 1,
      topics: [const SubscribeTopic('hello/ws', 0)],
    ));

    final subAck = await waitSubAck.timeout(const Duration(seconds: 5));
    expect(subAck.returnCodes, [0]);

    final waitPub = sub.waitFor<PublishPacket>();

    pub.send(PublishPacket(
      topic: 'hello/ws',
      payload: Uint8List.fromList('world'.codeUnits),
      qos: 0,
    ));

    final received = await waitPub.timeout(const Duration(seconds: 5));
    expect(String.fromCharCodes(received.payload), 'world');

    await sub.close();
    await pub.close();
  });

  test('TCP and WS brokers can run side by side on different ports', () async {
    final tcpPort = await _freePort();
    final tcpBroker = MqttBroker(
      address: '127.0.0.1',
      port: tcpPort,
      logger: SilentMqttLogger(),
    );

    await tcpBroker.start();

    try {
      final wsClient = await _WsMqttClient.connect(port);
      final wsConnAck = wsClient.waitFor<_ConnAckDecoded>();

      wsClient.send(ConnectPacket(
        protocolName: 'MQTT',
        protocolLevel: 4,
        clientId: 'coexist-ws',
        cleanSession: true,
        keepAlive: 0,
      ));

      expect((await wsConnAck.timeout(const Duration(seconds: 5))).returnCode, 0);

      await wsClient.close();
    } finally {
      await tcpBroker.stop();
    }
  });

  test('non-WebSocket request gets HTTP 400', () async {
    final res = await HttpClient().get('127.0.0.1', port, '/mqtt').then((req) => req.close());

    expect(res.statusCode, HttpStatus.badRequest);

    await res.drain<void>();
  });

  test('upgrade without MQTT subprotocol is rejected', () async {
    await expectLater(
      WebSocket.connect('ws://127.0.0.1:$port/mqtt', protocols: ['not-mqtt']),
      throwsA(isA<WebSocketException>()),
    );
  });

  test('request to wrong path gets HTTP 404', () async {
    final res = await HttpClient().get('127.0.0.1', port, '/other').then((req) => req.close());

    expect(res.statusCode, HttpStatus.notFound);

    await res.drain<void>();
  });
}
