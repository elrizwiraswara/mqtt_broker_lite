@Timeout(Duration(seconds: 30))
library;

import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'package:mqtt_broker_lite/mqtt_broker_lite.dart';
import 'package:mqtt_broker_lite/src/codec/packets.dart';
import 'package:mqtt_client/mqtt_client.dart';
import 'package:mqtt_client/mqtt_server_client.dart';
import 'package:test/test.dart';

Future<int> _freePort() async {
  final s = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
  final port = s.port;
  await s.close();

  return port;
}

Future<MqttServerClient> _connect(
  int port,
  String clientId, {
  String? willTopic,
  String? willMessage,
  MqttQos willQos = MqttQos.atMostOnce,
  bool willRetain = false,
}) async {
  final c = MqttServerClient.withPort('127.0.0.1', clientId, port)
    ..logging(on: false)
    ..keepAlivePeriod = 5
    ..autoReconnect = false;

  var conn = MqttConnectMessage().withClientIdentifier(clientId).startClean().withWillQos(willQos);

  if (willTopic != null) {
    conn = conn.withWillTopic(willTopic).withWillMessage(willMessage ?? '');
    if (willRetain) conn = conn.withWillRetain();
  }

  c.connectionMessage = conn;
  await c.connect();

  return c;
}

/// Waits for the next publish message on [client], settling outside any
/// stream listener so callers can safely call `disconnect` afterwards without
/// tripping mqtt_client's re-entrant controller guard.
Future<MqttPublishMessage> _waitNext(
  MqttServerClient client, {
  Duration timeout = const Duration(seconds: 5),
}) async {
  final completer = Completer<MqttPublishMessage>();
  late StreamSubscription sub;

  sub = client.updates!.listen((list) {
    if (completer.isCompleted || list.isEmpty) return;
    completer.complete(list.first.payload as MqttPublishMessage);
  });

  try {
    return await completer.future.timeout(timeout);
  } finally {
    await Future<void>.delayed(Duration.zero);
    await sub.cancel();
  }
}

String _topicOf(MqttReceivedMessage<MqttMessage> m) => m.topic;

MqttClientPayloadBuilder _bytes(String s) => MqttClientPayloadBuilder()..addString(s);

void main() {
  late MqttBroker broker;
  late int port;

  setUp(() async {
    port = await _freePort();
    broker = MqttBroker(
      address: '127.0.0.1',
      port: port,
      logger: SilentMqttLogger(),
    );
    await broker.start();
  });

  tearDown(() async {
    await broker.stop();
  });

  test('QoS 0 publish reaches subscriber', () async {
    final sub = await _connect(port, 'sub-q0');
    final pub = await _connect(port, 'pub-q0');

    sub.subscribe('t/qos0', MqttQos.atMostOnce);
    await Future<void>.delayed(const Duration(milliseconds: 100));

    final waiter = _waitNext(sub);
    pub.publishMessage('t/qos0', MqttQos.atMostOnce, _bytes('hello').payload!);

    final msg = await waiter;
    expect(MqttPublishPayload.bytesToStringAsString(msg.payload.message), 'hello');
  });

  test('QoS 1 publish round-trip', () async {
    final sub = await _connect(port, 'sub-q1');
    final pub = await _connect(port, 'pub-q1');

    sub.subscribe('t/qos1', MqttQos.atLeastOnce);
    await Future<void>.delayed(const Duration(milliseconds: 100));

    final waiter = _waitNext(sub);
    pub.publishMessage('t/qos1', MqttQos.atLeastOnce, _bytes('one').payload!);

    final msg = await waiter;
    expect(MqttPublishPayload.bytesToStringAsString(msg.payload.message), 'one');
  });

  test('QoS 2 publish round-trip', () async {
    final sub = await _connect(port, 'sub-q2');
    final pub = await _connect(port, 'pub-q2');

    sub.subscribe('t/qos2', MqttQos.exactlyOnce);
    await Future<void>.delayed(const Duration(milliseconds: 100));

    final waiter = _waitNext(sub);
    pub.publishMessage('t/qos2', MqttQos.exactlyOnce, _bytes('two').payload!);

    final msg = await waiter;
    expect(MqttPublishPayload.bytesToStringAsString(msg.payload.message), 'two');
  });

  test('wildcard + matches one level', () async {
    final sub = await _connect(port, 'sub-w1');
    final pub = await _connect(port, 'pub-w1');

    sub.subscribe('sensors/+/temp', MqttQos.atMostOnce);
    await Future<void>.delayed(const Duration(milliseconds: 100));

    final completer = Completer<MqttReceivedMessage<MqttMessage>>();

    final s = sub.updates!.listen((list) {
      if (!completer.isCompleted && list.isNotEmpty) completer.complete(list.first);
    });

    pub.publishMessage(
      'sensors/livingroom/temp',
      MqttQos.atMostOnce,
      _bytes('21.5').payload!,
    );

    final got = await completer.future.timeout(const Duration(seconds: 5));
    expect(_topicOf(got), 'sensors/livingroom/temp');

    await s.cancel();
  });

  test('wildcard # matches multiple levels', () async {
    final sub = await _connect(port, 'sub-w2');
    final pub = await _connect(port, 'pub-w2');

    sub.subscribe('a/#', MqttQos.atMostOnce);
    await Future<void>.delayed(const Duration(milliseconds: 100));

    final completer = Completer<String>();

    final s = sub.updates!.listen((list) {
      if (!completer.isCompleted && list.isNotEmpty) {
        completer.complete(_topicOf(list.first));
      }
    });

    pub.publishMessage('a/b/c/d', MqttQos.atMostOnce, _bytes('x').payload!);

    final topic = await completer.future.timeout(const Duration(seconds: 5));
    expect(topic, 'a/b/c/d');

    await s.cancel();
  });

  test('retained message is replayed to a new subscriber', () async {
    final pub = await _connect(port, 'pub-ret');
    pub.publishMessage(
      'status',
      MqttQos.atMostOnce,
      _bytes('latest').payload!,
      retain: true,
    );

    await Future<void>.delayed(const Duration(milliseconds: 200));

    final sub = await _connect(port, 'sub-ret');
    final waiter = _waitNext(sub);
    sub.subscribe('status', MqttQos.atMostOnce);

    final msg = await waiter;
    expect(MqttPublishPayload.bytesToStringAsString(msg.payload.message), 'latest');
    expect(msg.header!.retain, isTrue);
  });

  test('empty retained payload clears the retained store', () async {
    final pub = await _connect(port, 'pub-clr');
    pub.publishMessage(
      'clear/me',
      MqttQos.atMostOnce,
      _bytes('first').payload!,
      retain: true,
    );

    await Future<void>.delayed(const Duration(milliseconds: 100));

    pub.publishMessage(
      'clear/me',
      MqttQos.atMostOnce,
      MqttClientPayloadBuilder().payload!,
      retain: true,
    );

    await Future<void>.delayed(const Duration(milliseconds: 200));

    final sub = await _connect(port, 'sub-clr');

    var got = 0;
    final s = sub.updates!.listen((list) => got += list.length);
    sub.subscribe('clear/me', MqttQos.atMostOnce);

    await Future<void>.delayed(const Duration(milliseconds: 400));
    await s.cancel();

    expect(got, 0);
  });

  test('unsubscribe stops further deliveries', () async {
    final sub = await _connect(port, 'sub-un');
    final pub = await _connect(port, 'pub-un');

    sub.subscribe('un/x', MqttQos.atMostOnce);
    await Future<void>.delayed(const Duration(milliseconds: 100));

    final received = <String>[];

    final s = sub.updates!.listen((list) {
      for (final m in list) {
        final p = m.payload as MqttPublishMessage;
        received.add(MqttPublishPayload.bytesToStringAsString(p.payload.message));
      }
    });

    pub.publishMessage('un/x', MqttQos.atMostOnce, _bytes('one').payload!);
    await Future<void>.delayed(const Duration(milliseconds: 200));

    sub.unsubscribe('un/x');
    await Future<void>.delayed(const Duration(milliseconds: 200));

    pub.publishMessage('un/x', MqttQos.atMostOnce, _bytes('two').payload!);
    await Future<void>.delayed(const Duration(milliseconds: 200));

    await s.cancel();
    expect(received, ['one']);
  });

  test('broker emits connect / subscribe / publish / disconnect events', () async {
    final connects = <String>[];
    final subs = <String>[];
    final pubs = <String>[];
    final dcs = <String>[];

    broker.onConnect.listen((e) => connects.add(e.clientId));
    broker.onSubscribe.listen((e) => subs.add(e.filter));
    broker.onPublish.listen((e) => pubs.add(e.topic));
    broker.onDisconnect.listen((e) => dcs.add(e.clientId));

    final c = await _connect(port, 'evt-1');
    c.subscribe('evt/topic', MqttQos.atMostOnce);
    await Future<void>.delayed(const Duration(milliseconds: 100));

    c.publishMessage('evt/topic', MqttQos.atMostOnce, _bytes('x').payload!);
    await Future<void>.delayed(const Duration(milliseconds: 100));

    c.disconnect();
    await Future<void>.delayed(const Duration(milliseconds: 300));

    expect(connects, contains('evt-1'));
    expect(subs, contains('evt/topic'));
    expect(pubs, contains('evt/topic'));
    expect(dcs, contains('evt-1'));
  });

  test('broker.publish reaches subscribers', () async {
    final sub = await _connect(port, 'sub-bp');
    sub.subscribe('broker/out', MqttQos.atMostOnce);
    await Future<void>.delayed(const Duration(milliseconds: 100));

    final waiter = _waitNext(sub);
    broker.publish('broker/out', Uint8List.fromList([1, 2, 3]));

    final msg = await waiter;
    expect(msg.payload.message, [1, 2, 3]);
  });

  test('disconnectClient forcibly disconnects', () async {
    final c = await _connect(port, 'kick');
    await Future<void>.delayed(const Duration(milliseconds: 100));
    expect(broker.sessions.map((s) => s.clientId), contains('kick'));

    await broker.disconnectClient('kick');
    await Future<void>.delayed(const Duration(milliseconds: 300));

    expect(c.connectionStatus!.state, MqttConnectionState.disconnected);
  });

  test('session take-over closes the previous connection', () async {
    final c1 = await _connect(port, 'twin');
    await Future<void>.delayed(const Duration(milliseconds: 100));

    final c2 = await _connect(port, 'twin');
    await Future<void>.delayed(const Duration(milliseconds: 300));

    expect(c1.connectionStatus!.state, MqttConnectionState.disconnected);
    expect(c2.connectionStatus!.state, MqttConnectionState.connected);
    expect(broker.sessions.where((s) => s.clientId == 'twin').length, 1);
  });

  test('will message is published on abnormal disconnect', () async {
    final witness = await _connect(port, 'witness');
    witness.subscribe('last/words', MqttQos.atMostOnce);
    await Future<void>.delayed(const Duration(milliseconds: 100));

    final waiter = _waitNext(witness);

    final raw = await Socket.connect('127.0.0.1', port);
    final connect = ConnectPacket(
      protocolName: 'MQTT',
      protocolLevel: 4,
      clientId: 'raw-will',
      cleanSession: true,
      keepAlive: 60,
      willTopic: 'last/words',
      willPayload: Uint8List.fromList('rip'.codeUnits),
      willQos: 0,
      willRetain: false,
    );

    raw.add(connect.encode());
    await raw.flush();
    await raw.first;
    raw.destroy();

    final msg = await waiter;
    expect(MqttPublishPayload.bytesToStringAsString(msg.payload.message), 'rip');
  });

  test('graceful DISCONNECT does NOT publish the will', () async {
    final witness = await _connect(port, 'witness2');
    witness.subscribe('quiet/exit', MqttQos.atMostOnce);
    await Future<void>.delayed(const Duration(milliseconds: 100));

    final willClient = await _connect(
      port,
      'will-graceful',
      willTopic: 'quiet/exit',
      willMessage: 'should-not-fire',
    );
    willClient.disconnect();

    var got = 0;
    final s = witness.updates!.listen((list) => got += list.length);
    await Future<void>.delayed(const Duration(milliseconds: 400));
    await s.cancel();

    expect(got, 0);
  });

  test('keep-alive timeout disconnects an idle client', () async {
    final raw = await Socket.connect('127.0.0.1', port);

    final connackReceived = Completer<void>();
    final closedByServer = Completer<void>();

    final sub = raw.listen(
      (_) {
        if (!connackReceived.isCompleted) connackReceived.complete();
      },
      onDone: () {
        if (!closedByServer.isCompleted) closedByServer.complete();
      },
      onError: (_) {
        if (!closedByServer.isCompleted) closedByServer.complete();
      },
    );

    // keepAlive 1s × 1.5 = 1.5s timeout.
    final connect = ConnectPacket(
      protocolName: 'MQTT',
      protocolLevel: 4,
      clientId: 'idle',
      cleanSession: true,
      keepAlive: 1,
    );

    raw.add(connect.encode());
    await raw.flush();
    await connackReceived.future.timeout(const Duration(seconds: 2));
    await closedByServer.future.timeout(const Duration(seconds: 4));
    await sub.cancel();
    raw.destroy();

    await Future<void>.delayed(const Duration(milliseconds: 100));
    expect(broker.sessions.where((s) => s.clientId == 'idle').length, 0);
  });
}
