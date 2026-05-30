import 'dart:convert';
import 'dart:io';

import 'package:mqtt_broker_lite/mqtt_broker_lite.dart';

Future<void> main() async {
  final broker = MqttBroker(
    address: InternetAddress.anyIPv4.address,
    port: 1883,
  );

  broker.onConnect.listen((e) => print('Connected: ${e.clientId}'));
  broker.onDisconnect.listen((e) => print('Disconnected: ${e.clientId} (graceful=${e.graceful})'));
  broker.onSubscribe.listen(
    (e) => print('Subscribe: ${e.clientId} -> ${e.filter} qos=${e.grantedQos}'),
  );
  broker.onUnsubscribe.listen((e) => print('Unsubscribe: ${e.clientId} -> ${e.filter}'));
  broker.onPublish.listen((e) {
    final preview = _previewPayload(e.payload);
    print(
      'Publish: ${e.clientId ?? "broker"} -> ${e.topic} qos=${e.qos} retain=${e.retain} payload=$preview',
    );
  });

  await broker.start();
  print('MQTT Broker running on ${broker.address}:${broker.port}');

  ProcessSignal.sigint.watch().listen((_) async {
    print('\nShutting down...');
    await broker.stop();
    exit(0);
  });
}

String _previewPayload(List<int> payload) {
  try {
    final text = utf8.decode(payload, allowMalformed: false);
    return '"$text"';
  } catch (_) {
    return '<${payload.length} bytes>';
  }
}
