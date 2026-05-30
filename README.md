# dart_mqtt_broker

A pure-Dart MQTT 3.1.1 broker.

## Features

- **Full MQTT 3.1.1**: CONNECT, PUBLISH, SUBSCRIBE, UNSUBSCRIBE, PINGREQ, DISCONNECT and all ACK packets.
- **QoS 0, 1, and 2** with full inbound and outbound state machines.
- **Wildcard subscriptions** (`+` single-level, `#` multi-level).
- **Retained messages** with replay to new subscribers; empty payload clears.
- **Will messages** published on abnormal disconnect.
- **Pluggable authentication** via the `MqttAuthenticator` interface.
- **Session takeover** when a client reconnects with the same identifier.
- **Persistent sessions** (`cleanSession = false`): subscriptions and queued QoS 1/2 messages survive disconnect in memory.
- **Keep-alive enforcement** at 1.5× the negotiated interval.
- **TLS support** via `MqttBroker.tls(...)`.
- **Stream-based event API** for connect, disconnect, subscribe, unsubscribe, publish.

> Not implemented in this release: MQTT 5.0, WebSocket transport, on-disk session persistence.

## Install

```yaml
dependencies:
  dart_mqtt_broker: ^2.0.0
```

## Basic example

```dart
import 'dart:io';
import 'package:dart_mqtt_broker/dart_mqtt_broker.dart';

Future<void> main() async {
  final broker = MqttBroker(
    address: InternetAddress.anyIPv4.address,
    port: 1883,
  );

  broker.onConnect.listen((e) => print('Connected: ${e.clientId}'));
  broker.onDisconnect.listen((e) => print('Disconnected: ${e.clientId}'));
  broker.onSubscribe.listen((e) => print('Subscribed: ${e.clientId} -> ${e.filter}'));
  broker.onPublish.listen((e) => print('Published: ${e.topic}'));

  await broker.start();
}
```

## TLS

```dart
final ctx = SecurityContext()
  ..useCertificateChain('cert.pem')
  ..usePrivateKey('key.pem');

final broker = MqttBroker.tls(
  address: '0.0.0.0',
  port: 8883,
  context: ctx,
);
await broker.start();
```

## Authentication

```dart
class MyAuth extends MqttAuthenticator {
  @override
  Future<MqttAuthResult> authenticate({
    required String clientId,
    String? username,
    Uint8List? password,
  }) async {
    if (username == 'admin' && _checkPassword(password)) {
      return const MqttAuthResult.accept();
    }
    return const MqttAuthResult.reject(MqttConnectReturnCode.badUsernameOrPassword);
  }
}

final broker = MqttBroker(
  address: '0.0.0.0',
  authenticator: MyAuth(),
);
```

## Publishing from the broker

```dart
broker.publish(
  'sensors/heartbeat',
  Uint8List.fromList(utf8.encode('ok')),
  qos: 1,
  retain: false,
);
```

## Forcibly disconnect a client

```dart
await broker.disconnectClient('clientId');
```

## Contributing

Issues and pull requests are welcome at [github.com/elrizwiraswara/dart_mqtt_broker](https://github.com/elrizwiraswara/dart_mqtt_broker). For bug reports, please include the broker version, a minimal reproduction (client code + the packet flow if possible), and the broker log output. New features should come with tests under `test/` — the integration suite uses `mqtt_client` as a real MQTT client and is the easiest place to verify protocol behavior end-to-end.

## License

Released under the [MIT License](LICENSE).
