## 2.1.0

### New
- `MqttBroker.ws(...)` — MQTT-over-WebSocket transport (default port 8080).
- `MqttBroker.wss(...)` — MQTT-over-WebSocket-over-TLS transport (default port 8443).
- Pluggable `MqttTransport` abstraction; `TcpTransport`, `WebSocketTransport`.

### Changed
- Internal `ClientConnection` now drives any `MqttTransport`, not just a raw `Socket`. The public broker API is unchanged.

## 2.0.0

Complete rewrite. The broker is now a real MQTT 3.1.1 implementation.

### Breaking changes
- New `Stream`-based event API replaces `onXxxListener` callbacks. See README.
- The `Client` class is replaced by `MqttSession` (read-only view of a connected client).
- Library entry point is now `package:dart_mqtt_broker/dart_mqtt_broker.dart`.

### New
- Streaming TCP packet reassembler (fixes packet fragmentation / batching bug).
- Full wildcard topic matching (`+` and `#`).
- Retained messages with replay to new subscribers and clear-on-empty-payload.
- UNSUBSCRIBE / UNSUBACK.
- Will / Testament message published on abnormal disconnect.
- Pluggable authentication (`MqttAuthenticator`).
- Session takeover on duplicate client identifier.
- Clean session = false: subscriptions and queued QoS 1/2 messages persisted in-memory across reconnect.
- Keep-alive enforcement (1.5x timeout per spec).
- Full QoS 2 inbound state machine (store on PUBLISH, deliver on PUBREL).
- Per-session monotonic outbound packet identifiers with QoS 1/2 inflight tracking.
- Granted QoS downgrade per subscription.
- Topic validation (rejects `+`/`#` in PUBLISH topics, malformed filters).
- TLS server (`MqttBroker.tls`).
- Pluggable `MqttLogger`.

### Fixed
- TCP stream framing: previously dropped fragmented and batched packets.
- Packet-type matching now masks the high nibble (PUBLISH with RETAIN no longer "unknown").
- Outbound PUBLISH with QoS > 0 now uses a valid non-zero packet identifier.
- CONNACK Session Present flag now reflects server-side session state.
- CONNECT parser handles MQTT 3.1 (`MQIsdp`) as well as 3.1.1 (`MQTT`).

## 1.0.4

- Documentation updates.

## 1.0.0

- Initial version.
