import 'dart:async';
import 'dart:typed_data';

import '../codec/packet_reader.dart';
import '../codec/packets.dart';
import '../logger.dart';
import 'mqtt_transport.dart';

typedef PacketHandler = void Function(MqttPacket packet);
typedef ConnectionCloseHandler = void Function({required bool graceful, Object? error});

/// Drives one client over an [MqttTransport]. Decodes complete packets and
/// dispatches via [onPacket]. Enforces keep-alive once [armKeepAlive] is
/// called.
class ClientConnection {
  ClientConnection({
    required this.transport,
    required this.onPacket,
    required this.onClose,
    required this.logger,
  }) {
    _sub = transport.inbound.listen(
      _onBytes,
      onError: _onTransportError,
      onDone: _onTransportDone,
      cancelOnError: true,
    );
  }

  final MqttTransport transport;
  final PacketHandler onPacket;
  final ConnectionCloseHandler onClose;
  final MqttLogger logger;

  late final StreamSubscription<Uint8List> _sub;
  final PacketReader _reader = PacketReader();

  Timer? _keepAliveTimer;
  Duration? _keepAliveTimeout;
  bool _closed = false;

  String get remote => transport.remote;
  bool get isClosed => _closed;

  void armKeepAlive(int seconds) {
    _keepAliveTimer?.cancel();

    if (seconds <= 0) {
      _keepAliveTimeout = null;
      return;
    }

    // MQTT 3.1.1 section 3.1.2.10: server must disconnect after 1.5x keepalive of silence.
    _keepAliveTimeout = Duration(milliseconds: (seconds * 1500).toInt());
    _resetKeepAlive();
  }

  void _resetKeepAlive() {
    if (_keepAliveTimeout == null) return;

    _keepAliveTimer?.cancel();
    _keepAliveTimer = Timer(_keepAliveTimeout!, () {
      logger.warning('Keep-alive timeout from $remote');
      close(graceful: false);
    });
  }

  void send(MqttPacket packet) {
    if (_closed) return;

    try {
      transport.send(packet.encode());
    } catch (e, st) {
      logger.error('Write failed to $remote', e, st);
      close(graceful: false, error: e);
    }
  }

  void _onBytes(Uint8List data) {
    if (_closed) return;

    _resetKeepAlive();

    try {
      _reader.addBytes(data);

      final raws = _reader.readPackets();

      for (final raw in raws) {
        final MqttPacket pkt;

        try {
          pkt = decodeInboundPacket(raw);
        } on FormatException catch (e) {
          logger.warning('Malformed packet from $remote: ${e.message}');
          close(graceful: false, error: e);
          return;
        }

        onPacket(pkt);

        if (_closed) return;
      }
    } catch (e, st) {
      logger.error('Reader error for $remote', e, st);
      close(graceful: false, error: e);
    }
  }

  void _onTransportError(Object error, StackTrace stack) {
    logger.warning('Transport error from $remote: $error');
    close(graceful: false, error: error);
  }

  // Transport-level close without a prior MQTT DISCONNECT is abnormal: the
  // session's will (if any) must fire. A graceful DISCONNECT handler will
  // have already called [close] with graceful:true before the transport
  // finishes, so this branch only fires on a real abnormal exit.
  void _onTransportDone() {
    close(graceful: false);
  }

  Future<void> close({bool graceful = true, Object? error}) async {
    if (_closed) return;

    _closed = true;
    _keepAliveTimer?.cancel();

    try {
      await _sub.cancel();
    } catch (_) {}

    try {
      await transport.close();
    } catch (_) {}

    transport.destroy();

    onClose(graceful: graceful, error: error);
  }
}
