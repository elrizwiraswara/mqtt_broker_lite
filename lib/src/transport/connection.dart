import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import '../codec/packet_reader.dart';
import '../codec/packets.dart';
import '../logger.dart';

typedef PacketHandler = void Function(MqttPacket packet);
typedef ConnectionCloseHandler = void Function({required bool graceful, Object? error});

/// Wraps a TCP/TLS socket. Buffers bytes through a [PacketReader], decodes
/// complete packets, and dispatches them via [onPacket]. Enforces MQTT
/// keep-alive (1.5x the negotiated interval) once [armKeepAlive] is called.
class ClientConnection {
  ClientConnection({
    required this.socket,
    required this.onPacket,
    required this.onClose,
    required this.logger,
  }) {
    _sub = socket.listen(
      _onBytes,
      onError: _onSocketError,
      onDone: _onSocketDone,
      cancelOnError: true,
    );
  }

  final Socket socket;
  final PacketHandler onPacket;
  final ConnectionCloseHandler onClose;
  final MqttLogger logger;

  late final StreamSubscription<Uint8List> _sub;
  final PacketReader _reader = PacketReader();

  Timer? _keepAliveTimer;
  Duration? _keepAliveTimeout;
  bool _closed = false;

  String get remote => '${socket.remoteAddress.address}:${socket.remotePort}';

  /// Arm or reset the keep-alive timeout. [seconds] is the MQTT Keep Alive
  /// value from CONNECT; 0 means disabled.
  void armKeepAlive(int seconds) {
    _keepAliveTimer?.cancel();
    if (seconds <= 0) {
      _keepAliveTimeout = null;
      return;
    }
    // MQTT 3.1.1 §3.1.2.10: server must disconnect after 1.5x keepalive of silence.
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
      socket.add(packet.encode());
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

  void _onSocketError(Object error, StackTrace stack) {
    logger.warning('Socket error from $remote: $error');
    close(graceful: false, error: error);
  }

  void _onSocketDone() {
    // TCP-level close without a prior MQTT DISCONNECT is abnormal — the
    // session's will (if any) must be published. A graceful DISCONNECT
    // packet handler will have already called [close] with graceful:true
    // before the socket actually finishes, so this branch only fires when
    // the client really did vanish without notice.
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
      await socket.close();
    } catch (_) {}
    try {
      socket.destroy();
    } catch (_) {}
    onClose(graceful: graceful, error: error);
  }

  bool get isClosed => _closed;
}
