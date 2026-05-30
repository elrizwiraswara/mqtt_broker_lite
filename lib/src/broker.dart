import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'auth/authenticator.dart';
import 'codec/packet_type.dart';
import 'codec/packets.dart';
import 'events.dart';
import 'logger.dart';
import 'session/session.dart';
import 'session/session_store.dart';
import 'topic/retained_store.dart';
import 'topic/subscription_store.dart';
import 'topic/topic_filter.dart';
import 'transport/connection.dart';
import 'transport/mqtt_transport.dart';
import 'transport/tcp_transport.dart';
import 'transport/ws_transport.dart';

typedef _Stopper = Future<void> Function();
typedef _Acceptor = void Function(MqttTransport transport);

/// Pure-Dart MQTT 3.1.1 broker.
class MqttBroker {
  MqttBroker._({
    required this.address,
    required this.port,
    required this.maxQos,
    required this.connectTimeout,
    required MqttAuthenticator authenticator,
    required MqttLogger logger,
    required Future<_Stopper> Function(_Acceptor) bind,
  })  : _authenticator = authenticator,
        _logger = logger,
        _bind = bind;

  /// Plain-TCP broker on [address]:[port] (default 1883).
  factory MqttBroker({
    required String address,
    int port = 1883,
    int maxQos = 2,
    Duration connectTimeout = const Duration(seconds: 10),
    MqttAuthenticator? authenticator,
    MqttLogger? logger,
  }) {
    return MqttBroker._(
      address: address,
      port: port,
      maxQos: maxQos,
      connectTimeout: connectTimeout,
      authenticator: authenticator ?? AllowAllAuthenticator(),
      logger: logger ?? PrintMqttLogger(),
      bind: (onAccept) async {
        final server = await ServerSocket.bind(address, port, shared: true);

        final sub = server.listen((socket) {
          socket.setOption(SocketOption.tcpNoDelay, true);
          onAccept(TcpTransport(socket));
        });

        return () async {
          await sub.cancel();
          await server.close();
        };
      },
    );
  }

  /// TLS broker on [address]:[port] (default 8883).
  factory MqttBroker.tls({
    required String address,
    int port = 8883,
    required SecurityContext context,
    int maxQos = 2,
    Duration connectTimeout = const Duration(seconds: 10),
    MqttAuthenticator? authenticator,
    MqttLogger? logger,
    bool requestClientCertificate = false,
    bool requireClientCertificate = false,
  }) {
    return MqttBroker._(
      address: address,
      port: port,
      maxQos: maxQos,
      connectTimeout: connectTimeout,
      authenticator: authenticator ?? AllowAllAuthenticator(),
      logger: logger ?? PrintMqttLogger(),
      bind: (onAccept) async {
        final server = await SecureServerSocket.bind(
          address,
          port,
          context,
          shared: true,
          requestClientCertificate: requestClientCertificate,
          requireClientCertificate: requireClientCertificate,
        );

        final sub = server.listen((socket) {
          socket.setOption(SocketOption.tcpNoDelay, true);
          onAccept(TcpTransport(socket));
        });

        return () async {
          await sub.cancel();
          await server.close();
        };
      },
    );
  }

  /// MQTT-over-WebSocket broker on [address]:[port] (default 8080). Accepts
  /// HTTP upgrade requests at [path] with the `mqtt` subprotocol.
  factory MqttBroker.ws({
    required String address,
    int port = 8080,
    String path = '/mqtt',
    int maxQos = 2,
    Duration connectTimeout = const Duration(seconds: 10),
    MqttAuthenticator? authenticator,
    MqttLogger? logger,
  }) {
    return MqttBroker._(
      address: address,
      port: port,
      maxQos: maxQos,
      connectTimeout: connectTimeout,
      authenticator: authenticator ?? AllowAllAuthenticator(),
      logger: logger ?? PrintMqttLogger(),
      bind: (onAccept) async => _bindHttp(
        onAccept,
        log: logger ?? PrintMqttLogger(),
        path: path,
        server: await HttpServer.bind(address, port, shared: true),
      ),
    );
  }

  /// MQTT-over-WebSocket-over-TLS broker on [address]:[port] (default 8443).
  factory MqttBroker.wss({
    required String address,
    int port = 8443,
    String path = '/mqtt',
    required SecurityContext context,
    int maxQos = 2,
    Duration connectTimeout = const Duration(seconds: 10),
    MqttAuthenticator? authenticator,
    MqttLogger? logger,
  }) {
    return MqttBroker._(
      address: address,
      port: port,
      maxQos: maxQos,
      connectTimeout: connectTimeout,
      authenticator: authenticator ?? AllowAllAuthenticator(),
      logger: logger ?? PrintMqttLogger(),
      bind: (onAccept) async => _bindHttp(
        onAccept,
        log: logger ?? PrintMqttLogger(),
        path: path,
        server: await HttpServer.bindSecure(address, port, context, shared: true),
      ),
    );
  }

  static Future<_Stopper> _bindHttp(
    _Acceptor onAccept, {
    required MqttLogger log,
    required String path,
    required HttpServer server,
  }) async {
    final sub = server.listen((request) async {
      if (path.isNotEmpty && request.uri.path != path) {
        request.response
          ..statusCode = HttpStatus.notFound
          ..close().ignore();
        return;
      }

      if (!WebSocketTransformer.isUpgradeRequest(request)) {
        request.response
          ..statusCode = HttpStatus.badRequest
          ..write('Expected a WebSocket upgrade with MQTT subprotocol.')
          ..close().ignore();
        return;
      }

      try {
        final ws = await WebSocketTransformer.upgrade(
          request,
          protocolSelector: (offered) {
            final picked = selectMqttSubprotocol(offered.toList());

            if (picked == null) {
              throw const WebSocketException('No MQTT subprotocol offered by client');
            }

            return picked;
          },
        );

        final info = request.connectionInfo;
        final remote =
            info == null ? 'ws-unknown' : '${info.remoteAddress.address}:${info.remotePort}';

        onAccept(WebSocketTransport(ws, remote));
      } catch (e, st) {
        log.warning('WebSocket upgrade failed: $e');
        log.debug('$st');
      }
    });

    return () async {
      await sub.cancel();
      await server.close();
    };
  }

  final String address;
  final int port;
  final int maxQos;
  final Duration connectTimeout;

  final MqttAuthenticator _authenticator;
  final MqttLogger _logger;
  final Future<_Stopper> Function(_Acceptor) _bind;

  final SessionStore _sessions = SessionStore();
  final SubscriptionStore _subscriptions = SubscriptionStore();
  final RetainedStore _retained = RetainedStore();
  final Map<ClientConnection, MqttSession?> _pendingConnections = {};

  final StreamController<MqttConnectEvent> _connectCtrl = StreamController.broadcast();
  final StreamController<MqttDisconnectEvent> _disconnectCtrl = StreamController.broadcast();
  final StreamController<MqttSubscribeEvent> _subscribeCtrl = StreamController.broadcast();
  final StreamController<MqttUnsubscribeEvent> _unsubscribeCtrl = StreamController.broadcast();
  final StreamController<MqttPublishEvent> _publishCtrl = StreamController.broadcast();

  Stream<MqttConnectEvent> get onConnect => _connectCtrl.stream;
  Stream<MqttDisconnectEvent> get onDisconnect => _disconnectCtrl.stream;
  Stream<MqttSubscribeEvent> get onSubscribe => _subscribeCtrl.stream;
  Stream<MqttUnsubscribeEvent> get onUnsubscribe => _unsubscribeCtrl.stream;
  Stream<MqttPublishEvent> get onPublish => _publishCtrl.stream;

  /// All currently-tracked sessions, online or offline-with-clean-session=false.
  Iterable<MqttSession> get sessions => _sessions.all;

  _Stopper? _stopServer;

  int _autoIdCounter = 0;

  Future<void> start() async {
    if (_stopServer != null) {
      throw StateError('MqttBroker already started');
    }

    _stopServer = await _bind(_handleTransport);
    _logger.info('Broker listening on $address:$port');
  }

  Future<void> stop() async {
    final stop = _stopServer;
    _stopServer = null;

    if (stop != null) await stop();

    final pending = _pendingConnections.keys.toList();

    for (final c in pending) {
      await c.close(graceful: true);
    }

    _pendingConnections.clear();

    final sessions = _sessions.all.toList();

    for (final s in sessions) {
      await s.connection?.close(graceful: true);
      s.clear();
      _subscriptions.removeSession(s);
    }

    _retained.clear();
    _logger.info('Broker stopped');
  }

  /// Publishes a message from the broker itself (no originating session).
  void publish(String topic, Uint8List payload, {int qos = 0, bool retain = false}) {
    if (!TopicFilter.isValidPublishTopic(topic)) {
      throw ArgumentError('Invalid topic: $topic');
    }

    if (qos < 0 || qos > maxQos) {
      throw ArgumentError('QoS $qos out of range');
    }

    _publish(
      source: null,
      topic: topic,
      payload: payload,
      qos: qos,
      retain: retain,
    );
  }

  /// Forcibly disconnects a client by [clientId]. Returns true if found.
  Future<bool> disconnectClient(String clientId) async {
    final s = _sessions.get(clientId);

    if (s == null) return false;

    final conn = s.connection;

    if (conn != null) {
      await conn.close(graceful: true);
    } else {
      _evictOfflineSession(s);
    }

    return true;
  }

  void _handleTransport(MqttTransport transport) {
    late final ClientConnection conn;

    conn = ClientConnection(
      transport: transport,
      logger: _logger,
      onPacket: (pkt) => _onPacket(conn, pkt),
      onClose: ({required graceful, error}) => _onConnectionClosed(conn, graceful: graceful),
    );

    _pendingConnections[conn] = null;

    Timer(connectTimeout, () {
      if (_pendingConnections[conn] == null && !conn.isClosed) {
        _logger.warning('CONNECT timeout from ${conn.remote}');
        conn.close(graceful: false);
      }
    });
  }

  void _onConnectionClosed(ClientConnection conn, {required bool graceful}) {
    final session = _pendingConnections[conn];
    _pendingConnections.remove(conn);

    if (session == null) return;

    // Skip if this connection has already been replaced by a take-over.
    if (session.connection != conn) return;

    session.connection = null;

    final will = session.will;

    if (!graceful && will != null) {
      _publish(
        source: session,
        topic: will.topic,
        payload: will.payload,
        qos: will.qos,
        retain: will.retain,
      );
    }

    session.will = null;

    if (session.cleanSession) {
      _subscriptions.removeSession(session);
      _sessions.remove(session.clientId);
      session.clear();
    }

    _disconnectCtrl.add(MqttDisconnectEvent(session, graceful: graceful));
  }

  void _evictOfflineSession(MqttSession session) {
    _subscriptions.removeSession(session);
    _sessions.remove(session.clientId);
    session.clear();
  }

  void _onPacket(ClientConnection conn, MqttPacket packet) {
    final session = _pendingConnections[conn];

    if (session == null) {
      if (packet is ConnectPacket) {
        _handleConnect(conn, packet);
      } else {
        _logger.warning('First packet from ${conn.remote} was ${packet.type}, expected CONNECT');
        conn.close(graceful: false);
      }

      return;
    }

    switch (packet) {
      case ConnectPacket _:
        _logger.warning('Second CONNECT from ${session.clientId}, disconnecting');
        conn.close(graceful: false);

      case PublishPacket p:
        _handlePublish(session, p);

      case PubAckPacket p:
        session.inflightPublish.remove(p.packetId);

      case PubRecPacket p:
        final pub = session.inflightPublish.remove(p.packetId);

        if (pub != null) {
          session.inflightPubrel.add(p.packetId);
          conn.send(PubRelPacket(p.packetId));
        }

      case PubRelPacket p:
        final stored = session.inboundQos2.remove(p.packetId);

        if (stored != null) {
          _publish(
            source: session,
            topic: stored.topic,
            payload: stored.payload,
            qos: stored.qos,
            retain: stored.retain,
          );
        }

        conn.send(PubCompPacket(p.packetId));

      case PubCompPacket p:
        session.inflightPubrel.remove(p.packetId);

      case SubscribePacket p:
        _handleSubscribe(session, p);

      case UnsubscribePacket p:
        _handleUnsubscribe(session, p);

      case PingReqPacket _:
        conn.send(PingRespPacket());

      case DisconnectPacket _:
        // Graceful disconnect: discard will.
        session.will = null;
        conn.close(graceful: true);

      default:
        _logger.warning('Unhandled packet ${packet.type} from ${session.clientId}');
        conn.close(graceful: false);
    }
  }

  void _handleConnect(ClientConnection conn, ConnectPacket pkt) {
    if (!_isSupportedProtocol(pkt.protocolName, pkt.protocolLevel)) {
      conn.send(ConnAckPacket(
        sessionPresent: false,
        returnCode: MqttConnectReturnCode.unacceptableProtocolVersion,
      ));
      conn.close(graceful: false);
      return;
    }

    // MQTT-3.1.3-7: empty client id is permitted only with cleanSession=true.
    if (pkt.clientId.isEmpty && !pkt.cleanSession) {
      conn.send(ConnAckPacket(
        sessionPresent: false,
        returnCode: MqttConnectReturnCode.identifierRejected,
      ));
      conn.close(graceful: false);
      return;
    }

    final effectiveClientId = pkt.clientId.isEmpty ? _generateClientId() : pkt.clientId;

    _authenticator
        .authenticate(
      clientId: effectiveClientId,
      username: pkt.username,
      password: pkt.password,
    )
        .then((result) {
      if (conn.isClosed) return;

      if (!result.accepted) {
        conn.send(ConnAckPacket(sessionPresent: false, returnCode: result.returnCode));
        conn.close(graceful: false);
        return;
      }

      _completeConnect(conn, pkt, effectiveClientId);
    }).catchError((Object e, StackTrace st) {
      _logger.error('Authenticator failed', e, st);

      if (!conn.isClosed) {
        conn.send(ConnAckPacket(
          sessionPresent: false,
          returnCode: MqttConnectReturnCode.serverUnavailable,
        ));
        conn.close(graceful: false);
      }
    });
  }

  void _completeConnect(ClientConnection conn, ConnectPacket pkt, String clientId) {
    final existing = _sessions.get(clientId);
    bool sessionPresent = false;

    MqttSession session;

    if (existing != null) {
      final oldConn = existing.connection;

      if (oldConn != null) {
        _logger.info('Session take-over for $clientId from ${oldConn.remote}');
        // Take-over: graceful close, no will publish.
        existing.will = null;
        existing.connection = null;
        oldConn.close(graceful: true);
      }

      if (pkt.cleanSession) {
        existing.clear();
        _subscriptions.removeSession(existing);
        session = existing..cleanSession = true;
        sessionPresent = false;
      } else {
        session = existing
          ..cleanSession = false
          ..resetTransient();
        sessionPresent = true;
      }
    } else {
      session = MqttSession(clientId: clientId, cleanSession: pkt.cleanSession);
      _sessions.put(session);
      sessionPresent = false;
    }

    if (pkt.willTopic != null) {
      session.will = WillMessage(
        topic: pkt.willTopic!,
        payload: pkt.willPayload ?? Uint8List(0),
        qos: pkt.willQos,
        retain: pkt.willRetain,
      );
    }

    session.connection = conn;
    _pendingConnections[conn] = session;

    conn.send(ConnAckPacket(
      sessionPresent: sessionPresent,
      returnCode: MqttConnectReturnCode.accepted,
    ));
    conn.armKeepAlive(pkt.keepAlive);

    _connectCtrl.add(MqttConnectEvent(session));

    // Re-send any in-flight QoS 1/2 messages from a previous connection with DUP=1.
    for (final pkt in session.inflightPublish.values.toList()) {
      conn.send(pkt.copyWith(dup: true));
    }

    for (final packetId in session.inflightPubrel.toList()) {
      conn.send(PubRelPacket(packetId));
    }

    // Drain offline-queued messages. These have never been sent: DUP=0, then
    // become inflight on send.
    final pending = List<PublishPacket>.from(session.pendingPublishes);
    session.pendingPublishes.clear();

    for (final pkt in pending) {
      session.inflightPublish[pkt.packetId!] = pkt;
      conn.send(pkt);
    }
  }

  bool _isSupportedProtocol(String name, int level) {
    if (name == 'MQTT' && level == 4) return true;
    if (name == 'MQIsdp' && level == 3) return true;

    return false;
  }

  String _generateClientId() =>
      'auto-${DateTime.now().millisecondsSinceEpoch}-${_autoIdCounter++}';

  void _handlePublish(MqttSession session, PublishPacket pkt) {
    if (!TopicFilter.isValidPublishTopic(pkt.topic)) {
      _logger.warning('Invalid PUBLISH topic from ${session.clientId}: ${pkt.topic}');
      session.connection?.close(graceful: false);
      return;
    }

    if (pkt.qos > maxQos) {
      _logger.warning('PUBLISH QoS ${pkt.qos} exceeds maxQos ($maxQos)');
      session.connection?.close(graceful: false);
      return;
    }

    switch (pkt.qos) {
      case 0:
        _publish(
          source: session,
          topic: pkt.topic,
          payload: pkt.payload,
          qos: 0,
          retain: pkt.retain,
        );

      case 1:
        _publish(
          source: session,
          topic: pkt.topic,
          payload: pkt.payload,
          qos: 1,
          retain: pkt.retain,
        );
        session.connection?.send(PubAckPacket(pkt.packetId!));

      case 2:
        // Store and PUBREC; deliver on PUBREL.
        session.inboundQos2[pkt.packetId!] = pkt;
        session.connection?.send(PubRecPacket(pkt.packetId!));
    }
  }

  void _publish({
    required MqttSession? source,
    required String topic,
    required Uint8List payload,
    required int qos,
    required bool retain,
  }) {
    _publishCtrl.add(MqttPublishEvent(
      session: source,
      topic: topic,
      payload: payload,
      qos: qos,
      retain: retain,
    ));

    if (retain) {
      _retained.store(topic, payload, qos);
    }

    for (final m in _subscriptions.match(topic)) {
      _deliverTo(m.session, topic, payload, _min(qos, m.grantedQos), retain: false);
    }
  }

  void _deliverTo(
    MqttSession session,
    String topic,
    Uint8List payload,
    int qos, {
    required bool retain,
  }) {
    final conn = session.connection;

    if (qos == 0) {
      // Fire-and-forget; dropped if subscriber is offline.
      conn?.send(PublishPacket(
        topic: topic,
        payload: payload,
        qos: 0,
        retain: retain,
      ));
      return;
    }

    final pkt = PublishPacket(
      topic: topic,
      payload: payload,
      qos: qos,
      retain: retain,
      packetId: session.allocatePacketId(),
    );

    if (conn != null) {
      session.inflightPublish[pkt.packetId!] = pkt;
      conn.send(pkt);
    } else {
      // Moved into inflightPublish when the session reconnects.
      session.pendingPublishes.add(pkt);
    }
  }

  int _min(int a, int b) => a < b ? a : b;

  void _handleSubscribe(MqttSession session, SubscribePacket pkt) {
    final returnCodes = <int>[];
    final acceptedFilters = <String>[];

    for (final t in pkt.topics) {
      if (!TopicFilter.isValidFilter(t.filter) || t.qos > maxQos) {
        returnCodes.add(subAckFailure);
        continue;
      }

      final granted = _subscriptions.subscribe(session, t.filter, t.qos, maxQos: maxQos);
      returnCodes.add(granted);
      acceptedFilters.add(t.filter);

      _subscribeCtrl.add(MqttSubscribeEvent(
        session: session,
        filter: t.filter,
        grantedQos: granted,
      ));
    }

    session.connection?.send(SubAckPacket(packetId: pkt.packetId, returnCodes: returnCodes));

    // Replay retained messages matching any newly-accepted filter.
    for (var i = 0; i < acceptedFilters.length; i++) {
      final filter = acceptedFilters[i];
      final qos = returnCodes[i];

      for (final r in _retained.matching(filter)) {
        _deliverTo(session, r.topic, r.payload, _min(r.qos, qos), retain: true);
      }
    }
  }

  void _handleUnsubscribe(MqttSession session, UnsubscribePacket pkt) {
    for (final f in pkt.topics) {
      if (_subscriptions.unsubscribe(session, f)) {
        _unsubscribeCtrl.add(MqttUnsubscribeEvent(session: session, filter: f));
      }
    }

    session.connection?.send(UnsubAckPacket(pkt.packetId));
  }
}
