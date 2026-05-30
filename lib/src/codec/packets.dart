import 'dart:typed_data';

import 'byte_reader.dart';
import 'packet_type.dart';
import 'variable_byte_int.dart';

abstract class MqttPacket {
  MqttPacketType get type;

  Uint8List encode();

  static Uint8List _wrap(int firstByte, Uint8List body) {
    final length = VariableByteInt.encode(body.length);
    final out = Uint8List(1 + length.length + body.length);

    out[0] = firstByte;
    out.setRange(1, 1 + length.length, length);
    out.setRange(1 + length.length, out.length, body);

    return out;
  }
}

class ConnectPacket extends MqttPacket {
  ConnectPacket({
    required this.protocolName,
    required this.protocolLevel,
    required this.clientId,
    required this.cleanSession,
    required this.keepAlive,
    this.username,
    this.password,
    this.willTopic,
    this.willPayload,
    this.willQos = 0,
    this.willRetain = false,
  });

  final String protocolName;
  final int protocolLevel;
  final String clientId;
  final bool cleanSession;
  final int keepAlive;
  final String? username;
  final Uint8List? password;
  final String? willTopic;
  final Uint8List? willPayload;
  final int willQos;
  final bool willRetain;

  @override
  MqttPacketType get type => MqttPacketType.connect;

  static ConnectPacket decode(int firstByte, Uint8List rest) {
    final r = ByteReader(rest);

    final protocolName = r.readString();
    final protocolLevel = r.readUint8();
    final flags = r.readUint8();

    if ((flags & 0x01) != 0) {
      throw const FormatException('CONNECT reserved flag bit must be 0');
    }

    final hasUsername = (flags & 0x80) != 0;
    final hasPassword = (flags & 0x40) != 0;
    final willRetain = (flags & 0x20) != 0;
    final willQos = (flags & 0x18) >> 3;
    final hasWill = (flags & 0x04) != 0;
    final cleanSession = (flags & 0x02) != 0;

    final keepAlive = r.readUint16();
    final clientId = r.readString();

    String? willTopic;
    Uint8List? willPayload;

    if (hasWill) {
      willTopic = r.readString();
      willPayload = r.readBinary();
    } else if (willQos != 0 || willRetain) {
      throw const FormatException('Will QoS/Retain set without Will flag');
    }

    String? username;
    Uint8List? password;

    if (hasUsername) username = r.readString();
    if (hasPassword) password = r.readBinary();

    return ConnectPacket(
      protocolName: protocolName,
      protocolLevel: protocolLevel,
      clientId: clientId,
      cleanSession: cleanSession,
      keepAlive: keepAlive,
      username: username,
      password: password,
      willTopic: willTopic,
      willPayload: willPayload,
      willQos: willQos,
      willRetain: willRetain,
    );
  }

  @override
  Uint8List encode() {
    final w = ByteWriter();

    w.writeString(protocolName);
    w.writeUint8(protocolLevel);

    int flags = 0;
    if (username != null) flags |= 0x80;
    if (password != null) flags |= 0x40;
    if (willRetain) flags |= 0x20;
    flags |= (willQos & 0x03) << 3;
    if (willTopic != null) flags |= 0x04;
    if (cleanSession) flags |= 0x02;

    w.writeUint8(flags);
    w.writeUint16(keepAlive);
    w.writeString(clientId);

    if (willTopic != null) {
      w.writeString(willTopic!);
      w.writeBinary(willPayload ?? Uint8List(0));
    }

    if (username != null) w.writeString(username!);
    if (password != null) w.writeBinary(password!);

    return MqttPacket._wrap(0x10, w.takeBytes());
  }
}

class ConnAckPacket extends MqttPacket {
  ConnAckPacket({required this.sessionPresent, required this.returnCode});

  final bool sessionPresent;
  final MqttConnectReturnCode returnCode;

  @override
  MqttPacketType get type => MqttPacketType.connack;

  @override
  Uint8List encode() {
    final body = Uint8List(2);
    body[0] = sessionPresent ? 0x01 : 0x00;
    body[1] = returnCode.code;

    return MqttPacket._wrap(0x20, body);
  }
}

class PublishPacket extends MqttPacket {
  PublishPacket({
    required this.topic,
    required this.payload,
    this.qos = 0,
    this.dup = false,
    this.retain = false,
    this.packetId,
  });

  final String topic;
  final Uint8List payload;
  final int qos;
  final bool dup;
  final bool retain;
  final int? packetId;

  @override
  MqttPacketType get type => MqttPacketType.publish;

  static PublishPacket decode(int firstByte, Uint8List rest) {
    final dup = (firstByte & 0x08) != 0;
    final qos = (firstByte & 0x06) >> 1;
    final retain = (firstByte & 0x01) != 0;

    if (qos > 2) throw const FormatException('Invalid PUBLISH QoS');
    if (qos == 0 && dup) throw const FormatException('DUP must be 0 for QoS 0');

    final r = ByteReader(rest);
    final topic = r.readString();

    int? packetId;
    if (qos > 0) packetId = r.readUint16();

    final payload = r.readRemaining();

    return PublishPacket(
      topic: topic,
      payload: payload,
      qos: qos,
      dup: dup,
      retain: retain,
      packetId: packetId,
    );
  }

  @override
  Uint8List encode() {
    if (qos > 0 && packetId == null) {
      throw StateError('PUBLISH with QoS > 0 requires packetId');
    }

    final w = ByteWriter();
    w.writeString(topic);

    if (qos > 0) w.writeUint16(packetId!);

    w.writeBytes(payload);

    int firstByte = 0x30;
    if (dup) firstByte |= 0x08;
    firstByte |= (qos & 0x03) << 1;
    if (retain) firstByte |= 0x01;

    return MqttPacket._wrap(firstByte, w.takeBytes());
  }

  PublishPacket copyWith({int? qos, int? packetId, bool? dup, bool? retain}) {
    return PublishPacket(
      topic: topic,
      payload: payload,
      qos: qos ?? this.qos,
      dup: dup ?? this.dup,
      retain: retain ?? this.retain,
      packetId: packetId ?? this.packetId,
    );
  }
}

class _PacketIdOnly {
  static int decode(Uint8List rest) {
    if (rest.length < 2) throw const FormatException('Missing packet identifier');

    return (rest[0] << 8) | rest[1];
  }

  static Uint8List encode(int firstByte, int packetId) {
    final body = Uint8List(2);
    body[0] = (packetId >> 8) & 0xFF;
    body[1] = packetId & 0xFF;

    return MqttPacket._wrap(firstByte, body);
  }
}

class PubAckPacket extends MqttPacket {
  PubAckPacket(this.packetId);

  final int packetId;

  @override
  MqttPacketType get type => MqttPacketType.puback;

  static PubAckPacket decode(int _, Uint8List rest) => PubAckPacket(_PacketIdOnly.decode(rest));

  @override
  Uint8List encode() => _PacketIdOnly.encode(0x40, packetId);
}

class PubRecPacket extends MqttPacket {
  PubRecPacket(this.packetId);

  final int packetId;

  @override
  MqttPacketType get type => MqttPacketType.pubrec;

  static PubRecPacket decode(int _, Uint8List rest) => PubRecPacket(_PacketIdOnly.decode(rest));

  @override
  Uint8List encode() => _PacketIdOnly.encode(0x50, packetId);
}

class PubRelPacket extends MqttPacket {
  PubRelPacket(this.packetId);

  final int packetId;

  @override
  MqttPacketType get type => MqttPacketType.pubrel;

  static PubRelPacket decode(int firstByte, Uint8List rest) {
    if ((firstByte & 0x0F) != 0x02) {
      throw const FormatException('PUBREL flags must be 0010');
    }

    return PubRelPacket(_PacketIdOnly.decode(rest));
  }

  @override
  Uint8List encode() => _PacketIdOnly.encode(0x62, packetId);
}

class PubCompPacket extends MqttPacket {
  PubCompPacket(this.packetId);

  final int packetId;

  @override
  MqttPacketType get type => MqttPacketType.pubcomp;

  static PubCompPacket decode(int _, Uint8List rest) => PubCompPacket(_PacketIdOnly.decode(rest));

  @override
  Uint8List encode() => _PacketIdOnly.encode(0x70, packetId);
}

class SubscribeTopic {
  const SubscribeTopic(this.filter, this.qos);

  final String filter;
  final int qos;
}

class SubscribePacket extends MqttPacket {
  SubscribePacket({required this.packetId, required this.topics});

  final int packetId;
  final List<SubscribeTopic> topics;

  @override
  MqttPacketType get type => MqttPacketType.subscribe;

  static SubscribePacket decode(int firstByte, Uint8List rest) {
    if ((firstByte & 0x0F) != 0x02) {
      throw const FormatException('SUBSCRIBE flags must be 0010');
    }

    final r = ByteReader(rest);
    final packetId = r.readUint16();
    final topics = <SubscribeTopic>[];

    while (!r.isAtEnd) {
      final filter = r.readString();
      final qos = r.readUint8();

      if (qos > 2) throw const FormatException('Invalid requested QoS');

      topics.add(SubscribeTopic(filter, qos));
    }

    if (topics.isEmpty) {
      throw const FormatException('SUBSCRIBE must contain at least one topic');
    }

    return SubscribePacket(packetId: packetId, topics: topics);
  }

  @override
  Uint8List encode() {
    final w = ByteWriter();
    w.writeUint16(packetId);

    for (final t in topics) {
      w.writeString(t.filter);
      w.writeUint8(t.qos);
    }

    return MqttPacket._wrap(0x82, w.takeBytes());
  }
}

class SubAckPacket extends MqttPacket {
  SubAckPacket({required this.packetId, required this.returnCodes});

  final int packetId;
  final List<int> returnCodes;

  @override
  MqttPacketType get type => MqttPacketType.suback;

  @override
  Uint8List encode() {
    final w = ByteWriter();
    w.writeUint16(packetId);

    for (final rc in returnCodes) {
      w.writeUint8(rc);
    }

    return MqttPacket._wrap(0x90, w.takeBytes());
  }
}

class UnsubscribePacket extends MqttPacket {
  UnsubscribePacket({required this.packetId, required this.topics});

  final int packetId;
  final List<String> topics;

  @override
  MqttPacketType get type => MqttPacketType.unsubscribe;

  static UnsubscribePacket decode(int firstByte, Uint8List rest) {
    if ((firstByte & 0x0F) != 0x02) {
      throw const FormatException('UNSUBSCRIBE flags must be 0010');
    }

    final r = ByteReader(rest);
    final packetId = r.readUint16();
    final topics = <String>[];

    while (!r.isAtEnd) {
      topics.add(r.readString());
    }

    if (topics.isEmpty) {
      throw const FormatException('UNSUBSCRIBE must contain at least one topic');
    }

    return UnsubscribePacket(packetId: packetId, topics: topics);
  }

  @override
  Uint8List encode() {
    final w = ByteWriter();
    w.writeUint16(packetId);

    for (final t in topics) {
      w.writeString(t);
    }

    return MqttPacket._wrap(0xA2, w.takeBytes());
  }
}

class UnsubAckPacket extends MqttPacket {
  UnsubAckPacket(this.packetId);

  final int packetId;

  @override
  MqttPacketType get type => MqttPacketType.unsuback;

  @override
  Uint8List encode() => _PacketIdOnly.encode(0xB0, packetId);
}

class PingReqPacket extends MqttPacket {
  @override
  MqttPacketType get type => MqttPacketType.pingreq;

  @override
  Uint8List encode() => Uint8List.fromList([0xC0, 0x00]);
}

class PingRespPacket extends MqttPacket {
  @override
  MqttPacketType get type => MqttPacketType.pingresp;

  @override
  Uint8List encode() => Uint8List.fromList([0xD0, 0x00]);
}

class DisconnectPacket extends MqttPacket {
  @override
  MqttPacketType get type => MqttPacketType.disconnect;

  @override
  Uint8List encode() => Uint8List.fromList([0xE0, 0x00]);
}
