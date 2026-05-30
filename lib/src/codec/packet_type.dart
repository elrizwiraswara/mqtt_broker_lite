enum MqttPacketType {
  reserved(0),
  connect(1),
  connack(2),
  publish(3),
  puback(4),
  pubrec(5),
  pubrel(6),
  pubcomp(7),
  subscribe(8),
  suback(9),
  unsubscribe(10),
  unsuback(11),
  pingreq(12),
  pingresp(13),
  disconnect(14);

  const MqttPacketType(this.code);

  final int code;

  static MqttPacketType fromFirstByte(int firstByte) {
    final code = (firstByte & 0xF0) >> 4;

    if (code < 0 || code > 14) return MqttPacketType.reserved;

    return MqttPacketType.values[code];
  }
}

enum MqttConnectReturnCode {
  accepted(0),
  unacceptableProtocolVersion(1),
  identifierRejected(2),
  serverUnavailable(3),
  badUsernameOrPassword(4),
  notAuthorized(5);

  const MqttConnectReturnCode(this.code);

  final int code;
}

const int subAckFailure = 0x80;
