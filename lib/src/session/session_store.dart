import 'session.dart';

class SessionStore {
  final Map<String, MqttSession> _byClientId = {};

  MqttSession? get(String clientId) => _byClientId[clientId];

  void put(MqttSession session) => _byClientId[session.clientId] = session;

  MqttSession? remove(String clientId) => _byClientId.remove(clientId);

  Iterable<MqttSession> get all => _byClientId.values;
  int get size => _byClientId.length;
}
