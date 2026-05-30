import '../session/session.dart';
import 'topic_filter.dart';

class SubscriptionMatch {
  const SubscriptionMatch(this.session, this.grantedQos);

  final MqttSession session;
  final int grantedQos;
}

class SubscriptionStore {
  final Map<MqttSession, Map<String, int>> _bySession = {};

  int subscribe(MqttSession session, String filter, int requestedQos, {int maxQos = 2}) {
    final granted = requestedQos < maxQos ? requestedQos : maxQos;
    final filters = _bySession.putIfAbsent(session, () => {});
    filters[filter] = granted;

    return granted;
  }

  bool unsubscribe(MqttSession session, String filter) {
    final filters = _bySession[session];

    if (filters == null) return false;

    final existed = filters.remove(filter) != null;

    if (filters.isEmpty) _bySession.remove(session);

    return existed;
  }

  void removeSession(MqttSession session) => _bySession.remove(session);

  /// Returns matching subscriptions with the highest granted QoS per session.
  List<SubscriptionMatch> match(String topic) {
    final out = <SubscriptionMatch>[];

    _bySession.forEach((session, filters) {
      int? best;

      filters.forEach((filter, qos) {
        if (TopicFilter.matches(filter, topic)) {
          if (best == null || qos > best!) best = qos;
        }
      });

      if (best != null) {
        out.add(SubscriptionMatch(session, best!));
      }
    });

    return out;
  }

  Map<String, int> filtersOf(MqttSession session) =>
      Map.unmodifiable(_bySession[session] ?? const <String, int>{});

  void replaceFiltersFor(MqttSession session, Map<String, int> filters) {
    if (filters.isEmpty) {
      _bySession.remove(session);
    } else {
      _bySession[session] = Map.of(filters);
    }
  }
}
