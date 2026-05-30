import '../session/session.dart';
import 'topic_filter.dart';

/// Stores `(session, filter, qos)` triples and answers "who subscribes to
/// this topic?" with the granted QoS per subscriber.
///
/// A flat-list implementation. Acceptable for embedded-broker scale (≤ a few
/// thousand subscriptions); upgrade to a trie if profiling demands it.
class SubscriptionStore {
  final Map<MqttSession, Map<String, int>> _bySession = {};

  /// Adds or replaces a subscription. Returns the granted QoS (may be
  /// downgraded by the broker via [maxQos]).
  int subscribe(MqttSession session, String filter, int requestedQos, {int maxQos = 2}) {
    final granted = requestedQos < maxQos ? requestedQos : maxQos;
    final filters = _bySession.putIfAbsent(session, () => {});
    filters[filter] = granted;
    return granted;
  }

  /// Removes a subscription. Returns true if the subscription existed.
  bool unsubscribe(MqttSession session, String filter) {
    final filters = _bySession[session];
    if (filters == null) return false;
    final existed = filters.remove(filter) != null;
    if (filters.isEmpty) _bySession.remove(session);
    return existed;
  }

  /// Removes all subscriptions for a session (used on session close when
  /// clean session = true).
  void removeSession(MqttSession session) => _bySession.remove(session);

  /// Returns all `(session, grantedQos)` pairs whose filter matches [topic].
  /// For a given session the highest matching QoS is returned (when multiple
  /// of its filters match the same topic).
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

  /// Returns the set of filters subscribed by [session].
  Map<String, int> filtersOf(MqttSession session) =>
      Map.unmodifiable(_bySession[session] ?? const <String, int>{});

  /// Replaces the entire subscription set for [session] (used on session
  /// resume to restore persisted filters).
  void replaceFiltersFor(MqttSession session, Map<String, int> filters) {
    if (filters.isEmpty) {
      _bySession.remove(session);
    } else {
      _bySession[session] = Map.of(filters);
    }
  }
}

class SubscriptionMatch {
  const SubscriptionMatch(this.session, this.grantedQos);
  final MqttSession session;
  final int grantedQos;
}
