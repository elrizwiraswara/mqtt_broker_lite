import 'dart:typed_data';

import 'topic_filter.dart';

class RetainedMessage {
  RetainedMessage(this.topic, this.payload, this.qos);
  final String topic;
  final Uint8List payload;
  final int qos;
}

/// In-memory retained-message store. A retained PUBLISH with an empty payload
/// clears the entry for that topic (per MQTT 3.1.1 §3.3.1.3).
class RetainedStore {
  final Map<String, RetainedMessage> _byTopic = {};

  void store(String topic, Uint8List payload, int qos) {
    if (payload.isEmpty) {
      _byTopic.remove(topic);
    } else {
      _byTopic[topic] = RetainedMessage(topic, payload, qos);
    }
  }

  /// Returns all retained messages whose topic matches [filter].
  List<RetainedMessage> matching(String filter) {
    final out = <RetainedMessage>[];
    _byTopic.forEach((topic, msg) {
      if (TopicFilter.matches(filter, topic)) out.add(msg);
    });
    return out;
  }

  void clear() => _byTopic.clear();
  int get size => _byTopic.length;
}
