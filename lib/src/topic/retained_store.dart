import 'dart:typed_data';

import 'topic_filter.dart';

class RetainedMessage {
  RetainedMessage(this.topic, this.payload, this.qos);

  final String topic;
  final Uint8List payload;
  final int qos;
}

class RetainedStore {
  final Map<String, RetainedMessage> _byTopic = {};

  int get size => _byTopic.length;

  /// MQTT 3.1.1 section 3.3.1.3: empty payload clears the retained entry.
  void store(String topic, Uint8List payload, int qos) {
    if (payload.isEmpty) {
      _byTopic.remove(topic);
    } else {
      _byTopic[topic] = RetainedMessage(topic, payload, qos);
    }
  }

  List<RetainedMessage> matching(String filter) {
    final out = <RetainedMessage>[];

    _byTopic.forEach((topic, msg) {
      if (TopicFilter.matches(filter, topic)) out.add(msg);
    });

    return out;
  }

  void clear() => _byTopic.clear();
}
