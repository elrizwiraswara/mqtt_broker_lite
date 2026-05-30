import 'package:dart_mqtt_broker/src/topic/topic_filter.dart';
import 'package:test/test.dart';

void main() {
  group('TopicFilter.matches single-level (+)', () {
    test('+ matches exactly one level', () {
      expect(TopicFilter.matches('sport/+/player1', 'sport/tennis/player1'), isTrue);
      expect(TopicFilter.matches('sport/+/player1', 'sport/player1'), isFalse);
      expect(TopicFilter.matches('sport/+/player1', 'sport/tennis/extra/player1'), isFalse);
    });

    test('+ at root', () {
      expect(TopicFilter.matches('+', 'foo'), isTrue);
      expect(TopicFilter.matches('+', 'foo/bar'), isFalse);
      expect(TopicFilter.matches('+/+', 'foo/bar'), isTrue);
    });

    test('empty level is matched by +', () {
      expect(TopicFilter.matches('+/+', 'foo/'), isTrue);
    });
  });

  group('TopicFilter.matches multi-level (#)', () {
    test('# matches zero or more remaining levels', () {
      expect(TopicFilter.matches('sport/#', 'sport'), isTrue);
      expect(TopicFilter.matches('sport/#', 'sport/tennis'), isTrue);
      expect(TopicFilter.matches('sport/#', 'sport/tennis/player1'), isTrue);
      expect(TopicFilter.matches('sport/#', 'sports'), isFalse);
    });

    test('# alone matches everything', () {
      expect(TopicFilter.matches('#', 'a/b/c'), isTrue);
      expect(TopicFilter.matches('#', ''), isTrue);
    });
  });

  group(r'TopicFilter.matches $-topics', () {
    test('# at root does not match \$SYS', () {
      expect(TopicFilter.matches('#', r'$SYS/clients'), isFalse);
    });

    test('+ at root does not match \$SYS', () {
      expect(TopicFilter.matches('+/clients', r'$SYS/clients'), isFalse);
    });

    test('explicit \$SYS prefix matches', () {
      expect(TopicFilter.matches(r'$SYS/#', r'$SYS/clients/count'), isTrue);
    });
  });

  group('TopicFilter.isValidPublishTopic', () {
    test('rejects wildcards', () {
      expect(TopicFilter.isValidPublishTopic('a/+/b'), isFalse);
      expect(TopicFilter.isValidPublishTopic('a/#'), isFalse);
    });

    test('accepts plain topic', () {
      expect(TopicFilter.isValidPublishTopic('a/b/c'), isTrue);
    });

    test('rejects empty', () {
      expect(TopicFilter.isValidPublishTopic(''), isFalse);
    });
  });

  group('TopicFilter.isValidFilter', () {
    test('accepts +, # in valid positions', () {
      expect(TopicFilter.isValidFilter('a/+/b'), isTrue);
      expect(TopicFilter.isValidFilter('a/#'), isTrue);
      expect(TopicFilter.isValidFilter('#'), isTrue);
    });

    test('rejects # not at end', () {
      expect(TopicFilter.isValidFilter('a/#/b'), isFalse);
    });

    test('rejects mixed-content wildcard levels', () {
      expect(TopicFilter.isValidFilter('a/b+/c'), isFalse);
      expect(TopicFilter.isValidFilter('a/b#'), isFalse);
    });
  });
}
