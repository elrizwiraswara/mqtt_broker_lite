import 'package:dart_mqtt_broker/src/codec/variable_byte_int.dart';
import 'package:test/test.dart';

void main() {
  group('VariableByteInt', () {
    test('encodes spec boundary values', () {
      expect(VariableByteInt.encode(0), [0x00]);
      expect(VariableByteInt.encode(127), [0x7F]);
      expect(VariableByteInt.encode(128), [0x80, 0x01]);
      expect(VariableByteInt.encode(16383), [0xFF, 0x7F]);
      expect(VariableByteInt.encode(16384), [0x80, 0x80, 0x01]);
      expect(VariableByteInt.encode(2097151), [0xFF, 0xFF, 0x7F]);
      expect(VariableByteInt.encode(2097152), [0x80, 0x80, 0x80, 0x01]);
      expect(VariableByteInt.encode(268435455), [0xFF, 0xFF, 0xFF, 0x7F]);
    });

    test('round-trips a sample of values', () {
      for (final v in [0, 1, 127, 128, 16383, 16384, 2097151, 2097152, 268435455]) {
        final bytes = VariableByteInt.encode(v);
        final decoded = VariableByteInt.decode(bytes, 0);
        expect(decoded, isNotNull);
        expect(decoded!.value, v, reason: 'value=$v');
        expect(decoded.bytesConsumed, bytes.length);
      }
    });

    test('returns null when more bytes are needed', () {
      expect(VariableByteInt.decode([0x80], 0), isNull);
      expect(VariableByteInt.decode([0x80, 0x80], 0), isNull);
    });

    test('rejects out-of-range values', () {
      expect(() => VariableByteInt.encode(-1), throwsArgumentError);
      expect(() => VariableByteInt.encode(268435456), throwsArgumentError);
    });

    test('rejects malformed five-byte length', () {
      expect(
        () => VariableByteInt.decode([0xFF, 0xFF, 0xFF, 0xFF, 0x7F], 0),
        throwsA(isA<FormatException>()),
      );
    });
  });
}
