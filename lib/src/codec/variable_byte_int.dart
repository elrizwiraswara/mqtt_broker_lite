import 'dart:typed_data';

class VariableByteIntResult {
  const VariableByteIntResult(this.value, this.bytesConsumed);
  final int value;
  final int bytesConsumed;
}

class VariableByteInt {
  static const int maxValue = 268435455;

  /// Decodes a variable byte integer starting at [offset] in [data].
  /// Returns null if more bytes are needed. Throws [FormatException] if malformed.
  static VariableByteIntResult? decode(List<int> data, int offset) {
    int multiplier = 1;
    int value = 0;
    int index = offset;

    while (true) {
      if (index >= data.length) return null; // need more bytes
      final byte = data[index];
      value += (byte & 0x7F) * multiplier;
      if (multiplier > 128 * 128 * 128) {
        throw const FormatException('Malformed variable byte integer');
      }
      multiplier *= 128;
      index++;
      if ((byte & 0x80) == 0) {
        return VariableByteIntResult(value, index - offset);
      }
    }
  }

  static Uint8List encode(int value) {
    if (value < 0 || value > maxValue) {
      throw ArgumentError('Variable byte integer out of range: $value');
    }
    final bytes = <int>[];
    var remaining = value;
    do {
      var digit = remaining % 128;
      remaining = remaining ~/ 128;
      if (remaining > 0) digit |= 0x80;
      bytes.add(digit);
    } while (remaining > 0);
    return Uint8List.fromList(bytes);
  }
}
