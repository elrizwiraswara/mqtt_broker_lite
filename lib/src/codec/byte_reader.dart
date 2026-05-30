import 'dart:convert';
import 'dart:typed_data';

class ByteReader {
  ByteReader(this._data) : _offset = 0;

  final Uint8List _data;
  int _offset;

  int get offset => _offset;
  int get remaining => _data.length - _offset;
  bool get isAtEnd => _offset >= _data.length;

  int readUint8() {
    _requireBytes(1);

    return _data[_offset++];
  }

  int readUint16() {
    _requireBytes(2);

    final value = (_data[_offset] << 8) | _data[_offset + 1];
    _offset += 2;

    return value;
  }

  Uint8List readBytes(int length) {
    _requireBytes(length);

    final slice = Uint8List.sublistView(_data, _offset, _offset + length);
    _offset += length;

    return Uint8List.fromList(slice);
  }

  String readString() {
    final length = readUint16();
    _requireBytes(length);

    final bytes = Uint8List.sublistView(_data, _offset, _offset + length);
    _offset += length;

    return utf8.decode(bytes);
  }

  Uint8List readBinary() {
    final length = readUint16();

    return readBytes(length);
  }

  Uint8List readRemaining() {
    final slice = Uint8List.sublistView(_data, _offset, _data.length);
    _offset = _data.length;

    return Uint8List.fromList(slice);
  }

  void _requireBytes(int n) {
    if (_offset + n > _data.length) {
      throw const FormatException('Unexpected end of MQTT packet');
    }
  }
}

class ByteWriter {
  final BytesBuilder _buf = BytesBuilder(copy: false);

  int get length => _buf.length;

  void writeUint8(int value) => _buf.addByte(value & 0xFF);

  void writeUint16(int value) {
    _buf.addByte((value >> 8) & 0xFF);
    _buf.addByte(value & 0xFF);
  }

  void writeBytes(List<int> bytes) => _buf.add(bytes);

  void writeString(String value) {
    final bytes = utf8.encode(value);

    if (bytes.length > 65535) {
      throw ArgumentError('MQTT string exceeds 65535 bytes');
    }

    writeUint16(bytes.length);
    _buf.add(bytes);
  }

  void writeBinary(List<int> bytes) {
    if (bytes.length > 65535) {
      throw ArgumentError('MQTT binary field exceeds 65535 bytes');
    }

    writeUint16(bytes.length);
    _buf.add(bytes);
  }

  Uint8List takeBytes() => _buf.takeBytes();
}
