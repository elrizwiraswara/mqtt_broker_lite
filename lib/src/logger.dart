enum MqttLogLevel { debug, info, warning, error, none }

abstract class MqttLogger {
  void log(MqttLogLevel level, String message, [Object? error, StackTrace? stack]);

  void debug(String message) => log(MqttLogLevel.debug, message);
  void info(String message) => log(MqttLogLevel.info, message);
  void warning(String message, [Object? error]) => log(MqttLogLevel.warning, message, error);
  void error(String message, [Object? error, StackTrace? stack]) =>
      log(MqttLogLevel.error, message, error, stack);
}

class PrintMqttLogger extends MqttLogger {
  PrintMqttLogger({this.minLevel = MqttLogLevel.info});

  final MqttLogLevel minLevel;

  @override
  void log(MqttLogLevel level, String message, [Object? error, StackTrace? stack]) {
    if (level.index < minLevel.index || minLevel == MqttLogLevel.none) return;
    final prefix = '[MqttBroker][${level.name.toUpperCase()}]';
    if (error != null) {
      print('$prefix $message: $error');
      if (stack != null) print(stack);
    } else {
      print('$prefix $message');
    }
  }
}

class SilentMqttLogger extends MqttLogger {
  @override
  void log(MqttLogLevel level, String message, [Object? error, StackTrace? stack]) {}
}
