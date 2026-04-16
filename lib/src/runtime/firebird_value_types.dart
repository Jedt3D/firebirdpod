import 'package:meta/meta.dart';

@immutable
class FirebirdDecimal {
  const FirebirdDecimal(this.text);

  final String text;

  @override
  String toString() => text;

  @override
  bool operator ==(Object other) {
    return other is FirebirdDecimal && other.text == text;
  }

  @override
  int get hashCode => text.hashCode;
}

@immutable
class FirebirdTimeWithTimeZone {
  const FirebirdTimeWithTimeZone({
    required this.hour,
    required this.minute,
    required this.second,
    this.millisecond = 0,
    this.tenthMillisecond = 0,
    this.timeZoneName = '',
    this.timeZoneOffset = Duration.zero,
  });

  final int hour;
  final int minute;
  final int second;
  final int millisecond;
  final int tenthMillisecond;
  final String timeZoneName;
  final Duration timeZoneOffset;

  @override
  String toString() {
    final fraction =
        '.${millisecond.toString().padLeft(3, '0')}$tenthMillisecond';
    final zone = timeZoneName.isNotEmpty
        ? timeZoneName
        : _formatOffset(timeZoneOffset);
    return '${hour.toString().padLeft(2, '0')}:'
        '${minute.toString().padLeft(2, '0')}:'
        '${second.toString().padLeft(2, '0')}$fraction $zone';
  }
}

@immutable
class FirebirdTimestampWithTimeZone {
  const FirebirdTimestampWithTimeZone({
    required this.year,
    required this.month,
    required this.day,
    required this.hour,
    required this.minute,
    required this.second,
    this.millisecond = 0,
    this.tenthMillisecond = 0,
    this.timeZoneName = '',
    this.timeZoneOffset = Duration.zero,
  });

  final int year;
  final int month;
  final int day;
  final int hour;
  final int minute;
  final int second;
  final int millisecond;
  final int tenthMillisecond;
  final String timeZoneName;
  final Duration timeZoneOffset;

  @override
  String toString() {
    final fraction =
        '.${millisecond.toString().padLeft(3, '0')}$tenthMillisecond';
    final zone = timeZoneName.isNotEmpty
        ? timeZoneName
        : _formatOffset(timeZoneOffset);
    return '$year-${month.toString().padLeft(2, '0')}-'
        '${day.toString().padLeft(2, '0')} '
        '${hour.toString().padLeft(2, '0')}:'
        '${minute.toString().padLeft(2, '0')}:'
        '${second.toString().padLeft(2, '0')}$fraction $zone';
  }
}

String _formatOffset(Duration offset) {
  final sign = offset.isNegative ? '-' : '+';
  final totalMinutes = offset.inMinutes.abs();
  final hours = totalMinutes ~/ 60;
  final minutes = totalMinutes % 60;
  return '$sign${hours.toString().padLeft(2, '0')}:'
      '${minutes.toString().padLeft(2, '0')}';
}
