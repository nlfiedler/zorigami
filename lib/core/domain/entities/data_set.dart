//
// Copyright (c) 2024 Nathan Fiedler
//
import 'package:equatable/equatable.dart';
import 'package:intl/intl.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/snapshot.dart';
import 'package:zorigami/core/error/failures.dart';

/// Range in time represented as seconds since midnight.
///
/// Start and stop time may be "flipped" to span the midnight hour.
class TimeRange extends Equatable {
  final int start;
  final int stop;

  const TimeRange({required this.start, required this.stop});

  @override
  List<Object> get props => [start, stop];

  @override
  bool get stringify => true;

  String toPrettyString() {
    return 'from ${formatTime(start)} to ${formatTime(stop)}';
  }

  Result<dynamic, Failure> validate() {
    if (start < 0 || start > 86400) {
      return const Err(
        ValidationFailure('Start time must be between 0 and 86,400'),
      );
    }
    if (stop < 0 || stop > 86400) {
      return const Err(
        ValidationFailure('Stop time must be between 0 and 86,400'),
      );
    }
    return const Ok(0);
  }
}

enum Frequency { hourly, daily, weekly, monthly }

enum WeekOfMonth { first, second, third, fourth, fifth }

enum DayOfWeek { sun, mon, tue, wed, thu, fri, sat }

class Schedule extends Equatable {
  final Frequency frequency;
  final Option<TimeRange> timeRange;
  final Option<WeekOfMonth> weekOfMonth;
  final Option<DayOfWeek> dayOfWeek;
  final Option<int> dayOfMonth;

  const Schedule({
    required this.frequency,
    required this.timeRange,
    required this.weekOfMonth,
    required this.dayOfWeek,
    required this.dayOfMonth,
  });

  @override
  List<Object> get props => [
        frequency,
        timeRange,
        weekOfMonth,
        dayOfWeek,
        dayOfMonth,
      ];

  @override
  bool get stringify => true;

  String toPrettyString() {
    final buffer = StringBuffer();
    buffer.write(prettyFrequency(frequency));
    if (dayOfMonth is Some) {
      buffer.write(' on day ${dayOfMonth.unwrap()}');
    } else if (weekOfMonth is Some) {
      buffer.write(' on the ');
      buffer.write(prettyWeekOfMonth(weekOfMonth.unwrap()));
      buffer.write(' ${prettyDayOfWeek(dayOfWeek.unwrap())}');
    } else if (dayOfWeek is Some) {
      buffer.write(' on ${prettyDayOfWeek(dayOfWeek.unwrap())}');
    }
    if (timeRange is Some) {
      buffer.write(' ');
      buffer.write(timeRange.unwrap().toPrettyString());
    }
    return buffer.toString();
  }

  Result<dynamic, Failure> validate() {
    switch (frequency) {
      case Frequency.hourly:
        if (weekOfMonth is Some ||
            dayOfWeek is Some ||
            dayOfMonth is Some ||
            timeRange is Some) {
          return const Err(
            ValidationFailure('Hourly cannot take any range or days'),
          );
        }
        break;
      case Frequency.daily:
        if (weekOfMonth is Some || dayOfWeek is Some || dayOfMonth is Some) {
          return const Err(
            ValidationFailure('Daily can only take a time range'),
          );
        }
        if (timeRange is Some) {
          return timeRange.unwrap().validate();
        }
        break;
      case Frequency.weekly:
        if (weekOfMonth is Some || dayOfMonth is Some) {
          return const Err(
            ValidationFailure(
              'Weekly can only take a time range and day-of-week',
            ),
          );
        }
        if (timeRange is Some) {
          return timeRange.unwrap().validate();
        }
        break;
      case Frequency.monthly:
        if (dayOfMonth is Some && dayOfWeek is Some) {
          return const Err(
            ValidationFailure(
              'Monthly can only take either day-of-month or day-of-week',
            ),
          );
        }
        if (dayOfWeek is Some && weekOfMonth is None) {
          return const Err(
            ValidationFailure(
              'Monthly requires week-of-month when using day-of-week',
            ),
          );
        }
        if (timeRange is Some) {
          return timeRange.unwrap().validate();
        }
        break;
    }
    return const Ok(0);
  }
}

class BackupState extends Equatable {
  final bool paused;
  final bool stopRequested;
  final int changedFiles;
  final int packsUploaded;
  final int filesUploaded;
  final int bytesUploaded;

  const BackupState({
    required this.paused,
    required this.stopRequested,
    required this.changedFiles,
    required this.packsUploaded,
    required this.filesUploaded,
    required this.bytesUploaded,
  });

  @override
  List<Object> get props => [changedFiles];

  @override
  bool get stringify => true;

  String describeState() {
    if (paused) {
      return 'paused';
    } else if (stopRequested) {
      return 'stop requested';
    } else if (changedFiles == 0) {
      return 'scanning';
    } else {
      final f = NumberFormat();
      final u = f.format(filesUploaded);
      final c = f.format(changedFiles);
      final b = f.format(bytesUploaded);
      return '$u of $c files, $b bytes uploaded';
    }
  }
}

enum Status { none, running, finished, paused, failed }

/// A `DataSet` may have zero or more schedules.
///
/// With no [schedules], the data set is backed up manually by the user.
class DataSet extends Equatable {
  final String key;
  final String computerId;
  final String basepath;
  final List<Schedule> schedules;
  final int packSize;
  final List<String> stores;
  final List<String> excludes;
  final Option<Snapshot> snapshot;
  final Status status;
  final Option<BackupState> backupState;
  final Option<String> errorMsg;

  const DataSet({
    required this.key,
    required this.computerId,
    required this.basepath,
    required this.schedules,
    required this.packSize,
    required this.stores,
    required this.excludes,
    required this.snapshot,
    required this.status,
    required this.backupState,
    required this.errorMsg,
  });

  @override
  List<Object> get props => [key, computerId, basepath];

  @override
  bool get stringify => true;

  Result<dynamic, Failure> validate() {
    if (stores.isEmpty) {
      return const Err(
        ValidationFailure(
          'Data set must have at least one pack store',
        ),
      );
    }
    for (final schedule in schedules) {
      final result = schedule.validate();
      if (result is Err) {
        return result;
      }
    }
    return const Ok(0);
  }

  String describeStatus() {
    switch (status) {
      case Status.none:
        if (isFinished()) {
          return finishedLabel();
        }
        return 'not yet run';
      case Status.running:
        return backupState.match((s) => s.describeState(), () => 'in progress');
      case Status.finished:
        return finishedLabel();
      case Status.paused:
        return 'paused';
      case Status.failed:
        return 'error: ${errorMsg.unwrapOr("unknown")}';
    }
  }

  String finishedLabel() {
    final suffix = snapshot.mapOrElse(
      (s) => s.endTime.mapOrElse(
        (e) => ' at ${DateFormat.yMd().add_jm().format(e.toLocal())}',
        () => '',
      ),
      () => '',
    );
    return 'finished$suffix';
  }

  bool isFinished() {
    return snapshot.mapOrElse(
      (s) => s.endTime.mapOrElse(
        (e) => true,
        () => false,
      ),
      () => false,
    );
  }
}

// Format the seconds-since-midnight value as hour:minute format, with leading
// zeros (e.g. 12:01, 04:30).
String formatTime(int seconds) {
  if (seconds == 0 || seconds == 86400) {
    return '12:00 AM';
  }
  final hours = seconds ~/ 3600;
  final suffix = hours >= 12 ? 'PM' : 'AM';
  final hour = (hours > 12 ? hours % 12 : hours).toString();
  final minute = ((seconds % 3600) ~/ 60).toString().padLeft(2, '0');
  return '$hour:$minute $suffix';
}

String prettyFrequency(Frequency frequency) {
  switch (frequency) {
    case Frequency.hourly:
      return 'hourly';
    case Frequency.daily:
      return 'daily';
    case Frequency.weekly:
      return 'weekly';
    case Frequency.monthly:
      return 'monthly';
  }
}

String prettyDayOfWeek(DayOfWeek dow) {
  switch (dow) {
    case DayOfWeek.sun:
      return 'Sunday';
    case DayOfWeek.mon:
      return 'Monday';
    case DayOfWeek.tue:
      return 'Tuesday';
    case DayOfWeek.wed:
      return 'Wednesday';
    case DayOfWeek.thu:
      return 'Thursday';
    case DayOfWeek.fri:
      return 'Friday';
    case DayOfWeek.sat:
      return 'Saturday';
  }
}

String prettyWeekOfMonth(WeekOfMonth wom) {
  switch (wom) {
    case WeekOfMonth.first:
      return 'first';
    case WeekOfMonth.second:
      return 'second';
    case WeekOfMonth.third:
      return 'third';
    case WeekOfMonth.fourth:
      return 'fourth';
    case WeekOfMonth.fifth:
      return 'fifth';
  }
}
