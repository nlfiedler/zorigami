//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:equatable/equatable.dart';
import 'package:meta/meta.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/snapshot.dart';
import 'package:zorigami/core/error/failures.dart';

/// Range in time represented as seconds since midnight.
///
/// Start and stop time may be "flipped" to span the midnight hour.
class TimeRange extends Equatable {
  final int start;
  final int stop;

  TimeRange({@required this.start, @required this.stop});

  @override
  List<Object> get props => [start, stop];

  @override
  bool get stringify => true;

  Result<dynamic, Failure> validate() {
    if (start < 0 || start > 86400) {
      return Result.err(
        ValidationFailure('Start time must be between 0 and 86,400'),
      );
    }
    if (stop < 0 || stop > 86400) {
      return Result.err(
        ValidationFailure('Stop time must be between 0 and 86,400'),
      );
    }
    return Result.ok(0);
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

  Schedule({
    @required this.frequency,
    @required this.timeRange,
    @required this.weekOfMonth,
    @required this.dayOfWeek,
    @required this.dayOfMonth,
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

  Result<dynamic, Failure> validate() {
    switch (frequency) {
      case Frequency.hourly:
        if (weekOfMonth.isSome ||
            dayOfWeek.isSome ||
            dayOfMonth.isSome ||
            timeRange.isSome) {
          return Result.err(
            ValidationFailure('Hourly cannot take any range or days'),
          );
        }
        break;
      case Frequency.daily:
        if (weekOfMonth.isSome || dayOfWeek.isSome || dayOfMonth.isSome) {
          return Result.err(
            ValidationFailure('Daily can only take a time range'),
          );
        }
        if (timeRange.isSome) {
          return timeRange.unwrap().validate();
        }
        break;
      case Frequency.weekly:
        if (weekOfMonth.isSome || dayOfMonth.isSome) {
          return Result.err(
            ValidationFailure(
              'Weekly can only take a time range and day-of-week',
            ),
          );
        }
        if (timeRange.isSome) {
          return timeRange.unwrap().validate();
        }
        break;
      case Frequency.monthly:
        if (dayOfMonth.isSome && dayOfWeek.isSome) {
          return Result.err(
            ValidationFailure(
              'Monthly can only take either day-of-month or day-of-week',
            ),
          );
        }
        if (dayOfWeek.isSome && weekOfMonth.isNone) {
          return Result.err(
            ValidationFailure(
              'Monthly requires week-of-month when using day-of-week',
            ),
          );
        }
        if (timeRange.isSome) {
          return timeRange.unwrap().validate();
        }
        break;
      default:
        throw ArgumentError('frequency is not recognized');
    }
    return Result.ok(0);
  }
}

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
  final Option<Snapshot> snapshot;

  DataSet({
    @required this.key,
    @required this.computerId,
    @required this.basepath,
    @required this.schedules,
    @required this.packSize,
    @required this.stores,
    @required this.snapshot,
  });

  @override
  List<Object> get props => [key, computerId, basepath];

  @override
  bool get stringify => true;

  Result<dynamic, Failure> validate() {
    if (stores.isEmpty) {
      return Result.err(
        ValidationFailure(
          'Data set must have at least one pack store',
        ),
      );
    }
    for (final schedule in schedules) {
      final result = schedule.validate();
      if (result.isErr) {
        return result;
      }
    }
    return Result.ok(0);
  }
}
