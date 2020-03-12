//
// Copyright (c) 2019 Nathan Fiedler
//
import 'package:equatable/equatable.dart';
import 'package:meta/meta.dart';
import 'package:oxidized/oxidized.dart';

/// Range in time represented as seconds since midnight.
///
/// Start and stop time may be "flipped" to span the midnight hour.
class TimeRange extends Equatable {
  final int start;
  final int stop;

  TimeRange({@required this.start, @required this.stop});

  @override
  List<Object> get props => [start, stop];
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

  /// The snapshot is the hash digest of the most recent snapshot.
  final Option<String> snapshot;

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
  List<Object> get props => [key];
}
