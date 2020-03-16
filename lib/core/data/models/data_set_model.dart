//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:meta/meta.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/data/models/snapshot_model.dart';
import 'package:zorigami/core/domain/entities/data_set.dart';

class TimeRangeModel extends TimeRange {
  TimeRangeModel({
    @required int start,
    @required int stop,
  }) : super(
          start: start,
          stop: stop,
        );

  factory TimeRangeModel.from(TimeRange range) {
    return TimeRangeModel(
      start: range.start,
      stop: range.stop,
    );
  }

  factory TimeRangeModel.fromJson(Map<String, dynamic> json) {
    return TimeRangeModel(
      start: json['startTime'],
      stop: json['stopTime'],
    );
  }

  Map<String, dynamic> toJson() {
    return {
      'startTime': start,
      'stopTime': stop,
    };
  }
}

class ScheduleModel extends Schedule {
  ScheduleModel({
    @required Frequency frequency,
    @required Option<TimeRange> timeRange,
    @required Option<WeekOfMonth> weekOfMonth,
    @required Option<DayOfWeek> dayOfWeek,
    @required Option<int> dayOfMonth,
  }) : super(
          frequency: frequency,
          timeRange: timeRange,
          weekOfMonth: weekOfMonth,
          dayOfWeek: dayOfWeek,
          dayOfMonth: dayOfMonth,
        );

  factory ScheduleModel.from(Schedule schedule) {
    return ScheduleModel(
      frequency: schedule.frequency,
      timeRange: schedule.timeRange,
      weekOfMonth: schedule.weekOfMonth,
      dayOfWeek: schedule.dayOfWeek,
      dayOfMonth: schedule.dayOfMonth,
    );
  }

  factory ScheduleModel.fromJson(Map<String, dynamic> json) {
    final frequency = decodeFrequency(json['frequency']);
    final timeRange = decodeTimeRange(json['timeRange']);
    final weekOfMonth = decodeWeekOfMonth(json['weekOfMonth']);
    final dayOfWeek = decodeDayOfWeek(json['dayOfWeek']);
    final dayOfMonth = decodeDayOfMonth(json['dayOfMonth']);
    return ScheduleModel(
      frequency: frequency,
      timeRange: timeRange,
      weekOfMonth: weekOfMonth,
      dayOfWeek: dayOfWeek,
      dayOfMonth: dayOfMonth,
    );
  }

  Map<String, dynamic> toJson() {
    final frequency = encodeFrequency(this.frequency);
    final timeRange = encodeTimeRange(this.timeRange);
    final weekOfMonth = encodeWeekOfMonth(this.weekOfMonth);
    final dayOfWeek = encodeDayOfWeek(this.dayOfWeek);
    final dayOfMonth = encodeDayOfMonth(this.dayOfMonth);
    return {
      'frequency': frequency,
      'timeRange': timeRange,
      'weekOfMonth': weekOfMonth,
      'dayOfWeek': dayOfWeek,
      'dayOfMonth': dayOfMonth,
    };
  }
}

class DataSetModel extends DataSet {
  DataSetModel({
    @required String key,
    @required String computerId,
    @required String basepath,
    @required List<Schedule> schedules,
    @required int packSize,
    @required List<String> stores,
    @required Option<SnapshotModel> snapshot,
  }) : super(
          key: key,
          computerId: computerId,
          basepath: basepath,
          schedules: schedules,
          packSize: packSize,
          stores: stores,
          snapshot: snapshot,
        );

  factory DataSetModel.from(DataSet dataset) {
    final snapshot = dataset.snapshot.map((e) => SnapshotModel.from(e));
    return DataSetModel(
      key: dataset.key,
      computerId: dataset.computerId,
      basepath: dataset.basepath,
      schedules: dataset.schedules,
      packSize: dataset.packSize,
      stores: dataset.stores,
      snapshot: snapshot,
    );
  }

  factory DataSetModel.fromJson(Map<String, dynamic> json) {
    final List<Schedule> schedules = List.from(
      json['schedules'].map((s) => ScheduleModel.fromJson(s)),
    );
    final snapshot = Option.some(json['snapshot']).map(
      (v) => SnapshotModel.fromJson(v),
    );
    // ensure the stores are of type String (they ought to be)
    final List<String> stores = List.from(
      json['stores'].map((e) => e.toString()),
    );
    return DataSetModel(
      key: json['key'],
      computerId: json['computerId'],
      basepath: json['basepath'],
      schedules: schedules,
      // limiting pack size to 2^53 (in JavaScript) is acceptable
      packSize: int.parse(json['packSize']),
      stores: stores,
      snapshot: snapshot,
    );
  }

  Map<String, dynamic> toJson() {
    final List<Map<String, dynamic>> schedules = List.from(
      this.schedules.map((s) => ScheduleModel.from(s).toJson()),
    );
    return {
      'key': key,
      'computerId': computerId,
      'basepath': basepath,
      'schedules': schedules,
      'packSize': packSize.toString(),
      'stores': stores,
    };
  }
}

Option<TimeRange> decodeTimeRange(Map<String, dynamic> timeRange) {
  if (timeRange == null) {
    return Option.none();
  }
  return Option.some(TimeRangeModel.fromJson(timeRange));
}

Map<String, dynamic> encodeTimeRange(Option<TimeRange> timeRange) {
  return timeRange.mapOr((TimeRange tr) {
    return TimeRangeModel.from(tr).toJson();
  }, null);
}

Frequency decodeFrequency(String frequency) {
  if (frequency == 'HOURLY') {
    return Frequency.hourly;
  } else if (frequency == 'DAILY') {
    return Frequency.daily;
  } else if (frequency == 'WEEKLY') {
    return Frequency.weekly;
  } else if (frequency == 'MONTHLY') {
    return Frequency.monthly;
  } else {
    throw ArgumentError('frequency is not recognized');
  }
}

String encodeFrequency(Frequency frequency) {
  switch (frequency) {
    case Frequency.hourly:
      return 'HOURLY';
    case Frequency.daily:
      return 'DAILY';
    case Frequency.weekly:
      return 'WEEKLY';
    case Frequency.monthly:
      return 'MONTHLY';
    default:
      throw ArgumentError('frequency is not recognized');
  }
}

Option<WeekOfMonth> decodeWeekOfMonth(String weekOfMonth) {
  if (weekOfMonth == null) {
    return Option.none();
  } else if (weekOfMonth == 'FIRST') {
    return Option.some(WeekOfMonth.first);
  } else if (weekOfMonth == 'SECOND') {
    return Option.some(WeekOfMonth.second);
  } else if (weekOfMonth == 'THIRD') {
    return Option.some(WeekOfMonth.third);
  } else if (weekOfMonth == 'FOURTH') {
    return Option.some(WeekOfMonth.fourth);
  } else if (weekOfMonth == 'FIFTH') {
    return Option.some(WeekOfMonth.fifth);
  } else {
    throw ArgumentError('weekOfMonth is not recognized');
  }
}

String encodeWeekOfMonth(Option<WeekOfMonth> weekOfMonth) {
  return weekOfMonth.mapOr((WeekOfMonth wom) {
    switch (wom) {
      case WeekOfMonth.first:
        return 'FIRST';
      case WeekOfMonth.second:
        return 'SECOND';
      case WeekOfMonth.third:
        return 'THIRD';
      case WeekOfMonth.fourth:
        return 'FOURTH';
      case WeekOfMonth.fifth:
        return 'FIFTH';
      default:
        throw ArgumentError('weekOfMonth is not recognized');
    }
  }, null);
}

Option<DayOfWeek> decodeDayOfWeek(String dayOfWeek) {
  if (dayOfWeek == null) {
    return Option.none();
  } else if (dayOfWeek == 'SUN') {
    return Option.some(DayOfWeek.sun);
  } else if (dayOfWeek == 'MON') {
    return Option.some(DayOfWeek.mon);
  } else if (dayOfWeek == 'TUE') {
    return Option.some(DayOfWeek.tue);
  } else if (dayOfWeek == 'WED') {
    return Option.some(DayOfWeek.wed);
  } else if (dayOfWeek == 'THU') {
    return Option.some(DayOfWeek.thu);
  } else if (dayOfWeek == 'FRI') {
    return Option.some(DayOfWeek.fri);
  } else if (dayOfWeek == 'SAT') {
    return Option.some(DayOfWeek.sat);
  } else {
    throw ArgumentError('dayOfWeek is not recognized');
  }
}

String encodeDayOfWeek(Option<DayOfWeek> dayOfWeek) {
  return dayOfWeek.mapOr((DayOfWeek dow) {
    switch (dow) {
      case DayOfWeek.sun:
        return 'SUN';
      case DayOfWeek.mon:
        return 'MON';
      case DayOfWeek.tue:
        return 'TUE';
      case DayOfWeek.wed:
        return 'WED';
      case DayOfWeek.thu:
        return 'THU';
      case DayOfWeek.fri:
        return 'FRI';
      case DayOfWeek.sat:
        return 'SAT';
      default:
        throw ArgumentError('dayOfWeek is not recognized');
    }
  }, null);
}

Option<int> decodeDayOfMonth(dynamic dayOfMonth) {
  if (dayOfMonth == null) {
    return Option.none();
  }
  return Option.some((dayOfMonth as num).toInt());
}

int encodeDayOfMonth(Option<int> dayOfMonth) {
  return dayOfMonth.unwrapOr(null);
}
