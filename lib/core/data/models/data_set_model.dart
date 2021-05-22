//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/data/models/snapshot_model.dart';
import 'package:zorigami/core/domain/entities/data_set.dart';

class TimeRangeModel extends TimeRange {
  TimeRangeModel({
    required int start,
    required int stop,
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
    required Frequency frequency,
    required Option<TimeRange> timeRange,
    required Option<WeekOfMonth> weekOfMonth,
    required Option<DayOfWeek> dayOfWeek,
    required Option<int> dayOfMonth,
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
    required String key,
    required String computerId,
    required String basepath,
    required List<ScheduleModel> schedules,
    required int packSize,
    required List<String> stores,
    required Option<SnapshotModel> snapshot,
    required Status status,
    required Option<String> errorMsg,
  }) : super(
          key: key,
          computerId: computerId,
          basepath: basepath,
          schedules: schedules,
          packSize: packSize,
          stores: stores,
          snapshot: snapshot,
          status: status,
          errorMsg: errorMsg,
        );

  factory DataSetModel.from(DataSet dataset) {
    final List<ScheduleModel> schedules = List.from(
      dataset.schedules.map((s) => ScheduleModel.from(s)),
    );
    final snapshot = dataset.snapshot.map((e) => SnapshotModel.from(e));
    return DataSetModel(
      key: dataset.key,
      computerId: dataset.computerId,
      basepath: dataset.basepath,
      schedules: schedules,
      packSize: dataset.packSize,
      stores: dataset.stores,
      snapshot: snapshot,
      status: dataset.status,
      errorMsg: dataset.errorMsg,
    );
  }

  factory DataSetModel.fromJson(Map<String, dynamic> json) {
    final List<ScheduleModel> schedules = List.from(
      json['schedules'].map((s) => ScheduleModel.fromJson(s)),
    );
    final snapshot = Option.from(json['latestSnapshot']).map(
      (v) => SnapshotModel.fromJson(v as Map<String, dynamic>),
    );
    // ensure the stores are of type String (they ought to be)
    final List<String> stores = List.from(
      json['stores'].map((e) => e.toString()),
    );
    // note that computerId is optional, but we will ignore that for now
    return DataSetModel(
      key: json['id'],
      computerId: json['computerId'],
      basepath: json['basepath'],
      schedules: schedules,
      // limiting pack size to 2^53 (in JavaScript) is acceptable
      packSize: int.parse(json['packSize']),
      stores: stores,
      snapshot: snapshot,
      status: decodeStatus(json['status']),
      errorMsg: Option.from(json['errorMessage']),
    );
  }

  Map<String, dynamic> toJson({bool input = false}) {
    final List<Map<String, dynamic>> schedules = List.from(
      this.schedules.map((s) => ScheduleModel.from(s).toJson()),
    );
    final result = {
      'id': key,
      'basepath': basepath,
      'schedules': schedules,
      'packSize': packSize.toString(),
      'stores': stores,
    };
    if (!input) {
      result['computerId'] = computerId;
      result['status'] = encodeStatus(status);
    }
    return result;
  }
}

Status decodeStatus(String? status) {
  if (status == 'NONE' || status == null) {
    return Status.none;
  } else if (status == 'RUNNING') {
    return Status.running;
  } else if (status == 'FINISHED') {
    return Status.finished;
  } else if (status == 'PAUSED') {
    return Status.paused;
  } else if (status == 'FAILED') {
    return Status.failed;
  } else {
    throw ArgumentError('status is not recognized');
  }
}

String encodeStatus(Status status) {
  switch (status) {
    case Status.none:
      return 'NONE';
    case Status.running:
      return 'RUNNING';
    case Status.finished:
      return 'FINISHED';
    case Status.paused:
      return 'PAUSED';
    case Status.failed:
      return 'FAILED';
    default:
      throw ArgumentError('status is not recognized');
  }
}

Option<TimeRange> decodeTimeRange(Map<String, dynamic>? timeRange) {
  // Will get a non-null timeRange with unit tests that need to have a full
  // GraphQL structure to make the graphql package happy.
  if (timeRange == null || timeRange['startTime'] == null) {
    return None();
  }
  return Option.some(TimeRangeModel.fromJson(timeRange));
}

Map<String, dynamic>? encodeTimeRange(Option<TimeRange> timeRange) {
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

Option<WeekOfMonth> decodeWeekOfMonth(String? weekOfMonth) {
  if (weekOfMonth == null) {
    return None();
  } else if (weekOfMonth == 'FIRST') {
    return Some(WeekOfMonth.first);
  } else if (weekOfMonth == 'SECOND') {
    return Some(WeekOfMonth.second);
  } else if (weekOfMonth == 'THIRD') {
    return Some(WeekOfMonth.third);
  } else if (weekOfMonth == 'FOURTH') {
    return Some(WeekOfMonth.fourth);
  } else if (weekOfMonth == 'FIFTH') {
    return Some(WeekOfMonth.fifth);
  } else {
    throw ArgumentError('weekOfMonth is not recognized');
  }
}

String? encodeWeekOfMonth(Option<WeekOfMonth> weekOfMonth) {
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

Option<DayOfWeek> decodeDayOfWeek(String? dayOfWeek) {
  if (dayOfWeek == null) {
    return None();
  } else if (dayOfWeek == 'SUN') {
    return Some(DayOfWeek.sun);
  } else if (dayOfWeek == 'MON') {
    return Some(DayOfWeek.mon);
  } else if (dayOfWeek == 'TUE') {
    return Some(DayOfWeek.tue);
  } else if (dayOfWeek == 'WED') {
    return Some(DayOfWeek.wed);
  } else if (dayOfWeek == 'THU') {
    return Some(DayOfWeek.thu);
  } else if (dayOfWeek == 'FRI') {
    return Some(DayOfWeek.fri);
  } else if (dayOfWeek == 'SAT') {
    return Some(DayOfWeek.sat);
  } else {
    throw ArgumentError('dayOfWeek is not recognized');
  }
}

String? encodeDayOfWeek(Option<DayOfWeek> dayOfWeek) {
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
    return None();
  }
  return Option.some((dayOfMonth as num).toInt());
}

int? encodeDayOfMonth(Option<int> dayOfMonth) {
  return dayOfMonth.toNullable();
}
