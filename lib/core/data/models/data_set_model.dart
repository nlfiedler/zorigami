//
// Copyright (c) 2024 Nathan Fiedler
//
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/data/models/snapshot_model.dart';
import 'package:zorigami/core/domain/entities/data_set.dart';

class TimeRangeModel extends TimeRange {
  const TimeRangeModel({
    required super.start,
    required super.stop,
  });

  factory TimeRangeModel.from(TimeRange range) {
    return TimeRangeModel(
      start: range.start,
      stop: range.stop,
    );
  }

  factory TimeRangeModel.fromJson(Map<String, dynamic> json) {
    final localStart = convertToLocal(json['startTime'] as int);
    final localStop = convertToLocal(json['stopTime'] as int);
    return TimeRangeModel(
      start: localStart,
      stop: localStop,
    );
  }

  Map<String, dynamic> toJson() {
    final utcStart = convertToUtc(start);
    final utcStop = convertToUtc(stop);
    return {
      'startTime': utcStart,
      'stopTime': utcStop,
    };
  }
}

int convertToLocal(int inputSeconds) {
  // create a local time to get the year/month/day
  final nowLocal = DateTime.now();
  final hour = (inputSeconds / 3600).truncate();
  final minute = ((inputSeconds % 3600) / 60).truncate();
  final seconds = (inputSeconds % 60).truncate();
  final timeUtc = DateTime.utc(
    nowLocal.year,
    nowLocal.month,
    nowLocal.day,
    hour,
    minute,
    seconds,
  );
  final timeLocal = timeUtc.toLocal();
  return timeLocal.hour * 3600 + timeLocal.minute * 60 + timeLocal.second;
}

int convertToUtc(int inputSeconds) {
  // create a local time to get the year/month/day
  final nowLocal = DateTime.now();
  final hour = (inputSeconds / 3600).truncate();
  final minute = ((inputSeconds % 3600) / 60).truncate();
  final seconds = (inputSeconds % 60).truncate();
  final timeLocal = DateTime(
    nowLocal.year,
    nowLocal.month,
    nowLocal.day,
    hour,
    minute,
    seconds,
  );
  final timeUtc = timeLocal.toUtc();
  return timeUtc.hour * 3600 + timeUtc.minute * 60 + timeUtc.second;
}

class ScheduleModel extends Schedule {
  const ScheduleModel({
    required super.frequency,
    required super.timeRange,
    required super.weekOfMonth,
    required super.dayOfWeek,
    required super.dayOfMonth,
  });

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

class BackupStateModel extends BackupState {
  const BackupStateModel({
    required super.paused,
    required super.stopRequested,
    required super.changedFiles,
    required super.packsUploaded,
    required super.filesUploaded,
    required super.bytesUploaded,
  });

  factory BackupStateModel.from(BackupState state) {
    return BackupStateModel(
      paused: state.paused,
      stopRequested: state.stopRequested,
      changedFiles: state.changedFiles,
      packsUploaded: state.packsUploaded,
      filesUploaded: state.filesUploaded,
      bytesUploaded: state.bytesUploaded,
    );
  }

  factory BackupStateModel.fromJson(Map<String, dynamic> json) {
    return BackupStateModel(
      paused: json['paused'],
      stopRequested: json['stopRequested'],
      changedFiles: int.parse(json['changedFiles']),
      packsUploaded: int.parse(json['packsUploaded']),
      filesUploaded: int.parse(json['filesUploaded']),
      bytesUploaded: int.parse(json['bytesUploaded']),
    );
  }

  Map<String, dynamic> toJson() {
    return {
      'paused': paused,
      'stopRequested': stopRequested,
      'changedFiles': changedFiles.toString(),
      'packsUploaded': packsUploaded.toString(),
      'filesUploaded': filesUploaded.toString(),
      'bytesUploaded': bytesUploaded.toString(),
    };
  }
}

class DataSetModel extends DataSet {
  const DataSetModel({
    required super.key,
    required super.computerId,
    required super.basepath,
    required List<ScheduleModel> super.schedules,
    required super.packSize,
    required super.stores,
    required super.excludes,
    required Option<SnapshotModel> super.snapshot,
    required super.status,
    required Option<BackupStateModel> super.backupState,
    required super.errorMsg,
  });

  factory DataSetModel.from(DataSet dataset) {
    final List<ScheduleModel> schedules = List.from(
      dataset.schedules.map((s) => ScheduleModel.from(s)),
    );
    final snapshot = dataset.snapshot.map((e) => SnapshotModel.from(e));
    final state = dataset.backupState.map((e) => BackupStateModel.from(e));
    return DataSetModel(
      key: dataset.key,
      computerId: dataset.computerId,
      basepath: dataset.basepath,
      schedules: schedules,
      packSize: dataset.packSize,
      stores: dataset.stores,
      excludes: dataset.excludes,
      snapshot: snapshot,
      status: dataset.status,
      backupState: state,
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
    // ensure the excludes are of type String (they ought to be)
    final List<String> excludes = json['excludes'] == null
        ? []
        : List.from(
            json['excludes'].map((e) => e.toString()),
          );
    final backupState = Option.from(json['backupState']).map(
      (v) => BackupStateModel.fromJson(v as Map<String, dynamic>),
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
      excludes: excludes,
      snapshot: snapshot,
      status: decodeStatus(json['status']),
      backupState: backupState,
      errorMsg: Option.from(json['errorMessage']),
    );
  }

  Map<String, dynamic> toJson({bool input = false}) {
    final List<Map<String, dynamic>> schedules = List.from(
      this.schedules.map((s) => ScheduleModel.from(s).toJson()),
    );
    final Option<Map<String, dynamic>> backupState =
        this.backupState.map((s) => BackupStateModel.from(s).toJson());
    final Map<String, dynamic> result = {
      'id': key,
      'basepath': basepath,
      'schedules': schedules,
      'packSize': packSize.toString(),
      'stores': stores,
      'excludes': excludes,
    };
    if (!input) {
      result['computerId'] = computerId;
      result['status'] = encodeStatus(status);
      result['backupState'] = backupState.toNullable();
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
  }
}

Option<TimeRange> decodeTimeRange(Map<String, dynamic>? timeRange) {
  // Will get a non-null timeRange with unit tests that need to have a full
  // GraphQL structure to make the graphql package happy.
  if (timeRange == null || timeRange['startTime'] == null) {
    return const None();
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
  }
}

Option<WeekOfMonth> decodeWeekOfMonth(String? weekOfMonth) {
  if (weekOfMonth == null) {
    return const None();
  } else if (weekOfMonth == 'FIRST') {
    return const Some(WeekOfMonth.first);
  } else if (weekOfMonth == 'SECOND') {
    return const Some(WeekOfMonth.second);
  } else if (weekOfMonth == 'THIRD') {
    return const Some(WeekOfMonth.third);
  } else if (weekOfMonth == 'FOURTH') {
    return const Some(WeekOfMonth.fourth);
  } else if (weekOfMonth == 'FIFTH') {
    return const Some(WeekOfMonth.fifth);
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
    }
  }, null);
}

Option<DayOfWeek> decodeDayOfWeek(String? dayOfWeek) {
  if (dayOfWeek == null) {
    return const None();
  } else if (dayOfWeek == 'SUN') {
    return const Some(DayOfWeek.sun);
  } else if (dayOfWeek == 'MON') {
    return const Some(DayOfWeek.mon);
  } else if (dayOfWeek == 'TUE') {
    return const Some(DayOfWeek.tue);
  } else if (dayOfWeek == 'WED') {
    return const Some(DayOfWeek.wed);
  } else if (dayOfWeek == 'THU') {
    return const Some(DayOfWeek.thu);
  } else if (dayOfWeek == 'FRI') {
    return const Some(DayOfWeek.fri);
  } else if (dayOfWeek == 'SAT') {
    return const Some(DayOfWeek.sat);
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
    }
  }, null);
}

Option<int> decodeDayOfMonth(dynamic dayOfMonth) {
  if (dayOfMonth == null) {
    return const None();
  }
  return Option.some((dayOfMonth as num).toInt());
}

int? encodeDayOfMonth(Option<int> dayOfMonth) {
  return dayOfMonth.toNullable();
}
