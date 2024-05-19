//
// Copyright (c) 2024 Nathan Fiedler
//
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/data/models/data_set_model.dart';
import 'package:zorigami/core/data/models/snapshot_model.dart';
import 'package:zorigami/core/domain/entities/data_set.dart';
import 'package:zorigami/core/domain/entities/snapshot.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  group('TimeRangeModel', () {
    // pathological case that amounts to only mere seconds
    const tTimeRangeOneTwo = TimeRangeModel(start: 1, stop: 2);
    // special case, midnight to noon
    const tTimeRangeMidnight = TimeRangeModel(start: 0, stop: 43200);
    // range with hours and minutes: 10:30am to 6:30pm
    const tTimeRangeMinutes = TimeRangeModel(start: 37800, stop: 66600);
    // range with hours, minutes, and seconds: 8:45:25am to 12:30:36pm
    const tTimeRangeSeconds = TimeRangeModel(start: 31525, stop: 45036);
    // start time is greater than stop time
    const tTimeRangeOvernight = TimeRangeModel(start: 79200, stop: 21600);
    test(
      'should be a subclass of TimeRange entity',
      () {
        // assert
        expect(tTimeRangeOneTwo, isA<TimeRange>());
      },
    );

    test(
      'should convert to and from JSON',
      () {
        expect(
          TimeRangeModel.fromJson(tTimeRangeOneTwo.toJson()),
          equals(tTimeRangeOneTwo),
        );
        expect(
          TimeRangeModel.fromJson(tTimeRangeMidnight.toJson()),
          equals(tTimeRangeMidnight),
        );
        expect(
          TimeRangeModel.fromJson(tTimeRangeMinutes.toJson()),
          equals(tTimeRangeMinutes),
        );
        expect(
          TimeRangeModel.fromJson(tTimeRangeSeconds.toJson()),
          equals(tTimeRangeSeconds),
        );
        expect(
          TimeRangeModel.fromJson(tTimeRangeOvernight.toJson()),
          equals(tTimeRangeOvernight),
        );
      },
    );
  });

  group('Frequency', () {
    test(
      'should convert to and from JSON',
      () {
        expect(
          decodeFrequency(encodeFrequency(Frequency.hourly)),
          Frequency.hourly,
        );
        expect(
          decodeFrequency(encodeFrequency(Frequency.daily)),
          Frequency.daily,
        );
        expect(
          decodeFrequency(encodeFrequency(Frequency.weekly)),
          Frequency.weekly,
        );
        expect(
          decodeFrequency(encodeFrequency(Frequency.monthly)),
          Frequency.monthly,
        );
      },
    );
  });

  group('WeekOfMonth', () {
    test(
      'should convert to and from JSON',
      () {
        expect(
          decodeWeekOfMonth(encodeWeekOfMonth(const None())),
          equals(const None<WeekOfMonth>()),
        );
        expect(
          decodeWeekOfMonth(encodeWeekOfMonth(const Some(WeekOfMonth.first))),
          const Some(WeekOfMonth.first),
        );
        expect(
          decodeWeekOfMonth(encodeWeekOfMonth(const Some(WeekOfMonth.second))),
          const Some(WeekOfMonth.second),
        );
        expect(
          decodeWeekOfMonth(encodeWeekOfMonth(const Some(WeekOfMonth.third))),
          const Some(WeekOfMonth.third),
        );
        expect(
          decodeWeekOfMonth(encodeWeekOfMonth(const Some(WeekOfMonth.fourth))),
          const Some(WeekOfMonth.fourth),
        );
        expect(
          decodeWeekOfMonth(encodeWeekOfMonth(const Some(WeekOfMonth.fifth))),
          const Some(WeekOfMonth.fifth),
        );
      },
    );
  });

  group('DayOfWeek', () {
    test(
      'should convert to and from JSON',
      () {
        expect(
          decodeDayOfWeek(encodeDayOfWeek(const None())),
          equals(const None<DayOfWeek>()),
        );
        expect(
          decodeDayOfWeek(encodeDayOfWeek(const Some(DayOfWeek.sun))),
          const Some(DayOfWeek.sun),
        );
        expect(
          decodeDayOfWeek(encodeDayOfWeek(const Some(DayOfWeek.mon))),
          const Some(DayOfWeek.mon),
        );
        expect(
          decodeDayOfWeek(encodeDayOfWeek(const Some(DayOfWeek.tue))),
          const Some(DayOfWeek.tue),
        );
        expect(
          decodeDayOfWeek(encodeDayOfWeek(const Some(DayOfWeek.wed))),
          const Some(DayOfWeek.wed),
        );
        expect(
          decodeDayOfWeek(encodeDayOfWeek(const Some(DayOfWeek.thu))),
          const Some(DayOfWeek.thu),
        );
        expect(
          decodeDayOfWeek(encodeDayOfWeek(const Some(DayOfWeek.fri))),
          const Some(DayOfWeek.fri),
        );
        expect(
          decodeDayOfWeek(encodeDayOfWeek(const Some(DayOfWeek.sat))),
          const Some(DayOfWeek.sat),
        );
      },
    );
  });

  group('ScheduleModel', () {
    const tScheduleModel = ScheduleModel(
      frequency: Frequency.hourly,
      timeRange: None(),
      dayOfMonth: None(),
      dayOfWeek: None(),
      weekOfMonth: None(),
    );
    test(
      'should be a subclass of Schedule entity',
      () {
        // assert
        expect(tScheduleModel, isA<Schedule>());
      },
    );

    test(
      'should convert to and from JSON',
      () {
        expect(
          ScheduleModel.fromJson(tScheduleModel.toJson()),
          equals(tScheduleModel),
        );
        const weeklyThursday = ScheduleModel(
          frequency: Frequency.weekly,
          timeRange: None(),
          dayOfMonth: None(),
          dayOfWeek: Some(DayOfWeek.thu),
          weekOfMonth: None(),
        );
        expect(
          ScheduleModel.fromJson(weeklyThursday.toJson()),
          equals(weeklyThursday),
        );
        const monthlyThirdWed = ScheduleModel(
          frequency: Frequency.monthly,
          timeRange: None(),
          dayOfMonth: None(),
          dayOfWeek: Some(DayOfWeek.wed),
          weekOfMonth: Some(WeekOfMonth.third),
        );
        expect(
          ScheduleModel.fromJson(monthlyThirdWed.toJson()),
          equals(monthlyThirdWed),
        );
        const weeklySaturdayNight = ScheduleModel(
          frequency: Frequency.weekly,
          timeRange: Some(TimeRangeModel(start: 72000, stop: 14400)),
          dayOfMonth: None(),
          dayOfWeek: None(),
          weekOfMonth: None(),
        );
        expect(
          ScheduleModel.fromJson(weeklySaturdayNight.toJson()),
          equals(weeklySaturdayNight),
        );
      },
    );
  });

  group('DataSetModel', () {
    final tDataSetModel = DataSetModel(
      key: 'foo113',
      computerId: 'cray-11',
      basepath: '/home/planet',
      schedules: const [
        ScheduleModel(
          frequency: Frequency.weekly,
          timeRange: None(),
          dayOfMonth: None(),
          dayOfWeek: Some(DayOfWeek.thu),
          weekOfMonth: None(),
        )
      ],
      packSize: 67108864,
      stores: const ['store/local/storey'],
      excludes: const [],
      snapshot: Some(
        SnapshotModel(
          checksum: 'cafebabe',
          parent: const Some('ebebebeb'),
          startTime: DateTime.now(),
          endTime: Some(DateTime.now()),
          fileCount: 1234567890,
          tree: 'deadbeef',
        ),
      ),
      status: Status.finished,
      backupState: const Some(
        BackupStateModel(
          paused: false,
          stopRequested: false,
          changedFiles: 1001,
          packsUploaded: 10,
          filesUploaded: 101,
          bytesUploaded: 10001,
        ),
      ),
      errorMsg: const None(),
    );
    test(
      'should be a subclass of DataSet entity',
      () {
        // assert
        expect(tDataSetModel, isA<DataSet>());
      },
    );

    test(
      'should convert to and from JSON',
      () {
        // assert (round-trip)
        final encoded = tDataSetModel.toJson();
        final decoded = DataSetModel.fromJson(encoded);
        expect(decoded, equals(tDataSetModel));
        // compare everything else not listed in props
        expect(decoded.schedules, equals(tDataSetModel.schedules));
        expect(decoded.packSize, equals(tDataSetModel.packSize));
        expect(decoded.stores, equals(tDataSetModel.stores));
        // except this one we don't care about
        // expect(decoded.snapshot, equals(tDataSetModel.snapshot));
        expect(decoded.backupState, equals(tDataSetModel.backupState));

        // arrange (with minimal data)
        const model = DataSetModel(
          key: '',
          computerId: '',
          basepath: '',
          schedules: [],
          packSize: 0,
          stores: ['store/local/storey'],
          excludes: [],
          snapshot: None(),
          status: Status.none,
          backupState: None(),
          errorMsg: None(),
        );
        // assert
        expect(
          DataSetModel.fromJson(model.toJson()),
          equals(model),
        );
      },
    );

    test('should convert from a DataSet to a model', () {
      // arrange
      final dataSet = DataSet(
        key: 'setkey1',
        computerId: 'cray-11',
        basepath: '/home/planet',
        schedules: const [
          Schedule(
            frequency: Frequency.weekly,
            timeRange: None(),
            dayOfMonth: None(),
            dayOfWeek: Some(DayOfWeek.thu),
            weekOfMonth: None(),
          )
        ],
        packSize: 67108864,
        stores: const ['store/local/setkey1'],
        excludes: const [],
        snapshot: Some(
          Snapshot(
            checksum: 'sha1-a6c930a6f7f9aa4eb8ef67980e9e8e32cd02fa2b',
            parent: const Some('sha1-823bb0cf28e72fef2651cf1bb06abfc5fdc51634'),
            startTime: DateTime.parse('2020-03-15T05:36:04.960782134+00:00'),
            endTime: Some(
              DateTime.parse('2020-03-15T05:36:05.141905479+00:00'),
            ),
            fileCount: 125331,
            tree: 'sha1-698058583b2283b8c02ea5e40272c8364a0d6e78',
          ),
        ),
        status: Status.finished,
        backupState: const None(),
        errorMsg: const None(),
      );
      final dataSetModel = DataSetModel(
        key: 'setkey1',
        computerId: 'cray-11',
        basepath: '/home/planet',
        schedules: const [
          ScheduleModel(
            frequency: Frequency.weekly,
            timeRange: None(),
            dayOfMonth: None(),
            dayOfWeek: Some(DayOfWeek.thu),
            weekOfMonth: None(),
          )
        ],
        packSize: 67108864,
        stores: const ['store/local/setkey1'],
        excludes: const [],
        snapshot: Some(
          SnapshotModel(
            checksum: 'sha1-a6c930a6f7f9aa4eb8ef67980e9e8e32cd02fa2b',
            parent: const Some('sha1-823bb0cf28e72fef2651cf1bb06abfc5fdc51634'),
            startTime: DateTime.parse('2020-03-15T05:36:04.960782134+00:00'),
            endTime: Some(
              DateTime.parse('2020-03-15T05:36:05.141905479+00:00'),
            ),
            fileCount: 125331,
            tree: 'sha1-698058583b2283b8c02ea5e40272c8364a0d6e78',
          ),
        ),
        status: Status.finished,
        backupState: const None(),
        errorMsg: const None(),
      );
      // act
      final result = DataSetModel.from(dataSet);
      // assert
      expect(result, equals(dataSetModel));
    });
  });
}
