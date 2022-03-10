//
// Copyright (c) 2020 Nathan Fiedler
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
    final tTimeRangeOneTwo = TimeRangeModel(start: 1, stop: 2);
    // special case, midnight to noon
    final tTimeRangeMidnight = TimeRangeModel(start: 0, stop: 43200);
    // range with hours and minutes: 10:30am to 6:30pm
    final tTimeRangeMinutes = TimeRangeModel(start: 37800, stop: 66600);
    // range with hours, minutes, and seconds: 8:45:25am to 12:30:36pm
    final tTimeRangeSeconds = TimeRangeModel(start: 31525, stop: 45036);
    // start time is greater than stop time
    final tTimeRangeOvernight = TimeRangeModel(start: 79200, stop: 21600);
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
          decodeWeekOfMonth(encodeWeekOfMonth(None())),
          equals(None<WeekOfMonth>()),
        );
        expect(
          decodeWeekOfMonth(encodeWeekOfMonth(Some(WeekOfMonth.first))),
          Some(WeekOfMonth.first),
        );
        expect(
          decodeWeekOfMonth(encodeWeekOfMonth(Some(WeekOfMonth.second))),
          Some(WeekOfMonth.second),
        );
        expect(
          decodeWeekOfMonth(encodeWeekOfMonth(Some(WeekOfMonth.third))),
          Some(WeekOfMonth.third),
        );
        expect(
          decodeWeekOfMonth(encodeWeekOfMonth(Some(WeekOfMonth.fourth))),
          Some(WeekOfMonth.fourth),
        );
        expect(
          decodeWeekOfMonth(encodeWeekOfMonth(Some(WeekOfMonth.fifth))),
          Some(WeekOfMonth.fifth),
        );
      },
    );
  });

  group('DayOfWeek', () {
    test(
      'should convert to and from JSON',
      () {
        expect(
          decodeDayOfWeek(encodeDayOfWeek(None())),
          equals(None<DayOfWeek>()),
        );
        expect(
          decodeDayOfWeek(encodeDayOfWeek(Some(DayOfWeek.sun))),
          Some(DayOfWeek.sun),
        );
        expect(
          decodeDayOfWeek(encodeDayOfWeek(Some(DayOfWeek.mon))),
          Some(DayOfWeek.mon),
        );
        expect(
          decodeDayOfWeek(encodeDayOfWeek(Some(DayOfWeek.tue))),
          Some(DayOfWeek.tue),
        );
        expect(
          decodeDayOfWeek(encodeDayOfWeek(Some(DayOfWeek.wed))),
          Some(DayOfWeek.wed),
        );
        expect(
          decodeDayOfWeek(encodeDayOfWeek(Some(DayOfWeek.thu))),
          Some(DayOfWeek.thu),
        );
        expect(
          decodeDayOfWeek(encodeDayOfWeek(Some(DayOfWeek.fri))),
          Some(DayOfWeek.fri),
        );
        expect(
          decodeDayOfWeek(encodeDayOfWeek(Some(DayOfWeek.sat))),
          Some(DayOfWeek.sat),
        );
      },
    );
  });

  group('ScheduleModel', () {
    final tScheduleModel = ScheduleModel(
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
        final weeklyThursday = ScheduleModel(
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
        final monthlyThirdWed = ScheduleModel(
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
        final weeklySaturdayNight = ScheduleModel(
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
      schedules: [
        ScheduleModel(
          frequency: Frequency.weekly,
          timeRange: None(),
          dayOfMonth: None(),
          dayOfWeek: Some(DayOfWeek.thu),
          weekOfMonth: None(),
        )
      ],
      packSize: 67108864,
      stores: ['store/local/storey'],
      excludes: [],
      snapshot: Some(
        SnapshotModel(
          checksum: 'cafebabe',
          parent: Some('ebebebeb'),
          startTime: DateTime.now(),
          endTime: Some(DateTime.now()),
          fileCount: 1234567890,
          tree: 'deadbeef',
        ),
      ),
      status: Status.finished,
      errorMsg: None(),
    );
    test(
      'should be a subclass of Schedule entity',
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

        // arrange (with minimal data)
        final model = DataSetModel(
          key: '',
          computerId: '',
          basepath: '',
          schedules: [],
          packSize: 0,
          stores: ['store/local/storey'],
          excludes: [],
          snapshot: None(),
          status: Status.none,
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
        schedules: [
          Schedule(
            frequency: Frequency.weekly,
            timeRange: None(),
            dayOfMonth: None(),
            dayOfWeek: Some(DayOfWeek.thu),
            weekOfMonth: None(),
          )
        ],
        packSize: 67108864,
        stores: ['store/local/setkey1'],
        excludes: [],
        snapshot: Some(
          Snapshot(
            checksum: 'sha1-a6c930a6f7f9aa4eb8ef67980e9e8e32cd02fa2b',
            parent: Some('sha1-823bb0cf28e72fef2651cf1bb06abfc5fdc51634'),
            startTime: DateTime.parse('2020-03-15T05:36:04.960782134+00:00'),
            endTime: Some(
              DateTime.parse('2020-03-15T05:36:05.141905479+00:00'),
            ),
            fileCount: 125331,
            tree: 'sha1-698058583b2283b8c02ea5e40272c8364a0d6e78',
          ),
        ),
        status: Status.finished,
        errorMsg: None(),
      );
      final dataSetModel = DataSetModel(
        key: 'setkey1',
        computerId: 'cray-11',
        basepath: '/home/planet',
        schedules: [
          ScheduleModel(
            frequency: Frequency.weekly,
            timeRange: None(),
            dayOfMonth: None(),
            dayOfWeek: Some(DayOfWeek.thu),
            weekOfMonth: None(),
          )
        ],
        packSize: 67108864,
        stores: ['store/local/setkey1'],
        excludes: [],
        snapshot: Some(
          SnapshotModel(
            checksum: 'sha1-a6c930a6f7f9aa4eb8ef67980e9e8e32cd02fa2b',
            parent: Some('sha1-823bb0cf28e72fef2651cf1bb06abfc5fdc51634'),
            startTime: DateTime.parse('2020-03-15T05:36:04.960782134+00:00'),
            endTime: Some(
              DateTime.parse('2020-03-15T05:36:05.141905479+00:00'),
            ),
            fileCount: 125331,
            tree: 'sha1-698058583b2283b8c02ea5e40272c8364a0d6e78',
          ),
        ),
        status: Status.finished,
        errorMsg: None(),
      );
      // act
      final result = DataSetModel.from(dataSet);
      // assert
      expect(result, equals(dataSetModel));
    });
  });
}
