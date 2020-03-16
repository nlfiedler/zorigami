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
    final tTimeRangeModel = TimeRangeModel(start: 1, stop: 2);
    test(
      'should be a subclass of TimeRange entity',
      () {
        // assert
        expect(tTimeRangeModel, isA<TimeRange>());
      },
    );

    test(
      'should convert to and from JSON',
      () {
        expect(
          TimeRangeModel.fromJson(tTimeRangeModel.toJson()),
          equals(tTimeRangeModel),
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
          decodeWeekOfMonth(encodeWeekOfMonth(Option.none())),
          equals(Option.none()),
        );
        expect(
          decodeWeekOfMonth(encodeWeekOfMonth(Option.some(WeekOfMonth.first))),
          Option.some(WeekOfMonth.first),
        );
        expect(
          decodeWeekOfMonth(encodeWeekOfMonth(Option.some(WeekOfMonth.second))),
          Option.some(WeekOfMonth.second),
        );
        expect(
          decodeWeekOfMonth(encodeWeekOfMonth(Option.some(WeekOfMonth.third))),
          Option.some(WeekOfMonth.third),
        );
        expect(
          decodeWeekOfMonth(encodeWeekOfMonth(Option.some(WeekOfMonth.fourth))),
          Option.some(WeekOfMonth.fourth),
        );
        expect(
          decodeWeekOfMonth(encodeWeekOfMonth(Option.some(WeekOfMonth.fifth))),
          Option.some(WeekOfMonth.fifth),
        );
      },
    );
  });

  group('DayOfWeek', () {
    test(
      'should convert to and from JSON',
      () {
        expect(
          decodeDayOfWeek(encodeDayOfWeek(Option.none())),
          equals(Option.none()),
        );
        expect(
          decodeDayOfWeek(encodeDayOfWeek(Option.some(DayOfWeek.sun))),
          Option.some(DayOfWeek.sun),
        );
        expect(
          decodeDayOfWeek(encodeDayOfWeek(Option.some(DayOfWeek.mon))),
          Option.some(DayOfWeek.mon),
        );
        expect(
          decodeDayOfWeek(encodeDayOfWeek(Option.some(DayOfWeek.tue))),
          Option.some(DayOfWeek.tue),
        );
        expect(
          decodeDayOfWeek(encodeDayOfWeek(Option.some(DayOfWeek.wed))),
          Option.some(DayOfWeek.wed),
        );
        expect(
          decodeDayOfWeek(encodeDayOfWeek(Option.some(DayOfWeek.thu))),
          Option.some(DayOfWeek.thu),
        );
        expect(
          decodeDayOfWeek(encodeDayOfWeek(Option.some(DayOfWeek.fri))),
          Option.some(DayOfWeek.fri),
        );
        expect(
          decodeDayOfWeek(encodeDayOfWeek(Option.some(DayOfWeek.sat))),
          Option.some(DayOfWeek.sat),
        );
      },
    );
  });

  group('ScheduleModel', () {
    final tScheduleModel = ScheduleModel(
      frequency: Frequency.hourly,
      timeRange: Option.none(),
      dayOfMonth: Option.none(),
      dayOfWeek: Option.none(),
      weekOfMonth: Option.none(),
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
          timeRange: Option.none(),
          dayOfMonth: Option.none(),
          dayOfWeek: Option.some(DayOfWeek.thu),
          weekOfMonth: Option.none(),
        );
        expect(
          ScheduleModel.fromJson(weeklyThursday.toJson()),
          equals(weeklyThursday),
        );
        final monthlyThirdWed = ScheduleModel(
          frequency: Frequency.monthly,
          timeRange: Option.none(),
          dayOfMonth: Option.none(),
          dayOfWeek: Option.some(DayOfWeek.wed),
          weekOfMonth: Option.some(WeekOfMonth.third),
        );
        expect(
          ScheduleModel.fromJson(monthlyThirdWed.toJson()),
          equals(monthlyThirdWed),
        );
        final weeklySaturdayNight = ScheduleModel(
          frequency: Frequency.weekly,
          timeRange: Option.some(TimeRangeModel(start: 72000, stop: 14400)),
          dayOfMonth: Option.none(),
          dayOfWeek: Option.none(),
          weekOfMonth: Option.none(),
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
          timeRange: Option.none(),
          dayOfMonth: Option.none(),
          dayOfWeek: Option.some(DayOfWeek.thu),
          weekOfMonth: Option.none(),
        )
      ],
      packSize: 67108864,
      stores: ['store/local/storey'],
      snapshot: Option.some(
        SnapshotModel(
          checksum: 'cafebabe',
          parent: Option.some('ebebebeb'),
          startTime: DateTime.now(),
          endTime: Option.some(DateTime.now()),
          fileCount: 1234567890,
          tree: 'deadbeef',
        ),
      ),
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
        expect(
          DataSetModel.fromJson(tDataSetModel.toJson()),
          equals(tDataSetModel),
        );

        // arrange (with minimal data)
        final model = DataSetModel(
          key: '',
          computerId: '',
          basepath: '',
          schedules: [],
          packSize: 0,
          stores: ['store/local/storey'],
          snapshot: Option.none(),
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
            timeRange: Option.none(),
            dayOfMonth: Option.none(),
            dayOfWeek: Option.some(DayOfWeek.thu),
            weekOfMonth: Option.none(),
          )
        ],
        packSize: 67108864,
        stores: ['store/local/setkey1'],
        snapshot: Option.some(
          Snapshot(
            checksum: 'sha1-a6c930a6f7f9aa4eb8ef67980e9e8e32cd02fa2b',
            parent:
                Option.some('sha1-823bb0cf28e72fef2651cf1bb06abfc5fdc51634'),
            startTime: DateTime.parse('2020-03-15T05:36:04.960782134+00:00'),
            endTime: Option.some(
              DateTime.parse('2020-03-15T05:36:05.141905479+00:00'),
            ),
            fileCount: 125331,
            tree: 'sha1-698058583b2283b8c02ea5e40272c8364a0d6e78',
          ),
        ),
      );
      final dataSetModel = DataSetModel(
        key: 'setkey1',
        computerId: 'cray-11',
        basepath: '/home/planet',
        schedules: [
          ScheduleModel(
            frequency: Frequency.weekly,
            timeRange: Option.none(),
            dayOfMonth: Option.none(),
            dayOfWeek: Option.some(DayOfWeek.thu),
            weekOfMonth: Option.none(),
          )
        ],
        packSize: 67108864,
        stores: ['store/local/setkey1'],
        snapshot: Option.some(
          SnapshotModel(
            checksum: 'sha1-a6c930a6f7f9aa4eb8ef67980e9e8e32cd02fa2b',
            parent:
                Option.some('sha1-823bb0cf28e72fef2651cf1bb06abfc5fdc51634'),
            startTime: DateTime.parse('2020-03-15T05:36:04.960782134+00:00'),
            endTime: Option.some(
              DateTime.parse('2020-03-15T05:36:05.141905479+00:00'),
            ),
            fileCount: 125331,
            tree: 'sha1-698058583b2283b8c02ea5e40272c8364a0d6e78',
          ),
        ),
      );
      // act
      final result = DataSetModel.from(dataSet);
      // assert
      expect(result, equals(dataSetModel));
    });
  });
}
