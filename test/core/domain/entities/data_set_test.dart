//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/data_set.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  group('TimeRange', () {
    test(
      'should reject invalid start time',
      () {
        expect(
          TimeRange(start: -1, stop: 0).validate().isErr,
          isTrue,
        );
        expect(
          TimeRange(start: 86401, stop: 0).validate().isErr,
          isTrue,
        );
      },
    );

    test(
      'should reject invalid stop time',
      () {
        expect(
          TimeRange(start: 0, stop: -1).validate().isErr,
          isTrue,
        );
        expect(
          TimeRange(start: 0, stop: 86401).validate().isErr,
          isTrue,
        );
      },
    );

    test(
      'should accept valid time range',
      () {
        expect(
          TimeRange(start: 0, stop: 0).validate().isOk,
          isTrue,
        );
        expect(
          TimeRange(start: 86400, stop: 86400).validate().isOk,
          isTrue,
        );
        expect(
          TimeRange(start: 72000, stop: 14400).validate().isOk,
          isTrue,
        );
        expect(
          TimeRange(start: 14400, stop: 72000).validate().isOk,
          isTrue,
        );
      },
    );
  });

  group('Schedule', () {
    test(
      'should reject hourly with any other settings',
      () {
        expect(
          Schedule(
            frequency: Frequency.hourly,
            weekOfMonth: Option.some(WeekOfMonth.first),
            dayOfWeek: Option.none(),
            dayOfMonth: Option.none(),
            timeRange: Option.none(),
          ).validate().isErr,
          isTrue,
        );
        expect(
          Schedule(
            frequency: Frequency.hourly,
            weekOfMonth: Option.none(),
            dayOfWeek: Option.some(DayOfWeek.thu),
            dayOfMonth: Option.none(),
            timeRange: Option.none(),
          ).validate().isErr,
          isTrue,
        );
        expect(
          Schedule(
            frequency: Frequency.hourly,
            weekOfMonth: Option.none(),
            dayOfWeek: Option.none(),
            dayOfMonth: Option.some(10),
            timeRange: Option.none(),
          ).validate().isErr,
          isTrue,
        );
        expect(
          Schedule(
            frequency: Frequency.hourly,
            weekOfMonth: Option.none(),
            dayOfWeek: Option.none(),
            dayOfMonth: Option.none(),
            timeRange: Option.some(TimeRange(start: 0, stop: 0)),
          ).validate().isErr,
          isTrue,
        );
      },
    );

    test(
      'should reject daily with anything other than time range',
      () {
        expect(
          Schedule(
            frequency: Frequency.daily,
            weekOfMonth: Option.some(WeekOfMonth.first),
            dayOfWeek: Option.none(),
            dayOfMonth: Option.none(),
            timeRange: Option.none(),
          ).validate().isErr,
          isTrue,
        );
        expect(
          Schedule(
            frequency: Frequency.daily,
            weekOfMonth: Option.none(),
            dayOfWeek: Option.some(DayOfWeek.thu),
            dayOfMonth: Option.none(),
            timeRange: Option.none(),
          ).validate().isErr,
          isTrue,
        );
        expect(
          Schedule(
            frequency: Frequency.daily,
            weekOfMonth: Option.none(),
            dayOfWeek: Option.none(),
            dayOfMonth: Option.some(10),
            timeRange: Option.none(),
          ).validate().isErr,
          isTrue,
        );
        expect(
          Schedule(
            frequency: Frequency.daily,
            weekOfMonth: Option.none(),
            dayOfWeek: Option.none(),
            dayOfMonth: Option.none(),
            timeRange: Option.some(TimeRange(start: 0, stop: 0)),
          ).validate().isOk,
          isTrue,
        );
      },
    );

    test(
      'should reject weekly with week-of-month or day-of-month',
      () {
        expect(
          Schedule(
            frequency: Frequency.weekly,
            weekOfMonth: Option.some(WeekOfMonth.first),
            dayOfWeek: Option.none(),
            dayOfMonth: Option.none(),
            timeRange: Option.none(),
          ).validate().isErr,
          isTrue,
        );
        expect(
          Schedule(
            frequency: Frequency.weekly,
            weekOfMonth: Option.none(),
            dayOfWeek: Option.some(DayOfWeek.thu),
            dayOfMonth: Option.none(),
            timeRange: Option.none(),
          ).validate().isOk,
          isTrue,
        );
        expect(
          Schedule(
            frequency: Frequency.weekly,
            weekOfMonth: Option.none(),
            dayOfWeek: Option.none(),
            dayOfMonth: Option.some(10),
            timeRange: Option.none(),
          ).validate().isErr,
          isTrue,
        );
        expect(
          Schedule(
            frequency: Frequency.weekly,
            weekOfMonth: Option.none(),
            dayOfWeek: Option.none(),
            dayOfMonth: Option.none(),
            timeRange: Option.some(TimeRange(start: 0, stop: 0)),
          ).validate().isOk,
          isTrue,
        );
      },
    );

    test(
      'should reject monthly with day-of-month and day-of-week',
      () {
        final result = Schedule(
          frequency: Frequency.monthly,
          weekOfMonth: Option.some(WeekOfMonth.first),
          dayOfWeek: Option.some(DayOfWeek.thu),
          dayOfMonth: Option.some(10),
          timeRange: Option.none(),
        ).validate();
        expect(result.isErr, isTrue);
        expect(
          result.err().unwrap().message,
          contains('can only take either day-of-month or day-of-week'),
        );
      },
    );

    test(
      'should reject monthly with day-of-week but not week-of-month',
      () {
        final result = Schedule(
          frequency: Frequency.monthly,
          weekOfMonth: Option.none(),
          dayOfWeek: Option.some(DayOfWeek.thu),
          dayOfMonth: Option.none(),
          timeRange: Option.none(),
        ).validate();
        expect(result.isErr, isTrue);
        expect(
          result.err().unwrap().message,
          contains('requires week-of-month when using day-of-week'),
        );
      },
    );
  });

  group('DataSet', () {
    test(
      'should reject set without any stores',
      () {
        final result = DataSet(
          key: '',
          computerId: '',
          basepath: '',
          schedules: [],
          packSize: 0,
          stores: [],
          snapshot: Option.none(),
        ).validate();
        expect(result.isErr, isTrue);
        expect(
          result.err().unwrap().message,
          contains('must have at least one pack'),
        );
      },
    );

    test('should reject set with invalid schedule', () {
      final result = DataSet(
        key: '',
        computerId: '',
        basepath: '',
        schedules: [
          Schedule(
            frequency: Frequency.monthly,
            weekOfMonth: Option.none(),
            dayOfWeek: Option.some(DayOfWeek.thu),
            dayOfMonth: Option.none(),
            timeRange: Option.none(),
          )
        ],
        packSize: 0,
        stores: ['foo'],
        snapshot: Option.none(),
      ).validate();
      expect(result.isErr, isTrue);
      expect(
        result.err().unwrap().message,
        contains('requires week-of-month when using day-of-week'),
      );
    });

    test(
      'should accept set with valid properties',
      () {
        final result = DataSet(
          key: '',
          computerId: '',
          basepath: '',
          schedules: [],
          packSize: 0,
          stores: ['foo'],
          snapshot: Option.none(),
        ).validate();
        expect(result.isOk, isTrue);
      },
    );
  });
}
