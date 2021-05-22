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
          TimeRange(start: -1, stop: 0).validate(),
          isA<Err>(),
        );
        expect(
          TimeRange(start: 86401, stop: 0).validate(),
          isA<Err>(),
        );
      },
    );

    test(
      'should reject invalid stop time',
      () {
        expect(
          TimeRange(start: 0, stop: -1).validate(),
          isA<Err>(),
        );
        expect(
          TimeRange(start: 0, stop: 86401).validate(),
          isA<Err>(),
        );
      },
    );

    test(
      'should accept valid time range',
      () {
        expect(
          TimeRange(start: 0, stop: 0).validate(),
          isA<Ok>(),
        );
        expect(
          TimeRange(start: 86400, stop: 86400).validate(),
          isA<Ok>(),
        );
        expect(
          TimeRange(start: 72000, stop: 14400).validate(),
          isA<Ok>(),
        );
        expect(
          TimeRange(start: 14400, stop: 72000).validate(),
          isA<Ok>(),
        );
      },
    );

    test(
      'should pretty print various ranges',
      () {
        expect(
          TimeRange(start: 0, stop: 0).toPrettyString(),
          equals('from 12:00 to 12:00'),
        );
        expect(
          TimeRange(start: 86400, stop: 86400).toPrettyString(),
          equals('from 12:00 to 12:00'),
        );
        expect(
          TimeRange(start: 72000, stop: 14400).toPrettyString(),
          equals('from 20:00 to 04:00'),
        );
        expect(
          TimeRange(start: 12660, stop: 54060).toPrettyString(),
          equals('from 03:31 to 15:01'),
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
            weekOfMonth: Some(WeekOfMonth.first),
            dayOfWeek: None(),
            dayOfMonth: None(),
            timeRange: None(),
          ).validate(),
          isA<Err>(),
        );
        expect(
          Schedule(
            frequency: Frequency.hourly,
            weekOfMonth: None(),
            dayOfWeek: Some(DayOfWeek.thu),
            dayOfMonth: None(),
            timeRange: None(),
          ).validate(),
          isA<Err>(),
        );
        expect(
          Schedule(
            frequency: Frequency.hourly,
            weekOfMonth: None(),
            dayOfWeek: None(),
            dayOfMonth: Some(10),
            timeRange: None(),
          ).validate(),
          isA<Err>(),
        );
        expect(
          Schedule(
            frequency: Frequency.hourly,
            weekOfMonth: None(),
            dayOfWeek: None(),
            dayOfMonth: None(),
            timeRange: Some(TimeRange(start: 0, stop: 0)),
          ).validate(),
          isA<Err>(),
        );
      },
    );

    test(
      'should reject daily with anything other than time range',
      () {
        expect(
          Schedule(
            frequency: Frequency.daily,
            weekOfMonth: Some(WeekOfMonth.first),
            dayOfWeek: None(),
            dayOfMonth: None(),
            timeRange: None(),
          ).validate(),
          isA<Err>(),
        );
        expect(
          Schedule(
            frequency: Frequency.daily,
            weekOfMonth: None(),
            dayOfWeek: Some(DayOfWeek.thu),
            dayOfMonth: None(),
            timeRange: None(),
          ).validate(),
          isA<Err>(),
        );
        expect(
          Schedule(
            frequency: Frequency.daily,
            weekOfMonth: None(),
            dayOfWeek: None(),
            dayOfMonth: Some(10),
            timeRange: None(),
          ).validate(),
          isA<Err>(),
        );
        expect(
          Schedule(
            frequency: Frequency.daily,
            weekOfMonth: None(),
            dayOfWeek: None(),
            dayOfMonth: None(),
            timeRange: Some(TimeRange(start: 0, stop: 0)),
          ).validate(),
          isA<Ok>(),
        );
      },
    );

    test(
      'should reject weekly with week-of-month or day-of-month',
      () {
        expect(
          Schedule(
            frequency: Frequency.weekly,
            weekOfMonth: Some(WeekOfMonth.first),
            dayOfWeek: None(),
            dayOfMonth: None(),
            timeRange: None(),
          ).validate(),
          isA<Err>(),
        );
        expect(
          Schedule(
            frequency: Frequency.weekly,
            weekOfMonth: None(),
            dayOfWeek: Some(DayOfWeek.thu),
            dayOfMonth: None(),
            timeRange: None(),
          ).validate(),
          isA<Ok>(),
        );
        expect(
          Schedule(
            frequency: Frequency.weekly,
            weekOfMonth: None(),
            dayOfWeek: None(),
            dayOfMonth: Some(10),
            timeRange: None(),
          ).validate(),
          isA<Err>(),
        );
        expect(
          Schedule(
            frequency: Frequency.weekly,
            weekOfMonth: None(),
            dayOfWeek: None(),
            dayOfMonth: None(),
            timeRange: Some(TimeRange(start: 0, stop: 0)),
          ).validate(),
          isA<Ok>(),
        );
      },
    );

    test(
      'should reject monthly with day-of-month and day-of-week',
      () {
        final result = Schedule(
          frequency: Frequency.monthly,
          weekOfMonth: Some(WeekOfMonth.first),
          dayOfWeek: Some(DayOfWeek.thu),
          dayOfMonth: Some(10),
          timeRange: None(),
        ).validate();
        expect(result, isA<Err>());
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
          weekOfMonth: None(),
          dayOfWeek: Some(DayOfWeek.thu),
          dayOfMonth: None(),
          timeRange: None(),
        ).validate();
        expect(result, isA<Err>());
        expect(
          result.err().unwrap().message,
          contains('requires week-of-month when using day-of-week'),
        );
      },
    );

    test(
      'should pretty print the schedule',
      () {
        expect(
          Schedule(
            frequency: Frequency.hourly,
            weekOfMonth: None(),
            dayOfWeek: None(),
            dayOfMonth: None(),
            timeRange: None(),
          ).toPrettyString(),
          equals('hourly'),
        );
        expect(
          Schedule(
            frequency: Frequency.daily,
            weekOfMonth: None(),
            dayOfWeek: None(),
            dayOfMonth: None(),
            timeRange: None(),
          ).toPrettyString(),
          equals('daily'),
        );
        expect(
          Schedule(
            frequency: Frequency.weekly,
            weekOfMonth: None(),
            dayOfWeek: Some(DayOfWeek.mon),
            dayOfMonth: None(),
            timeRange: None(),
          ).toPrettyString(),
          equals('weekly on Monday'),
        );
        expect(
          Schedule(
            frequency: Frequency.monthly,
            weekOfMonth: Some(WeekOfMonth.second),
            dayOfWeek: Some(DayOfWeek.thu),
            dayOfMonth: None(),
            timeRange: Some(TimeRange(start: 79200, stop: 16200)),
          ).toPrettyString(),
          equals('monthly on the second Thursday from 22:00 to 04:30'),
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
          snapshot: None(),
          status: Status.none,
          errorMsg: None(),
        ).validate();
        expect(result, isA<Err>());
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
            weekOfMonth: None(),
            dayOfWeek: Some(DayOfWeek.thu),
            dayOfMonth: None(),
            timeRange: None(),
          )
        ],
        packSize: 0,
        stores: ['foo'],
        snapshot: None(),
        status: Status.none,
        errorMsg: None(),
      ).validate();
      expect(result, isA<Err>());
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
          snapshot: None(),
          status: Status.none,
          errorMsg: None(),
        ).validate();
        expect(result, isA<Ok>());
      },
    );
  });
}
