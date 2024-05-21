//
// Copyright (c) 2024 Nathan Fiedler
//
import 'package:intl/intl.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/data_set.dart';
import 'package:zorigami/core/domain/entities/snapshot.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  group('TimeRange', () {
    test(
      'should reject invalid start time',
      () {
        expect(
          const TimeRange(start: -1, stop: 0).validate(),
          isA<Err>(),
        );
        expect(
          const TimeRange(start: 86401, stop: 0).validate(),
          isA<Err>(),
        );
      },
    );

    test(
      'should reject invalid stop time',
      () {
        expect(
          const TimeRange(start: 0, stop: -1).validate(),
          isA<Err>(),
        );
        expect(
          const TimeRange(start: 0, stop: 86401).validate(),
          isA<Err>(),
        );
      },
    );

    test(
      'should accept valid time range',
      () {
        expect(
          const TimeRange(start: 0, stop: 0).validate(),
          isA<Ok>(),
        );
        expect(
          const TimeRange(start: 86400, stop: 86400).validate(),
          isA<Ok>(),
        );
        expect(
          const TimeRange(start: 72000, stop: 14400).validate(),
          isA<Ok>(),
        );
        expect(
          const TimeRange(start: 14400, stop: 72000).validate(),
          isA<Ok>(),
        );
      },
    );

    test(
      'should pretty print various ranges',
      () {
        expect(
          const TimeRange(start: 0, stop: 0).toPrettyString(),
          equals('from 12:00 AM to 12:00 AM'),
        );
        expect(
          const TimeRange(start: 0, stop: 43200).toPrettyString(),
          equals('from 12:00 AM to 12:00 PM'),
        );
        expect(
          const TimeRange(start: 43200, stop: 0).toPrettyString(),
          equals('from 12:00 PM to 12:00 AM'),
        );
        expect(
          const TimeRange(start: 86400, stop: 86400).toPrettyString(),
          equals('from 12:00 AM to 12:00 AM'),
        );
        expect(
          const TimeRange(start: 72000, stop: 14400).toPrettyString(),
          equals('from 8:00 PM to 4:00 AM'),
        );
        expect(
          const TimeRange(start: 12660, stop: 54060).toPrettyString(),
          equals('from 3:31 AM to 3:01 PM'),
        );
      },
    );
  });

  group('Schedule', () {
    test(
      'should reject hourly with any other settings',
      () {
        expect(
          const Schedule(
            frequency: Frequency.hourly,
            weekOfMonth: Some(WeekOfMonth.first),
            dayOfWeek: None(),
            dayOfMonth: None(),
            timeRange: None(),
          ).validate(),
          isA<Err>(),
        );
        expect(
          const Schedule(
            frequency: Frequency.hourly,
            weekOfMonth: None(),
            dayOfWeek: Some(DayOfWeek.thu),
            dayOfMonth: None(),
            timeRange: None(),
          ).validate(),
          isA<Err>(),
        );
        expect(
          const Schedule(
            frequency: Frequency.hourly,
            weekOfMonth: None(),
            dayOfWeek: None(),
            dayOfMonth: Some(10),
            timeRange: None(),
          ).validate(),
          isA<Err>(),
        );
        expect(
          const Schedule(
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
          const Schedule(
            frequency: Frequency.daily,
            weekOfMonth: Some(WeekOfMonth.first),
            dayOfWeek: None(),
            dayOfMonth: None(),
            timeRange: None(),
          ).validate(),
          isA<Err>(),
        );
        expect(
          const Schedule(
            frequency: Frequency.daily,
            weekOfMonth: None(),
            dayOfWeek: Some(DayOfWeek.thu),
            dayOfMonth: None(),
            timeRange: None(),
          ).validate(),
          isA<Err>(),
        );
        expect(
          const Schedule(
            frequency: Frequency.daily,
            weekOfMonth: None(),
            dayOfWeek: None(),
            dayOfMonth: Some(10),
            timeRange: None(),
          ).validate(),
          isA<Err>(),
        );
        expect(
          const Schedule(
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
          const Schedule(
            frequency: Frequency.weekly,
            weekOfMonth: Some(WeekOfMonth.first),
            dayOfWeek: None(),
            dayOfMonth: None(),
            timeRange: None(),
          ).validate(),
          isA<Err>(),
        );
        expect(
          const Schedule(
            frequency: Frequency.weekly,
            weekOfMonth: None(),
            dayOfWeek: Some(DayOfWeek.thu),
            dayOfMonth: None(),
            timeRange: None(),
          ).validate(),
          isA<Ok>(),
        );
        expect(
          const Schedule(
            frequency: Frequency.weekly,
            weekOfMonth: None(),
            dayOfWeek: None(),
            dayOfMonth: Some(10),
            timeRange: None(),
          ).validate(),
          isA<Err>(),
        );
        expect(
          const Schedule(
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
        final result = const Schedule(
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
        final result = const Schedule(
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
          const Schedule(
            frequency: Frequency.hourly,
            weekOfMonth: None(),
            dayOfWeek: None(),
            dayOfMonth: None(),
            timeRange: None(),
          ).toPrettyString(),
          equals('hourly'),
        );
        expect(
          const Schedule(
            frequency: Frequency.daily,
            weekOfMonth: None(),
            dayOfWeek: None(),
            dayOfMonth: None(),
            timeRange: None(),
          ).toPrettyString(),
          equals('daily'),
        );
        expect(
          const Schedule(
            frequency: Frequency.weekly,
            weekOfMonth: None(),
            dayOfWeek: Some(DayOfWeek.mon),
            dayOfMonth: None(),
            timeRange: None(),
          ).toPrettyString(),
          equals('weekly on Monday'),
        );
        expect(
          const Schedule(
            frequency: Frequency.monthly,
            weekOfMonth: Some(WeekOfMonth.second),
            dayOfWeek: Some(DayOfWeek.thu),
            dayOfMonth: None(),
            timeRange: Some(TimeRange(start: 79200, stop: 16200)),
          ).toPrettyString(),
          equals('monthly on the second Thursday from 10:00 PM to 4:30 AM'),
        );
      },
    );
  });

  group('DataSet', () {
    test(
      'should reject set without any stores',
      () {
        final result = const DataSet(
          key: '',
          computerId: '',
          basepath: '',
          schedules: [],
          packSize: 0,
          stores: [],
          excludes: [],
          snapshot: None(),
          status: Status.none,
          backupState: None(),
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
      final result = const DataSet(
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
        excludes: [],
        snapshot: None(),
        status: Status.none,
        backupState: None(),
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
        final result = const DataSet(
          key: '',
          computerId: '',
          basepath: '',
          schedules: [],
          packSize: 0,
          stores: ['foo'],
          excludes: [],
          snapshot: None(),
          status: Status.none,
          backupState: None(),
          errorMsg: None(),
        ).validate();
        expect(result, isA<Ok>());
      },
    );
  });

  group('Dataset.describeStatus', () {
    test('should say finished if status none but end time set', () {
      final sut = DataSet(
        key: '',
        computerId: '',
        basepath: '',
        schedules: const [],
        packSize: 0,
        stores: const [],
        excludes: const [],
        snapshot: Some(Snapshot(
          checksum: 'cafed00d',
          parent: const None(),
          startTime: DateTime(2021, 9, 29, 12, 42, 51),
          endTime: Some(DateTime(2021, 9, 29, 16, 24, 15)),
          fileCount: 121,
          tree: 'beefdead',
        )),
        status: Status.none,
        backupState: const None(),
        errorMsg: const None(),
      );
      final dt = DateTime(2021, 9, 29, 16, 24, 15);
      final localdt = DateFormat.yMd().add_jm().format(dt);
      expect(sut.describeStatus(), equals('finished at $localdt'));
    });

    test('should say not yet run if no status or end time', () {
      final sut = DataSet(
        key: '',
        computerId: '',
        basepath: '',
        schedules: const [],
        packSize: 0,
        stores: const [],
        excludes: const [],
        snapshot: Some(Snapshot(
          checksum: 'cafed00d',
          parent: const None(),
          startTime: DateTime.now(),
          endTime: const None(),
          fileCount: 121,
          tree: 'beefdead',
        )),
        status: Status.none,
        backupState: const None(),
        errorMsg: const None(),
      );
      expect(sut.describeStatus(), equals('not yet run'));
    });

    test('should say in progress if status running but no state', () {
      final sut = DataSet(
        key: '',
        computerId: '',
        basepath: '',
        schedules: const [],
        packSize: 0,
        stores: const [],
        excludes: const [],
        snapshot: Some(Snapshot(
          checksum: 'cafed00d',
          parent: const None(),
          startTime: DateTime.now(),
          endTime: Some(DateTime.now()),
          fileCount: 121,
          tree: 'beefdead',
        )),
        status: Status.running,
        backupState: const None(),
        errorMsg: const None(),
      );
      expect(sut.describeStatus(), equals('in progress'));
    });

    test('should give details if status running and has state', () {
      final sut = DataSet(
        key: '',
        computerId: '',
        basepath: '',
        schedules: const [],
        packSize: 0,
        stores: const [],
        excludes: const [],
        snapshot: Some(Snapshot(
          checksum: 'cafed00d',
          parent: const None(),
          startTime: DateTime.now(),
          endTime: Some(DateTime.now()),
          fileCount: 121,
          tree: 'beefdead',
        )),
        status: Status.running,
        backupState: const Some(
          BackupState(
            paused: false,
            stopRequested: false,
            changedFiles: 101,
            packsUploaded: 3,
            filesUploaded: 18,
            bytesUploaded: 10001,
          ),
        ),
        errorMsg: const None(),
      );
      expect(
        sut.describeStatus(),
        equals('18 of 101 files, 10,001 bytes uploaded'),
      );
    });

    test('should say finished at time if status and end time set', () {
      final sut = DataSet(
        key: '',
        computerId: '',
        basepath: '',
        schedules: const [],
        packSize: 0,
        stores: const [],
        excludes: const [],
        snapshot: Some(Snapshot(
          checksum: 'cafed00d',
          parent: const None(),
          startTime: DateTime(2021, 9, 29, 12, 42, 51),
          endTime: Some(DateTime(2021, 9, 29, 16, 24, 15)),
          fileCount: 121,
          tree: 'beefdead',
        )),
        status: Status.finished,
        backupState: const None(),
        errorMsg: const None(),
      );
      final dt = DateTime(2021, 9, 29, 16, 24, 15);
      final localdt = DateFormat.yMd().add_jm().format(dt);
      expect(sut.describeStatus(), equals('finished at $localdt'));
    });

    test('should say paused if status paused temporarily', () {
      final sut = DataSet(
        key: '',
        computerId: '',
        basepath: '',
        schedules: const [],
        packSize: 0,
        stores: const [],
        excludes: const [],
        snapshot: Some(Snapshot(
          checksum: 'cafed00d',
          parent: const None(),
          startTime: DateTime.now(),
          endTime: const None(),
          fileCount: 121,
          tree: 'beefdead',
        )),
        status: Status.paused,
        backupState: const None(),
        errorMsg: const None(),
      );
      expect(sut.describeStatus(), equals('paused'));
    });

    test('should say error unknown if status failed w/o cause', () {
      final sut = DataSet(
        key: '',
        computerId: '',
        basepath: '',
        schedules: const [],
        packSize: 0,
        stores: const [],
        excludes: const [],
        snapshot: Some(Snapshot(
          checksum: 'cafed00d',
          parent: const None(),
          startTime: DateTime.now(),
          endTime: const None(),
          fileCount: 121,
          tree: 'beefdead',
        )),
        status: Status.failed,
        backupState: const None(),
        errorMsg: const None(),
      );
      expect(sut.describeStatus(), equals('error: unknown'));
    });

    test('should say error foobar if status failed w/cause', () {
      final sut = DataSet(
        key: '',
        computerId: '',
        basepath: '',
        schedules: const [],
        packSize: 0,
        stores: const [],
        excludes: const [],
        snapshot: Some(Snapshot(
          checksum: 'cafed00d',
          parent: const None(),
          startTime: DateTime.now(),
          endTime: const None(),
          fileCount: 121,
          tree: 'beefdead',
        )),
        status: Status.failed,
        backupState: const None(),
        errorMsg: const Some('foobar'),
      );
      expect(sut.describeStatus(), equals('error: foobar'));
    });
  });

  group('formatTime', () {
    test('should handle midnight values', () {
      expect(formatTime(0), equals('12:00 AM'));
      expect(formatTime(86400), equals('12:00 AM'));
    });

    test('should format PM values', () {
      expect(formatTime(61920), equals('5:12 PM'));
      expect(formatTime(82860), equals('11:01 PM'));
    });

    test('should format AM values', () {
      expect(formatTime(34380), equals('9:33 AM'));
      expect(formatTime(40080), equals('11:08 AM'));
    });
  });
}
