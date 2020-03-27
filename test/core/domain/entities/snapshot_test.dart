//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/snapshot.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  group('TimeRange', () {
    test(
      'should reject invalid start time',
      () {
        expect(
          Snapshot(
            checksum: 'cafebabe',
            parent: Some('cafed00d'),
            startTime: DateTime.now(),
            endTime: None(),
            fileCount: 101,
            tree: 'deadbeef',
          ),
          equals(Snapshot(
            checksum: 'cafebabe',
            parent: Some('cafed00d'),
            startTime: DateTime.now(),
            endTime: None(),
            fileCount: 101,
            tree: 'deadbeef',
          )),
        );
        expect(
          Snapshot(
            checksum: 'cafebabe',
            parent: Some('cafed00d'),
            startTime: DateTime.now(),
            endTime: None(),
            fileCount: 101,
            tree: 'deadbeef',
          ),
          isNot(equals(Snapshot(
            checksum: 'cafed00d',
            parent: None(),
            startTime: DateTime.now(),
            endTime: None(),
            fileCount: 121,
            tree: 'beefdead',
          ))),
        );
      },
    );
  });
}
