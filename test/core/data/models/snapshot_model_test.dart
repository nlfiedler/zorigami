//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/data/models/snapshot_model.dart';
import 'package:zorigami/core/domain/entities/snapshot.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  group('SnapshotModel', () {
    final tSnapshotModel = SnapshotModel(
      checksum: 'cafebabe',
      parent: Option.none(),
      startTime: DateTime.now(),
      endTime: Option.none(),
      fileCount: 123,
      tree: 'deadbeef',
    );
    test(
      'should be a subclass of Snapshot entity',
      () {
        // assert
        expect(tSnapshotModel, isA<Snapshot>());
      },
    );

    test(
      'should convert to and from JSON',
      () {
        expect(
          SnapshotModel.fromJson(tSnapshotModel.toJson()),
          equals(tSnapshotModel),
        );
        final model = SnapshotModel(
          checksum: 'cafebabe',
          parent: Option.some('ebebebeb'),
          startTime: DateTime.now(),
          endTime: Option.some(DateTime.now()),
          fileCount: 1234567890,
          tree: 'deadbeef',
        );
        expect(
          SnapshotModel.fromJson(model.toJson()),
          equals(model),
        );
      },
    );
  });
}
