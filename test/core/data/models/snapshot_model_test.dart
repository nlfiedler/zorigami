//
// Copyright (c) 2024 Nathan Fiedler
//
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/data/models/snapshot_model.dart';
import 'package:zorigami/core/domain/entities/snapshot.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  group('SnapshotModel', () {
    final tSnapshotModel = SnapshotModel(
      checksum: 'cafebabe',
      parent: const None(),
      startTime: DateTime.now(),
      endTime: const None(),
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
        final actual = SnapshotModel(
          checksum: 'cafebabe',
          parent: const Some('ebebebeb'),
          startTime: DateTime.now(),
          endTime: Some(DateTime.now()),
          fileCount: 1234567890,
          tree: 'deadbeef',
        );
        final encoded = actual.toJson();
        final decoded = SnapshotModel.fromJson(encoded);
        expect(decoded, equals(actual));
        // compare everything else not listed in props
        expect(decoded.parent, equals(actual.parent));
        expect(decoded.startTime, equals(actual.startTime));
        expect(decoded.endTime, equals(actual.endTime));
        expect(decoded.fileCount, equals(actual.fileCount));
        expect(decoded.tree, equals(actual.tree));
      },
    );
  });
}
