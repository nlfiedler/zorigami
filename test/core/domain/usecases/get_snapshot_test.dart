//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:flutter_test/flutter_test.dart';
import 'package:mockito/mockito.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/snapshot.dart';
import 'package:zorigami/core/domain/repositories/snapshot_repository.dart';
import 'package:zorigami/core/domain/usecases/get_snapshot.dart';

class MockSnapshotRepository extends Mock implements SnapshotRepository {}

void main() {
  GetSnapshot usecase;
  MockSnapshotRepository mockSnapshotRepository;

  setUp(() {
    mockSnapshotRepository = MockSnapshotRepository();
    usecase = GetSnapshot(mockSnapshotRepository);
  });

  final tSnapshot = Snapshot(
    checksum: 'cafebabe',
    parent: Option.none(),
    startTime: DateTime.now(),
    endTime: Option.none(),
    fileCount: 101,
    tree: 'deadbeef',
  );

  test(
    'should get a snapshot from the repository',
    () async {
      // arrange
      when(mockSnapshotRepository.getSnapshot(any))
          .thenAnswer((_) async => Result.ok(tSnapshot));
      // act
      final result = await usecase(Params(checksum: 'deadbeef'));
      // assert
      expect(result, Result.ok(tSnapshot));
      verify(mockSnapshotRepository.getSnapshot(any));
      verifyNoMoreInteractions(mockSnapshotRepository);
    },
  );
}
