//
// Copyright (c) 2022 Nathan Fiedler
//
import 'package:flutter_test/flutter_test.dart';
import 'package:mocktail/mocktail.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/snapshot.dart';
import 'package:zorigami/core/domain/repositories/snapshot_repository.dart';
import 'package:zorigami/core/domain/usecases/get_snapshot.dart';

class MockSnapshotRepository extends Mock implements SnapshotRepository {}

void main() {
  late GetSnapshot usecase;
  late MockSnapshotRepository mockSnapshotRepository;

  setUp(() {
    mockSnapshotRepository = MockSnapshotRepository();
    usecase = GetSnapshot(mockSnapshotRepository);
  });

  final tSnapshot = Snapshot(
    checksum: 'cafebabe',
    parent: None(),
    startTime: DateTime.now(),
    endTime: None(),
    fileCount: 101,
    tree: 'deadbeef',
  );

  test(
    'should get a snapshot from the repository',
    () async {
      // arrange
      when(() => mockSnapshotRepository.getSnapshot(any()))
          .thenAnswer((_) async => Ok(tSnapshot));
      // act
      final result = await usecase(Params(checksum: 'deadbeef'));
      // assert
      expect(result, Ok(tSnapshot));
      verify(() => mockSnapshotRepository.getSnapshot(any()));
      verifyNoMoreInteractions(mockSnapshotRepository);
    },
  );
}
