//
// Copyright (c) 2024 Nathan Fiedler
//
import 'package:flutter_test/flutter_test.dart';
import 'package:mocktail/mocktail.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/repositories/snapshot_repository.dart';
import 'package:zorigami/core/domain/usecases/cancel_restore.dart';
import 'package:zorigami/core/error/failures.dart';

class MockSnapshotRepository extends Mock implements SnapshotRepository {}

void main() {
  late CancelRestore usecase;
  late MockSnapshotRepository mockSnapshotRepository;

  setUp(() {
    mockSnapshotRepository = MockSnapshotRepository();
    usecase = CancelRestore(mockSnapshotRepository);
  });

  test(
    'should cancel a restore request in the repository',
    () async {
      // arrange
      when(() =>
              mockSnapshotRepository.cancelRestore(any(), any(), any(), any()))
          .thenAnswer((_) async => const Ok<bool, Failure>(true));
      // act
      final result = await usecase(const Params(
        tree: 'sha1-deadbeef',
        entry: 'filename.txt',
        filepath: 'filename.txt',
        dataset: 'homeset',
      ));
      // assert
      expect(result, equals(const Ok<bool, Failure>(true)));
      verify(() =>
          mockSnapshotRepository.cancelRestore(any(), any(), any(), any()));
      verifyNoMoreInteractions(mockSnapshotRepository);
    },
  );
}
