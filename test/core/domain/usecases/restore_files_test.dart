//
// Copyright (c) 2022 Nathan Fiedler
//
import 'package:flutter_test/flutter_test.dart';
import 'package:mocktail/mocktail.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/repositories/snapshot_repository.dart';
import 'package:zorigami/core/domain/usecases/restore_files.dart';
import 'package:zorigami/core/error/failures.dart';

class MockSnapshotRepository extends Mock implements SnapshotRepository {}

void main() {
  late RestoreFiles usecase;
  late MockSnapshotRepository mockSnapshotRepository;

  setUp(() {
    mockSnapshotRepository = MockSnapshotRepository();
    usecase = RestoreFiles(mockSnapshotRepository);
  });

  test(
    'should send file restore request to the repository',
    () async {
      // arrange
      when(() => mockSnapshotRepository.restoreFiles(any(), any(), any()))
          .thenAnswer((_) async => Ok<bool, Failure>(true));
      // act
      final result = await usecase(Params(
        digest: 'deadbeef',
        filepath: 'filename.txt',
        dataset: 'homeset',
      ));
      // assert
      expect(result, equals(Ok<bool, Failure>(true)));
      verify(() => mockSnapshotRepository.restoreFiles(any(), any(), any()));
      verifyNoMoreInteractions(mockSnapshotRepository);
    },
  );
}
