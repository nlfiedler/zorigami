//
// Copyright (c) 2024 Nathan Fiedler
//
import 'package:flutter_test/flutter_test.dart';
import 'package:mocktail/mocktail.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/repositories/snapshot_repository.dart';
import 'package:zorigami/core/domain/usecases/restore_database.dart';
import 'package:zorigami/core/error/failures.dart';

class MockSnapshotRepository extends Mock implements SnapshotRepository {}

void main() {
  late RestoreDatabase usecase;
  late MockSnapshotRepository mockSnapshotRepository;

  setUp(() {
    mockSnapshotRepository = MockSnapshotRepository();
    usecase = RestoreDatabase(mockSnapshotRepository);
  });

  test(
    'should request database restore from the repository',
    () async {
      // arrange
      when(() => mockSnapshotRepository.restoreDatabase(any()))
          .thenAnswer((_) async => const Ok<String, Failure>('ok'));
      // act
      final result = await usecase(const Params(storeId: 'localstore'));
      // assert
      expect(result, equals(const Ok<String, Failure>('ok')));
      verify(() => mockSnapshotRepository.restoreDatabase(any()));
      verifyNoMoreInteractions(mockSnapshotRepository);
    },
  );
}
