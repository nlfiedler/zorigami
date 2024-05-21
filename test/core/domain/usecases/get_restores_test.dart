//
// Copyright (c) 2024 Nathan Fiedler
//
import 'package:flutter_test/flutter_test.dart';
import 'package:mocktail/mocktail.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/request.dart';
import 'package:zorigami/core/domain/repositories/snapshot_repository.dart';
import 'package:zorigami/core/domain/usecases/get_restores.dart';
import 'package:zorigami/core/domain/usecases/usecase.dart';
import 'package:zorigami/core/error/failures.dart';

class MockSnapshotRepository extends Mock implements SnapshotRepository {}

void main() {
  late GetRestores usecase;
  late MockSnapshotRepository mockSnapshotRepository;

  setUp(() {
    mockSnapshotRepository = MockSnapshotRepository();
    usecase = GetRestores(mockSnapshotRepository);
  });

  test(
    'should get all pending restore requests from the repository',
    () async {
      // arrange
      when(() => mockSnapshotRepository.getAllRestores())
          .thenAnswer((_) async => const Ok<List<Request>, Failure>([]));
      // act
      final result = await usecase(NoParams());
      // assert
      expect(result, equals(const Ok<List<Request>, Failure>([])));
      verify(() => mockSnapshotRepository.getAllRestores());
      verifyNoMoreInteractions(mockSnapshotRepository);
    },
  );
}
