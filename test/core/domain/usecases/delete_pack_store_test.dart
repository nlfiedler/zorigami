//
// Copyright (c) 2019 Nathan Fiedler
//
import 'package:flutter_test/flutter_test.dart';
import 'package:mockito/mockito.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/repositories/pack_store_repository.dart';
import 'package:zorigami/core/domain/usecases/delete_pack_store.dart';

class MockPackStoreRepository extends Mock implements PackStoreRepository {}

void main() {
  DeletePackStore usecase;
  MockPackStoreRepository mockPackStoreRepository;

  setUp(() {
    mockPackStoreRepository = MockPackStoreRepository();
    usecase = DeletePackStore(mockPackStoreRepository);
  });

  final key = 'cafebabe';

  test(
    'should delete a pack store within the repository',
    () async {
      // arrange
      when(mockPackStoreRepository.deletePackStore(any))
          .thenAnswer((_) async => Result.ok(key));
      // act
      final result = await usecase(Params(key: key));
      // assert
      expect(result, Result.ok(key));
      verify(mockPackStoreRepository.deletePackStore(any));
      verifyNoMoreInteractions(mockPackStoreRepository);
    },
  );
}
