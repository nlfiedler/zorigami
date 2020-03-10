//
// Copyright (c) 2019 Nathan Fiedler
//
import 'package:flutter_test/flutter_test.dart';
import 'package:mockito/mockito.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/pack_store.dart';
import 'package:zorigami/core/domain/repositories/pack_store_repository.dart';
import 'package:zorigami/core/domain/usecases/update_pack_store.dart';

class MockPackStoreRepository extends Mock implements PackStoreRepository {}

void main() {
  UpdatePackStore usecase;
  MockPackStoreRepository mockPackStoreRepository;

  setUp(() {
    mockPackStoreRepository = MockPackStoreRepository();
    usecase = UpdatePackStore(mockPackStoreRepository);
  });

  final key = 'cafebabe';
  final tPackStore =
      PackStore(key: key, label: 'ok go', kind: StoreKind.local, options: '');

  test(
    'should update an existing pack store within the repository',
    () async {
      // arrange
      when(mockPackStoreRepository.updatePackStore(any, any))
          .thenAnswer((_) async => Result.ok(tPackStore));
      // act
      final result = await usecase(Params(key: key, options: ''));
      // assert
      expect(result, Result.ok(tPackStore));
      verify(mockPackStoreRepository.updatePackStore(any, any));
      verifyNoMoreInteractions(mockPackStoreRepository);
    },
  );
}
