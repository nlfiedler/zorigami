//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:flutter_test/flutter_test.dart';
import 'package:mockito/mockito.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/pack_store.dart';
import 'package:zorigami/core/domain/repositories/pack_store_repository.dart';
import 'package:zorigami/core/domain/usecases/define_pack_store.dart';

class MockPackStoreRepository extends Mock implements PackStoreRepository {}

void main() {
  DefinePackStore usecase;
  MockPackStoreRepository mockPackStoreRepository;

  setUp(() {
    mockPackStoreRepository = MockPackStoreRepository();
    usecase = DefinePackStore(mockPackStoreRepository);
  });

  final tPackStore = PackStore(
    key: 'cafebabe',
    label: 'ok go',
    kind: StoreKind.local,
    options: {},
  );

  test(
    'should define a pack store within the repository',
    () async {
      // arrange
      when(mockPackStoreRepository.definePackStore(any))
          .thenAnswer((_) async => Ok(tPackStore));
      // act
      final result = await usecase(Params(store: tPackStore));
      // assert
      expect(result, Ok(tPackStore));
      verify(mockPackStoreRepository.definePackStore(any));
      verifyNoMoreInteractions(mockPackStoreRepository);
    },
  );
}
