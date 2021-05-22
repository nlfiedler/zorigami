//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:flutter_test/flutter_test.dart';
import 'package:mockito/annotations.dart';
import 'package:mockito/mockito.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/pack_store.dart';
import 'package:zorigami/core/domain/repositories/pack_store_repository.dart';
import 'package:zorigami/core/domain/usecases/get_pack_stores.dart';
import 'package:zorigami/core/domain/usecases/usecase.dart';
import './get_pack_stores_test.mocks.dart';

@GenerateMocks([PackStoreRepository])
void main() {
  late GetPackStores usecase;
  late MockPackStoreRepository mockPackStoreRepository;

  setUp(() {
    mockPackStoreRepository = MockPackStoreRepository();
    usecase = GetPackStores(mockPackStoreRepository);
  });

  final tPackStore = PackStore(
    key: 'cafebabe',
    label: 'ok go',
    kind: StoreKind.local,
    options: {},
  );
  // annotate the type to assist with matching
  final List<PackStore> tPackStores = List.from([tPackStore]);

  test(
    'should get all pack stores from the repository',
    () async {
      // arrange
      when(mockPackStoreRepository.getAllPackStores())
          .thenAnswer((_) async => Ok(tPackStores));
      // act
      final result = await usecase(NoParams());
      // assert
      expect(result, Ok(tPackStores));
      verify(mockPackStoreRepository.getAllPackStores());
      verifyNoMoreInteractions(mockPackStoreRepository);
    },
  );
}
