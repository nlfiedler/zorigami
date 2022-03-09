//
// Copyright (c) 2022 Nathan Fiedler
//
import 'package:flutter_test/flutter_test.dart';
import 'package:mocktail/mocktail.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/pack_store.dart';
import 'package:zorigami/core/domain/repositories/pack_store_repository.dart';
import 'package:zorigami/core/domain/usecases/update_pack_store.dart';

class MockPackStoreRepository extends Mock implements PackStoreRepository {}

void main() {
  late UpdatePackStore usecase;
  late MockPackStoreRepository mockPackStoreRepository;

  setUp(() {
    mockPackStoreRepository = MockPackStoreRepository();
    usecase = UpdatePackStore(mockPackStoreRepository);
  });

  final key = 'cafebabe';
  final tPackStore = PackStore(
    key: key,
    label: 'ok go',
    kind: StoreKind.local,
    options: {},
  );

  setUpAll(() {
    // mocktail needs a fallback for any() that involves custom types
    registerFallbackValue(tPackStore);
  });

  test(
    'should update an existing pack store within the repository',
    () async {
      // arrange
      when(() => mockPackStoreRepository.updatePackStore(any()))
          .thenAnswer((_) async => Ok(tPackStore));
      // act
      final result = await usecase(Params(store: tPackStore));
      // assert
      expect(result, Ok(tPackStore));
      verify(() => mockPackStoreRepository.updatePackStore(any()));
      verifyNoMoreInteractions(mockPackStoreRepository);
    },
  );
}
