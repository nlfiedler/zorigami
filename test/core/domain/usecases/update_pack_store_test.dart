//
// Copyright (c) 2024 Nathan Fiedler
//
import 'package:flutter_test/flutter_test.dart';
import 'package:mocktail/mocktail.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/pack_store.dart';
import 'package:zorigami/core/domain/repositories/pack_store_repository.dart';
import 'package:zorigami/core/domain/usecases/update_pack_store.dart';
import 'package:zorigami/core/error/failures.dart';

class MockPackStoreRepository extends Mock implements PackStoreRepository {}

void main() {
  late UpdatePackStore usecase;
  late MockPackStoreRepository mockPackStoreRepository;

  setUp(() {
    mockPackStoreRepository = MockPackStoreRepository();
    usecase = UpdatePackStore(mockPackStoreRepository);
  });

  const key = 'cafebabe';
  const tPackStore = PackStore(
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
          .thenAnswer((_) async => const Ok<PackStore, Failure>(tPackStore));
      // act
      final result = await usecase(const Params(store: tPackStore));
      // assert
      expect(result, equals(const Ok<PackStore, Failure>(tPackStore)));
      verify(() => mockPackStoreRepository.updatePackStore(any()));
      verifyNoMoreInteractions(mockPackStoreRepository);
    },
  );
}
