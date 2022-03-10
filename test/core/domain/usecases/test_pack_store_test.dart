//
// Copyright (c) 2022 Nathan Fiedler
//
import 'package:flutter_test/flutter_test.dart';
import 'package:mocktail/mocktail.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/pack_store.dart';
import 'package:zorigami/core/domain/repositories/pack_store_repository.dart';
import 'package:zorigami/core/domain/usecases/test_pack_store.dart';
import 'package:zorigami/core/error/failures.dart';

class MockPackStoreRepository extends Mock implements PackStoreRepository {}

void main() {
  late TestPackStore usecase;
  late MockPackStoreRepository mockPackStoreRepository;

  setUp(() {
    mockPackStoreRepository = MockPackStoreRepository();
    usecase = TestPackStore(mockPackStoreRepository);
  });

  final key = 'cafebabe';
  final tPackStore = PackStore(
    key: key,
    label: 'label1',
    kind: StoreKind.local,
    options: {},
  );

  setUpAll(() {
    // mocktail needs a fallback for any() that involves custom types
    registerFallbackValue(tPackStore);
  });

  test(
    'should delete a pack store within the repository',
    () async {
      // arrange
      when(() => mockPackStoreRepository.testPackStore(any()))
          .thenAnswer((_) async => Ok<String, Failure>('ok'));
      // act
      final result = await usecase(Params(store: tPackStore));
      // assert
      expect(result, Ok<String, Failure>('ok'));
      verify(() => mockPackStoreRepository.testPackStore(any()));
      verifyNoMoreInteractions(mockPackStoreRepository);
    },
  );
}
