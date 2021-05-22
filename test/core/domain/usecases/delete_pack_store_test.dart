//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:flutter_test/flutter_test.dart';
import 'package:mockito/annotations.dart';
import 'package:mockito/mockito.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/pack_store.dart';
import 'package:zorigami/core/domain/repositories/pack_store_repository.dart';
import 'package:zorigami/core/domain/usecases/delete_pack_store.dart';
import './define_pack_store_test.mocks.dart';

@GenerateMocks([PackStoreRepository])
void main() {
  late DeletePackStore usecase;
  late MockPackStoreRepository mockPackStoreRepository;

  setUp(() {
    mockPackStoreRepository = MockPackStoreRepository();
    usecase = DeletePackStore(mockPackStoreRepository);
  });

  final key = 'cafebabe';
  final tPackStore = PackStore(
    key: key,
    label: 'label1',
    kind: StoreKind.local,
    options: {},
  );

  test(
    'should delete a pack store within the repository',
    () async {
      // arrange
      when(mockPackStoreRepository.deletePackStore(any))
          .thenAnswer((_) async => Ok(tPackStore));
      // act
      final result = await usecase(Params(store: tPackStore));
      // assert
      expect(result, Ok(tPackStore));
      verify(mockPackStoreRepository.deletePackStore(any));
      verifyNoMoreInteractions(mockPackStoreRepository);
    },
  );
}
