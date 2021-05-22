//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:flutter_test/flutter_test.dart';
import 'package:mockito/annotations.dart';
import 'package:mockito/mockito.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/pack_store.dart';
import 'package:zorigami/core/domain/repositories/pack_store_repository.dart';
import 'package:zorigami/core/domain/usecases/update_pack_store.dart';
import './update_pack_store_test.mocks.dart';

@GenerateMocks([PackStoreRepository])
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

  test(
    'should update an existing pack store within the repository',
    () async {
      // arrange
      when(mockPackStoreRepository.updatePackStore(any))
          .thenAnswer((_) async => Ok(tPackStore));
      // act
      final result = await usecase(Params(store: tPackStore));
      // assert
      expect(result, Ok(tPackStore));
      verify(mockPackStoreRepository.updatePackStore(any));
      verifyNoMoreInteractions(mockPackStoreRepository);
    },
  );
}
