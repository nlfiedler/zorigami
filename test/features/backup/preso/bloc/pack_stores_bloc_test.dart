//
// Copyright (c) 2024 Nathan Fiedler
//
import 'package:bloc_test/bloc_test.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:mocktail/mocktail.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/pack_store.dart';
import 'package:zorigami/core/domain/repositories/pack_store_repository.dart';
import 'package:zorigami/core/domain/usecases/get_pack_stores.dart';
import 'package:zorigami/core/error/failures.dart';
import 'package:zorigami/features/backup/preso/bloc/pack_stores_bloc.dart';

class MockPackStoreRepository extends Mock implements PackStoreRepository {}

void main() {
  late MockPackStoreRepository mockPackStoreRepository;
  late GetPackStores usecase;

  const tPackStore = PackStore(
    key: 'PackStore1',
    kind: StoreKind.local,
    label: 'Locally',
    options: {'basepath': '/home/planet'},
  );

  group('normal cases', () {
    setUp(() {
      mockPackStoreRepository = MockPackStoreRepository();
      usecase = GetPackStores(mockPackStoreRepository);
      when(() => mockPackStoreRepository.getAllPackStores())
          .thenAnswer((_) async => const Ok([tPackStore]));
    });

    blocTest(
      'emits [] when nothing is added',
      build: () => PackStoresBloc(usecase: usecase),
      expect: () => [],
    );

    blocTest(
      'emits [Loading, Loaded] when LoadAllPackStores is added',
      build: () => PackStoresBloc(usecase: usecase),
      act: (PackStoresBloc bloc) => bloc.add(LoadAllPackStores()),
      expect: () => [
        Loading(),
        Loaded(stores: const [tPackStore])
      ],
    );

    blocTest(
      'emits [Loading, Loaded, Empty] when ReloadDataSets is added',
      build: () => PackStoresBloc(usecase: usecase),
      act: (PackStoresBloc bloc) {
        bloc.add(LoadAllPackStores());
        bloc.add(ReloadPackStores());
        return;
      },
      expect: () => [
        Loading(),
        Loaded(stores: const [tPackStore]),
        Empty()
      ],
    );
  });

  group('error cases', () {
    setUp(() {
      mockPackStoreRepository = MockPackStoreRepository();
      usecase = GetPackStores(mockPackStoreRepository);
      when(() => mockPackStoreRepository.getAllPackStores())
          .thenAnswer((_) async => const Err(ServerFailure('oh no!')));
    });

    blocTest(
      'emits [Loading, Loaded] when LoadAllPackStores is added',
      build: () => PackStoresBloc(usecase: usecase),
      act: (PackStoresBloc bloc) => bloc.add(LoadAllPackStores()),
      expect: () => [
        Loading(),
        Error(message: 'ServerFailure(oh no!)'),
      ],
    );
  });
}
