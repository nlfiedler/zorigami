//
// Copyright (c) 2024 Nathan Fiedler
//
import 'package:bloc_test/bloc_test.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:mocktail/mocktail.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/pack_store.dart';
import 'package:zorigami/core/domain/repositories/pack_store_repository.dart';
import 'package:zorigami/core/domain/usecases/delete_pack_store.dart' as dps;
import 'package:zorigami/core/domain/usecases/update_pack_store.dart' as ups;
import 'package:zorigami/core/domain/usecases/test_pack_store.dart' as tps;
import 'package:zorigami/core/error/failures.dart';
import 'package:zorigami/features/backup/preso/bloc/edit_pack_stores_bloc.dart';

class MockPackStoreRepository extends Mock implements PackStoreRepository {}

void main() {
  late MockPackStoreRepository mockPackStoreRepository;
  late dps.DeletePackStore deleteUsecase;
  late ups.UpdatePackStore updateUsecase;
  late tps.TestPackStore testUsecase;

  const tPackStore = PackStore(
    key: 'PackStore1',
    kind: StoreKind.local,
    label: 'Locally',
    options: {'basepath': '/home/planet'},
  );

  setUpAll(() {
    // mocktail needs a fallback for any() that involves custom types
    registerFallbackValue(tPackStore);
  });

  group('normal cases', () {
    setUp(() {
      mockPackStoreRepository = MockPackStoreRepository();
      deleteUsecase = dps.DeletePackStore(mockPackStoreRepository);
      updateUsecase = ups.UpdatePackStore(mockPackStoreRepository);
      testUsecase = tps.TestPackStore(mockPackStoreRepository);
      when(() => mockPackStoreRepository.deletePackStore(any()))
          .thenAnswer((_) async => const Ok(tPackStore));
      when(() => mockPackStoreRepository.updatePackStore(any()))
          .thenAnswer((_) async => const Ok(tPackStore));
    });

    blocTest(
      'emits [] when nothing is added',
      build: () => EditPackStoresBloc(
        updatePackStore: updateUsecase,
        deletePackStore: deleteUsecase,
        testPackStore: testUsecase,
      ),
      expect: () => [],
    );

    blocTest(
      'emits [Submitting, Submitted] when DeletePackStore is added',
      build: () => EditPackStoresBloc(
        updatePackStore: updateUsecase,
        deletePackStore: deleteUsecase,
        testPackStore: testUsecase,
      ),
      act: (EditPackStoresBloc bloc) =>
          bloc.add(DeletePackStore(store: tPackStore)),
      expect: () => [Submitting(), Submitted()],
    );

    blocTest(
      'emits [Submitting, Submitted] when UpdatePackStore is added',
      build: () => EditPackStoresBloc(
        updatePackStore: updateUsecase,
        deletePackStore: deleteUsecase,
        testPackStore: testUsecase,
      ),
      act: (EditPackStoresBloc bloc) =>
          bloc.add(UpdatePackStore(store: tPackStore)),
      expect: () => [Submitting(), Submitted()],
    );
  });

  group('error cases', () {
    setUp(() {
      mockPackStoreRepository = MockPackStoreRepository();
      deleteUsecase = dps.DeletePackStore(mockPackStoreRepository);
      updateUsecase = ups.UpdatePackStore(mockPackStoreRepository);
      when(() => mockPackStoreRepository.deletePackStore(any()))
          .thenAnswer((_) async => const Err(ServerFailure('oh no!')));
      when(() => mockPackStoreRepository.updatePackStore(any()))
          .thenAnswer((_) async => const Err(ServerFailure('oh no!')));
    });

    blocTest(
      'emits [Submitting, Error] when DeletePackStore is added',
      build: () => EditPackStoresBloc(
        updatePackStore: updateUsecase,
        deletePackStore: deleteUsecase,
        testPackStore: testUsecase,
      ),
      act: (EditPackStoresBloc bloc) =>
          bloc.add(DeletePackStore(store: tPackStore)),
      expect: () => [Submitting(), Error(message: 'ServerFailure(oh no!)')],
    );

    blocTest(
      'emits [Submitting, Error] when UpdatePackStore is added',
      build: () => EditPackStoresBloc(
        updatePackStore: updateUsecase,
        deletePackStore: deleteUsecase,
        testPackStore: testUsecase,
      ),
      act: (EditPackStoresBloc bloc) =>
          bloc.add(UpdatePackStore(store: tPackStore)),
      expect: () => [Submitting(), Error(message: 'ServerFailure(oh no!)')],
    );
  });
}
