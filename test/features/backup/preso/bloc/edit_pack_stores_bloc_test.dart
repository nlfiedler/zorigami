//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:bloc_test/bloc_test.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:mockito/mockito.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/pack_store.dart';
import 'package:zorigami/core/domain/repositories/pack_store_repository.dart';
import 'package:zorigami/core/domain/usecases/delete_pack_store.dart' as dds;
import 'package:zorigami/core/domain/usecases/update_pack_store.dart' as uds;
import 'package:zorigami/core/error/failures.dart';
import 'package:zorigami/features/backup/preso/bloc/edit_pack_stores_bloc.dart';

class MockPackStoreRepository extends Mock implements PackStoreRepository {}

void main() {
  MockPackStoreRepository mockPackStoreRepository;
  dds.DeletePackStore deleteUsecase;
  uds.UpdatePackStore updateUsecase;

  final tPackStore = PackStore(
    key: 'PackStore1',
    kind: StoreKind.local,
    label: 'Locally',
    options: {'basepath': '/home/planet'},
  );

  group('normal cases', () {
    setUp(() {
      mockPackStoreRepository = MockPackStoreRepository();
      deleteUsecase = dds.DeletePackStore(mockPackStoreRepository);
      updateUsecase = uds.UpdatePackStore(mockPackStoreRepository);
      when(mockPackStoreRepository.deletePackStore(any))
          .thenAnswer((_) async => Ok(tPackStore));
      when(mockPackStoreRepository.updatePackStore(any))
          .thenAnswer((_) async => Ok(tPackStore));
    });

    blocTest(
      'emits [] when nothing is added',
      build: () => EditPackStoresBloc(
        updatePackStore: updateUsecase,
        deletePackStore: deleteUsecase,
      ),
      expect: [],
    );

    blocTest(
      'emits [Submitting, Submitted] when DeletePackStore is added',
      build: () => EditPackStoresBloc(
        updatePackStore: updateUsecase,
        deletePackStore: deleteUsecase,
      ),
      act: (bloc) => bloc.add(DeletePackStore(store: tPackStore)),
      expect: [Submitting(), Submitted()],
    );

    blocTest(
      'emits [Submitting, Submitted] when UpdatePackStore is added',
      build: () => EditPackStoresBloc(
        updatePackStore: updateUsecase,
        deletePackStore: deleteUsecase,
      ),
      act: (bloc) => bloc.add(UpdatePackStore(store: tPackStore)),
      expect: [Submitting(), Submitted()],
    );
  });

  group('error cases', () {
    setUp(() {
      mockPackStoreRepository = MockPackStoreRepository();
      deleteUsecase = dds.DeletePackStore(mockPackStoreRepository);
      updateUsecase = uds.UpdatePackStore(mockPackStoreRepository);
      when(mockPackStoreRepository.deletePackStore(any))
          .thenAnswer((_) async => Err(ServerFailure('oh no!')));
      when(mockPackStoreRepository.updatePackStore(any))
          .thenAnswer((_) async => Err(ServerFailure('oh no!')));
    });

    blocTest(
      'emits [Submitting, Error] when DeletePackStore is added',
      build: () => EditPackStoresBloc(
        updatePackStore: updateUsecase,
        deletePackStore: deleteUsecase,
      ),
      act: (bloc) => bloc.add(DeletePackStore(store: tPackStore)),
      expect: [Submitting(), Error(message: 'ServerFailure(oh no!)')],
    );

    blocTest(
      'emits [Submitting, Error] when UpdatePackStore is added',
      build: () => EditPackStoresBloc(
        updatePackStore: updateUsecase,
        deletePackStore: deleteUsecase,
      ),
      act: (bloc) => bloc.add(UpdatePackStore(store: tPackStore)),
      expect: [Submitting(), Error(message: 'ServerFailure(oh no!)')],
    );
  });
}
