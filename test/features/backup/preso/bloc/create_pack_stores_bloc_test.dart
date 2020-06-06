//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:bloc_test/bloc_test.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:mockito/mockito.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/pack_store.dart';
import 'package:zorigami/core/domain/repositories/pack_store_repository.dart';
import 'package:zorigami/core/domain/usecases/define_pack_store.dart' as dps;
import 'package:zorigami/core/error/failures.dart';
import 'package:zorigami/features/backup/preso/bloc/create_pack_stores_bloc.dart';

class MockPackStoreRepository extends Mock implements PackStoreRepository {}

void main() {
  MockPackStoreRepository mockPackStoreRepository;
  dps.DefinePackStore usecase;

  final tPackStore = PackStore(
    key: 'PackStore1',
    kind: StoreKind.local,
    label: 'Locally',
    options: {'basepath': '/home/planet'},
  );

  group('normal cases', () {
    setUp(() {
      mockPackStoreRepository = MockPackStoreRepository();
      usecase = dps.DefinePackStore(mockPackStoreRepository);
      when(mockPackStoreRepository.definePackStore(any))
          .thenAnswer((_) async => Ok(tPackStore));
    });

    blocTest(
      'emits [] when nothing is added',
      build: () async => CreatePackStoresBloc(usecase: usecase),
      expect: [],
    );

    blocTest(
      'emits [Submitting, Submitted] when DefinePackStore is added',
      build: () async => CreatePackStoresBloc(usecase: usecase),
      act: (bloc) => bloc.add(DefinePackStore(store: tPackStore)),
      expect: [Submitting(), Submitted()],
    );
  });

  group('error cases', () {
    setUp(() {
      mockPackStoreRepository = MockPackStoreRepository();
      usecase = dps.DefinePackStore(mockPackStoreRepository);
      when(mockPackStoreRepository.definePackStore(any))
          .thenAnswer((_) async => Err(ServerFailure('oh no!')));
    });

    blocTest(
      'emits [Submitting, Error] when DefinePackStore is added',
      build: () async => CreatePackStoresBloc(usecase: usecase),
      act: (bloc) => bloc.add(DefinePackStore(store: tPackStore)),
      expect: [Submitting(), Error(message: 'ServerFailure(oh no!)')],
    );
  });
}