//
// Copyright (c) 2024 Nathan Fiedler
//
import 'package:bloc_test/bloc_test.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:mocktail/mocktail.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/pack_store.dart';
import 'package:zorigami/core/domain/repositories/pack_store_repository.dart';
import 'package:zorigami/core/domain/usecases/define_pack_store.dart' as dps;
import 'package:zorigami/core/error/failures.dart';
import 'package:zorigami/features/backup/preso/bloc/create_pack_stores_bloc.dart';

class MockPackStoreRepository extends Mock implements PackStoreRepository {}

void main() {
  late MockPackStoreRepository mockPackStoreRepository;
  late dps.DefinePackStore usecase;

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
      usecase = dps.DefinePackStore(mockPackStoreRepository);
      when(() => mockPackStoreRepository.definePackStore(any()))
          .thenAnswer((_) async => const Ok(tPackStore));
    });

    blocTest(
      'emits [] when nothing is added',
      build: () => CreatePackStoresBloc(usecase: usecase),
      expect: () => [],
    );

    blocTest(
      'emits [Submitting, Submitted] when DefinePackStore is added',
      build: () => CreatePackStoresBloc(usecase: usecase),
      act: (CreatePackStoresBloc bloc) =>
          bloc.add(DefinePackStore(store: tPackStore)),
      expect: () => [Submitting(), Submitted()],
    );
  });

  group('error cases', () {
    setUp(() {
      mockPackStoreRepository = MockPackStoreRepository();
      usecase = dps.DefinePackStore(mockPackStoreRepository);
      when(() => mockPackStoreRepository.definePackStore(any()))
          .thenAnswer((_) async => const Err(ServerFailure('oh no!')));
    });

    blocTest(
      'emits [Submitting, Error] when DefinePackStore is added',
      build: () => CreatePackStoresBloc(usecase: usecase),
      act: (CreatePackStoresBloc bloc) =>
          bloc.add(DefinePackStore(store: tPackStore)),
      expect: () => [Submitting(), Error(message: 'ServerFailure(oh no!)')],
    );
  });
}
