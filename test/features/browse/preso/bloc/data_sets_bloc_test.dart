//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:bloc_test/bloc_test.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:mockito/mockito.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/data_set.dart';
import 'package:zorigami/core/domain/repositories/data_set_repository.dart';
import 'package:zorigami/core/domain/usecases/get_data_sets.dart';
import 'package:zorigami/core/error/failures.dart';
import 'package:zorigami/features/browse/preso/bloc/data_sets_bloc.dart';

class MockDataSetRepository extends Mock implements DataSetRepository {}

void main() {
  MockDataSetRepository mockDataSetRepository;
  GetDataSets usecase;

  final tDataSet = DataSet(
    key: 'dataset1',
    computerId: 'localhost',
    basepath: '/home/planet',
    schedules: [],
    packSize: 1048576,
    stores: ['store/local/abc'],
    snapshot: None(),
    errorMsg: None(),
  );

  group('normal cases', () {
    setUp(() {
      mockDataSetRepository = MockDataSetRepository();
      usecase = GetDataSets(mockDataSetRepository);
      when(mockDataSetRepository.getAllDataSets())
          .thenAnswer((_) async => Ok([tDataSet]));
    });

    blocTest(
      'emits [] when nothing is added',
      build: () => DataSetsBloc(usecase: usecase),
      expect: [],
    );

    blocTest(
      'emits [Loading, Loaded] when LoadAllDataSets is added',
      build: () => DataSetsBloc(usecase: usecase),
      act: (bloc) => bloc.add(LoadAllDataSets()),
      expect: [
        Loading(),
        Loaded(sets: [tDataSet])
      ],
    );

    blocTest(
      'emits [Loading, Loaded, Empty] when ReloadDataSets is added',
      build: () => DataSetsBloc(usecase: usecase),
      act: (bloc) {
        bloc.add(LoadAllDataSets());
        bloc.add(ReloadDataSets());
        return;
      },
      expect: [
        Loading(),
        Loaded(sets: [tDataSet]),
        Empty()
      ],
    );
  });

  group('error cases', () {
    setUp(() {
      mockDataSetRepository = MockDataSetRepository();
      usecase = GetDataSets(mockDataSetRepository);
      when(mockDataSetRepository.getAllDataSets())
          .thenAnswer((_) async => Err(ServerFailure('oh no!')));
    });

    blocTest(
      'emits [Loading, Error] when LoadAllDataSets is added',
      build: () => DataSetsBloc(usecase: usecase),
      act: (bloc) => bloc.add(LoadAllDataSets()),
      expect: [Loading(), Error(message: 'ServerFailure(oh no!)')],
    );
  });
}
