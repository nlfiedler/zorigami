//
// Copyright (c) 2022 Nathan Fiedler
//
import 'package:bloc_test/bloc_test.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:mocktail/mocktail.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/data_set.dart';
import 'package:zorigami/core/domain/repositories/data_set_repository.dart';
import 'package:zorigami/core/domain/usecases/define_data_set.dart' as dds;
import 'package:zorigami/core/error/failures.dart';
import 'package:zorigami/features/backup/preso/bloc/create_data_sets_bloc.dart';

class MockDataSetRepository extends Mock implements DataSetRepository {}

void main() {
  late MockDataSetRepository mockDataSetRepository;
  late dds.DefineDataSet usecase;

  final tDataSet = DataSet(
    key: 'dataset1',
    computerId: 'localhost',
    basepath: '/home/planet',
    schedules: [],
    packSize: 1048576,
    stores: ['store/local/abc'],
    excludes: [],
    snapshot: None(),
    status: Status.none,
    errorMsg: None(),
  );

  setUpAll(() {
    // mocktail needs a fallback for any() that involves custom types
    registerFallbackValue(tDataSet);
  });

  group('normal cases', () {
    setUp(() {
      mockDataSetRepository = MockDataSetRepository();
      usecase = dds.DefineDataSet(mockDataSetRepository);
      when(() => mockDataSetRepository.defineDataSet(any()))
          .thenAnswer((_) async => Ok(tDataSet));
    });

    blocTest(
      'emits [] when nothing is added',
      build: () => CreateDataSetsBloc(usecase: usecase),
      expect: () => [],
    );

    blocTest(
      'emits [Submitting, Submitted] when DefineDataSet is added',
      build: () => CreateDataSetsBloc(usecase: usecase),
      act: (CreateDataSetsBloc bloc) =>
          bloc.add(DefineDataSet(dataset: tDataSet)),
      expect: () => [Submitting(), Submitted()],
    );
  });

  group('error cases', () {
    setUp(() {
      mockDataSetRepository = MockDataSetRepository();
      usecase = dds.DefineDataSet(mockDataSetRepository);
      when(() => mockDataSetRepository.defineDataSet(any()))
          .thenAnswer((_) async => Err(ServerFailure('oh no!')));
    });

    blocTest(
      'emits [Submitting, Error] when DefineDataSet is added',
      build: () => CreateDataSetsBloc(usecase: usecase),
      act: (CreateDataSetsBloc bloc) =>
          bloc.add(DefineDataSet(dataset: tDataSet)),
      expect: () => [Submitting(), Error(message: 'ServerFailure(oh no!)')],
    );
  });
}
