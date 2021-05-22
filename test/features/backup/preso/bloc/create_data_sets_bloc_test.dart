//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:bloc_test/bloc_test.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:mockito/annotations.dart';
import 'package:mockito/mockito.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/data_set.dart';
import 'package:zorigami/core/domain/repositories/data_set_repository.dart';
import 'package:zorigami/core/domain/usecases/define_data_set.dart' as dds;
import 'package:zorigami/core/error/failures.dart';
import 'package:zorigami/features/backup/preso/bloc/create_data_sets_bloc.dart';
import './create_data_sets_bloc_test.mocks.dart';

@GenerateMocks([DataSetRepository])
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
    snapshot: None(),
    status: Status.none,
    errorMsg: None(),
  );

  group('normal cases', () {
    setUp(() {
      mockDataSetRepository = MockDataSetRepository();
      usecase = dds.DefineDataSet(mockDataSetRepository);
      when(mockDataSetRepository.defineDataSet(any))
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
      when(mockDataSetRepository.defineDataSet(any))
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
