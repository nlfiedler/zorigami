//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:bloc_test/bloc_test.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:mockito/mockito.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/data_set.dart';
import 'package:zorigami/core/domain/repositories/data_set_repository.dart';
import 'package:zorigami/core/domain/usecases/delete_data_set.dart' as dds;
import 'package:zorigami/core/domain/usecases/update_data_set.dart' as uds;
import 'package:zorigami/core/error/failures.dart';
import 'package:zorigami/features/backup/preso/bloc/edit_data_sets_bloc.dart';

class MockDataSetRepository extends Mock implements DataSetRepository {}

void main() {
  MockDataSetRepository mockDataSetRepository;
  dds.DeleteDataSet deleteUsecase;
  uds.UpdateDataSet updateUsecase;

  final tDataSet = DataSet(
    key: 'dataset1',
    computerId: 'localhost',
    basepath: '/home/planet',
    schedules: [],
    packSize: 1048576,
    stores: ['store/local/abc'],
    snapshot: None(),
  );

  group('normal cases', () {
    setUp(() {
      mockDataSetRepository = MockDataSetRepository();
      deleteUsecase = dds.DeleteDataSet(mockDataSetRepository);
      updateUsecase = uds.UpdateDataSet(mockDataSetRepository);
      when(mockDataSetRepository.deleteDataSet(any))
          .thenAnswer((_) async => Ok(tDataSet));
      when(mockDataSetRepository.updateDataSet(any))
          .thenAnswer((_) async => Ok(tDataSet));
    });

    blocTest(
      'emits [] when nothing is added',
      build: () => EditDataSetsBloc(
        updateDataSet: updateUsecase,
        deleteDataSet: deleteUsecase,
      ),
      expect: [],
    );

    blocTest(
      'emits [Submitting, Submitted] when DeleteDataSet is added',
      build: () => EditDataSetsBloc(
        updateDataSet: updateUsecase,
        deleteDataSet: deleteUsecase,
      ),
      act: (bloc) => bloc.add(DeleteDataSet(dataset: tDataSet)),
      expect: [Submitting(), Submitted()],
    );

    blocTest(
      'emits [Submitting, Submitted] when UpdateDataSet is added',
      build: () => EditDataSetsBloc(
        updateDataSet: updateUsecase,
        deleteDataSet: deleteUsecase,
      ),
      act: (bloc) => bloc.add(UpdateDataSet(dataset: tDataSet)),
      expect: [Submitting(), Submitted()],
    );
  });

  group('error cases', () {
    setUp(() {
      mockDataSetRepository = MockDataSetRepository();
      deleteUsecase = dds.DeleteDataSet(mockDataSetRepository);
      updateUsecase = uds.UpdateDataSet(mockDataSetRepository);
      when(mockDataSetRepository.deleteDataSet(any))
          .thenAnswer((_) async => Err(ServerFailure('oh no!')));
      when(mockDataSetRepository.updateDataSet(any))
          .thenAnswer((_) async => Err(ServerFailure('oh no!')));
    });

    blocTest(
      'emits [Submitting, Error] when DeleteDataSet is added',
      build: () => EditDataSetsBloc(
        updateDataSet: updateUsecase,
        deleteDataSet: deleteUsecase,
      ),
      act: (bloc) => bloc.add(DeleteDataSet(dataset: tDataSet)),
      expect: [Submitting(), Error(message: 'ServerFailure(oh no!)')],
    );

    blocTest(
      'emits [Submitting, Error] when UpdateDataSet is added',
      build: () => EditDataSetsBloc(
        updateDataSet: updateUsecase,
        deleteDataSet: deleteUsecase,
      ),
      act: (bloc) => bloc.add(UpdateDataSet(dataset: tDataSet)),
      expect: [Submitting(), Error(message: 'ServerFailure(oh no!)')],
    );
  });
}
