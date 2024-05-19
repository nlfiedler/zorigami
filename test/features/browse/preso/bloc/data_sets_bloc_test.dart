//
// Copyright (c) 2024 Nathan Fiedler
//
import 'package:bloc_test/bloc_test.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:mocktail/mocktail.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/data_set.dart';
import 'package:zorigami/core/domain/repositories/data_set_repository.dart';
import 'package:zorigami/core/domain/usecases/get_data_sets.dart';
import 'package:zorigami/core/domain/usecases/start_backup.dart' as start;
import 'package:zorigami/core/domain/usecases/stop_backup.dart' as stop;
import 'package:zorigami/core/error/failures.dart';
import 'package:zorigami/features/browse/preso/bloc/data_sets_bloc.dart';

class MockDataSetRepository extends Mock implements DataSetRepository {}

void main() {
  late MockDataSetRepository mockDataSetRepository;
  late GetDataSets getDataSets;
  late start.StartBackup startBackup;
  late stop.StopBackup stopBackup;

  const tDataSet = DataSet(
    key: 'dataset1',
    computerId: 'localhost',
    basepath: '/home/planet',
    schedules: [],
    packSize: 1048576,
    stores: ['store/local/abc'],
    excludes: [],
    snapshot: None(),
    status: Status.none,
    backupState: None(),
    errorMsg: None(),
  );

  setUpAll(() {
    // mocktail needs a fallback for any() that involves custom types
    registerFallbackValue(tDataSet);
  });

  group('normal cases', () {
    setUp(() {
      mockDataSetRepository = MockDataSetRepository();
      getDataSets = GetDataSets(mockDataSetRepository);
      startBackup = start.StartBackup(mockDataSetRepository);
      stopBackup = stop.StopBackup(mockDataSetRepository);
      when(() => mockDataSetRepository.getAllDataSets())
          .thenAnswer((_) async => const Ok([tDataSet]));
      when(() => mockDataSetRepository.startBackup(any()))
          .thenAnswer((_) async => const Ok(true));
      when(() => mockDataSetRepository.stopBackup(any()))
          .thenAnswer((_) async => const Ok(true));
    });

    blocTest(
      'emits [] when nothing is added',
      build: () => DataSetsBloc(
        getDataSets: getDataSets,
        startBackup: startBackup,
        stopBackup: stopBackup,
      ),
      expect: () => [],
    );

    blocTest(
      'emits [Loading, Loaded] when LoadAllDataSets is added',
      build: () => DataSetsBloc(
        getDataSets: getDataSets,
        startBackup: startBackup,
        stopBackup: stopBackup,
      ),
      act: (DataSetsBloc bloc) => bloc.add(LoadAllDataSets()),
      expect: () => [
        Loading(),
        Loaded(sets: const [tDataSet])
      ],
    );

    blocTest(
      'emits [Loading, Loaded, Empty] when ReloadDataSets is added',
      build: () => DataSetsBloc(
        getDataSets: getDataSets,
        startBackup: startBackup,
        stopBackup: stopBackup,
      ),
      act: (DataSetsBloc bloc) {
        bloc.add(LoadAllDataSets());
        bloc.add(ReloadDataSets());
        return;
      },
      expect: () => [
        Loading(),
        Loaded(sets: const [tDataSet]),
        Empty()
      ],
    );

    blocTest(
      'emits [Loading, Loaded]*2 when StartBackup is added',
      build: () => DataSetsBloc(
        getDataSets: getDataSets,
        startBackup: startBackup,
        stopBackup: stopBackup,
      ),
      act: (DataSetsBloc bloc) {
        bloc.add(LoadAllDataSets());
        bloc.add(StartBackup(dataset: tDataSet));
        return;
      },
      expect: () => [
        Loading(),
        Loaded(sets: const [tDataSet]),
        Loading(),
        Loaded(sets: const [tDataSet])
      ],
    );

    blocTest(
      'emits [Loading, Loaded]*2 when StopBackup is added',
      build: () => DataSetsBloc(
        getDataSets: getDataSets,
        startBackup: startBackup,
        stopBackup: stopBackup,
      ),
      act: (DataSetsBloc bloc) {
        bloc.add(LoadAllDataSets());
        bloc.add(StopBackup(dataset: tDataSet));
        return;
      },
      expect: () => [
        Loading(),
        Loaded(sets: const [tDataSet]),
        Loading(),
        Loaded(sets: const [tDataSet])
      ],
    );
  });

  group('error cases', () {
    setUp(() {
      mockDataSetRepository = MockDataSetRepository();
      getDataSets = GetDataSets(mockDataSetRepository);
      when(() => mockDataSetRepository.getAllDataSets())
          .thenAnswer((_) async => const Err(ServerFailure('oh no!')));
    });

    blocTest(
      'emits [Loading, Error] when LoadAllDataSets is added',
      build: () => DataSetsBloc(
        getDataSets: getDataSets,
        startBackup: startBackup,
        stopBackup: stopBackup,
      ),
      act: (DataSetsBloc bloc) => bloc.add(LoadAllDataSets()),
      expect: () => [Loading(), Error(message: 'ServerFailure(oh no!)')],
    );
  });
}
