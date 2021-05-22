//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:bloc_test/bloc_test.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:mockito/annotations.dart';
import 'package:mockito/mockito.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/repositories/snapshot_repository.dart';
import 'package:zorigami/core/domain/usecases/restore_database.dart' as rd;
import 'package:zorigami/core/error/failures.dart';
import 'package:zorigami/features/browse/preso/bloc/database_restore_bloc.dart';
import './database_restore_bloc_test.mocks.dart';

@GenerateMocks([SnapshotRepository])
void main() {
  late MockSnapshotRepository mockSnapshotRepository;
  late rd.RestoreDatabase usecase;

  group('normal cases', () {
    setUp(() {
      mockSnapshotRepository = MockSnapshotRepository();
      usecase = rd.RestoreDatabase(mockSnapshotRepository);
      when(mockSnapshotRepository.restoreDatabase('local123'))
          .thenAnswer((_) async => Ok('ok'));
    });

    test('ensure Loaded implements Equatable', () {
      expect(
        Loaded(result: 'ok'),
        equals(Loaded(result: 'ok')),
      );
      expect(
        Loaded(result: 'ok'),
        isNot(equals(Loaded(result: 'error'))),
      );
    });

    blocTest(
      'emits [] when nothing is added',
      build: () => DatabaseRestoreBloc(usecase: usecase),
      expect: () => [],
    );

    blocTest(
      'emits [Loading, Loaded] when LoadSnapshot is added',
      build: () => DatabaseRestoreBloc(usecase: usecase),
      act: (DatabaseRestoreBloc bloc) =>
          bloc.add(RestoreDatabase(storeId: 'local123')),
      expect: () => [Loading(), Loaded(result: 'ok')],
    );
  });

  group('error cases', () {
    setUp(() {
      mockSnapshotRepository = MockSnapshotRepository();
      usecase = rd.RestoreDatabase(mockSnapshotRepository);
      when(mockSnapshotRepository.restoreDatabase(any))
          .thenAnswer((_) async => Err(ServerFailure('oh no!')));
    });

    blocTest(
      'emits [Loading, Error] when LoadSnapshot is added',
      build: () => DatabaseRestoreBloc(usecase: usecase),
      act: (DatabaseRestoreBloc bloc) =>
          bloc.add(RestoreDatabase(storeId: 'local123')),
      expect: () => [Loading(), Error(message: 'ServerFailure(oh no!)')],
    );
  });
}
