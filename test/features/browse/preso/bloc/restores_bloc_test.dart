//
// Copyright (c) 2022 Nathan Fiedler
//
import 'package:bloc_test/bloc_test.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:mocktail/mocktail.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/data/models/request_model.dart';
import 'package:zorigami/core/domain/entities/request.dart';
import 'package:zorigami/core/domain/repositories/snapshot_repository.dart';
import 'package:zorigami/core/domain/usecases/cancel_restore.dart';
import 'package:zorigami/core/domain/usecases/get_restores.dart';
import 'package:zorigami/core/error/failures.dart';
import 'package:zorigami/features/browse/preso/bloc/restores_bloc.dart';

class MockSnapshotRepository extends Mock implements SnapshotRepository {}

void main() {
  late MockSnapshotRepository mockSnapshotRepository;
  late CancelRestore cancelRestore;
  late GetRestores getRestores;

  final tRequestModel = RequestModel(
    digest: 'cafebabe',
    filepath: 'dir/file',
    dataset: 'data123',
    finished: None(),
    filesRestored: 123,
    errorMessage: None(),
  );
  final List<Request> tRequests = [tRequestModel];

  group('normal cases', () {
    setUp(() {
      mockSnapshotRepository = MockSnapshotRepository();
      cancelRestore = CancelRestore(mockSnapshotRepository);
      getRestores = GetRestores(mockSnapshotRepository);
      when(() => mockSnapshotRepository.getAllRestores())
          .thenAnswer((_) async => Ok(tRequests));
      when(() => mockSnapshotRepository.cancelRestore(any(), any(), any()))
          .thenAnswer((_) async => Ok(true));
    });

    test('ensure Loaded implements Equatable', () {
      expect(
        Loaded(requests: tRequests, requestCancelled: false),
        equals(Loaded(requests: tRequests, requestCancelled: false)),
      );
      expect(
        Loaded(requests: tRequests, requestCancelled: false),
        isNot(equals(Loaded(requests: tRequests, requestCancelled: true))),
      );
    });

    blocTest(
      'emits [] when nothing is added',
      build: () => RestoresBloc(
        getRestores: getRestores,
        cancelRestore: cancelRestore,
      ),
      expect: () => [],
    );

    blocTest(
      'emits [Loading, Loaded] when loading requests',
      build: () => RestoresBloc(
        getRestores: getRestores,
        cancelRestore: cancelRestore,
      ),
      act: (RestoresBloc bloc) => bloc.add(LoadRequests()),
      expect: () =>
          [Loading(), Loaded(requests: tRequests, requestCancelled: false)],
    );

    blocTest(
      'emits [Loading, Loaded, Loaded] when canceling request',
      build: () => RestoresBloc(
        getRestores: getRestores,
        cancelRestore: cancelRestore,
      ),
      act: (RestoresBloc bloc) {
        bloc.add(LoadRequests());
        bloc.add(CancelRequest(
          digest: 'cafebabe',
          filepath: 'dir/file',
          dataset: 'superset',
        ));
        return;
      },
      expect: () => [
        Loading(),
        Loaded(requests: tRequests, requestCancelled: false),
        Loaded(requests: [], requestCancelled: true),
      ],
    );
  });

  group('error cases', () {
    setUp(() {
      mockSnapshotRepository = MockSnapshotRepository();
      getRestores = GetRestores(mockSnapshotRepository);
      when(() => mockSnapshotRepository.getAllRestores())
          .thenAnswer((_) async => Err(ServerFailure('oh no!')));
    });

    blocTest(
      'emits [Loading, Error] when request loading fails',
      build: () => RestoresBloc(
        getRestores: getRestores,
        cancelRestore: cancelRestore,
      ),
      act: (RestoresBloc bloc) => bloc.add(LoadRequests()),
      expect: () => [Loading(), Error(message: 'ServerFailure(oh no!)')],
    );
  });
}
