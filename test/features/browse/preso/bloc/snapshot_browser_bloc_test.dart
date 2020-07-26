//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:bloc_test/bloc_test.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:mockito/mockito.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/snapshot.dart';
import 'package:zorigami/core/domain/repositories/snapshot_repository.dart';
import 'package:zorigami/core/domain/usecases/get_snapshot.dart';
import 'package:zorigami/core/error/failures.dart';
import 'package:zorigami/features/browse/preso/bloc/snapshot_browser_bloc.dart';

class MockSnapshotRepository extends Mock implements SnapshotRepository {}

void main() {
  MockSnapshotRepository mockSnapshotRepository;
  GetSnapshot usecase;

  final tSubsequent = Snapshot(
    checksum: 'cafebabe',
    parent: Some('cafed00d'),
    startTime: DateTime.now(),
    endTime: None(),
    fileCount: 101,
    tree: 'deadbeef',
  );

  final tParent = Snapshot(
    checksum: 'cafed00d',
    parent: None(),
    startTime: DateTime.now(),
    endTime: Some(DateTime.now()),
    fileCount: 121,
    tree: 'beefdead',
  );

  group('normal cases', () {
    setUp(() {
      mockSnapshotRepository = MockSnapshotRepository();
      usecase = GetSnapshot(mockSnapshotRepository);
      when(mockSnapshotRepository.getSnapshot('cafebabe'))
          .thenAnswer((_) async => Ok(tSubsequent));
      when(mockSnapshotRepository.getSnapshot('cafed00d'))
          .thenAnswer((_) async => Ok(tParent));
    });

    test('ensure Loaded implements Equatable', () {
      expect(
        Loaded(snapshot: tSubsequent),
        equals(Loaded(snapshot: tSubsequent)),
      );
      expect(
        Loaded(snapshot: tParent),
        isNot(equals(Loaded(snapshot: tSubsequent))),
      );
    });

    blocTest(
      'emits [] when nothing is added',
      build: () => SnapshotBrowserBloc(usecase: usecase),
      expect: [],
    );

    blocTest(
      'emits [Loading, Loaded] when LoadSnapshot is added',
      build: () => SnapshotBrowserBloc(usecase: usecase),
      act: (bloc) => bloc.add(LoadSnapshot(digest: 'cafebabe')),
      expect: [Loading(), Loaded(snapshot: tSubsequent)],
    );

    blocTest(
      'should support moving forward and backward',
      build: () => SnapshotBrowserBloc(usecase: usecase),
      act: (bloc) {
        bloc.add(LoadSnapshot(digest: 'cafebabe'));
        bloc.add(LoadParent());
        // will not move past the end
        bloc.add(LoadParent());
        bloc.add(LoadParent());
        bloc.add(LoadSubsequent());
        // will not move past the beginning
        bloc.add(LoadSubsequent());
        bloc.add(LoadSubsequent());
        return;
      },
      expect: [
        Loading(),
        Loaded(snapshot: tSubsequent),
        Loading(),
        Loaded(snapshot: tParent, hasSubsequent: true),
        Loading(),
        Loaded(snapshot: tSubsequent),
      ],
      verify: (bloc) async {
        expect(bloc.history.length, equals(0));
      },
    );
  });

  group('error cases', () {
    setUp(() {
      mockSnapshotRepository = MockSnapshotRepository();
      usecase = GetSnapshot(mockSnapshotRepository);
      when(mockSnapshotRepository.getSnapshot(any))
          .thenAnswer((_) async => Err(ServerFailure('oh no!')));
    });

    blocTest(
      'emits [Loading, Error] when LoadSnapshot is added',
      build: () => SnapshotBrowserBloc(usecase: usecase),
      act: (bloc) => bloc.add(LoadSnapshot(digest: 'cafebabe')),
      expect: [Loading(), Error(message: 'ServerFailure(oh no!)')],
    );
  });
}
