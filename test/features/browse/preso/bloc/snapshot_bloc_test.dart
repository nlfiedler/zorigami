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
import 'package:zorigami/features/browse/preso/bloc/snapshot_bloc.dart';

class MockSnapshotRepository extends Mock implements SnapshotRepository {}

void main() {
  MockSnapshotRepository mockSnapshotRepository;
  GetSnapshot usecase;

  final tSnapshot = Snapshot(
    checksum: 'cafebabe',
    parent: None(),
    startTime: DateTime.now(),
    endTime: None(),
    fileCount: 101,
    tree: 'deadbeef',
  );

  setUp(() {
    mockSnapshotRepository = MockSnapshotRepository();
    usecase = GetSnapshot(mockSnapshotRepository);
    when(mockSnapshotRepository.getSnapshot(any))
        .thenAnswer((_) async => Ok(tSnapshot));
  });

  group('SnapshotBloc', () {
    blocTest(
      'emits [] when nothing is added',
      build: () async => SnapshotBloc(usecase: usecase),
      expect: [],
    );

    blocTest(
      'emits [Loading, Loaded] when LoadAllDataSets is added',
      build: () async => SnapshotBloc(usecase: usecase),
      act: (bloc) => bloc.add(LoadSnapshot(digest: 'cafebabe')),
      expect: [Loading(), Loaded(snapshot: tSnapshot)],
    );
  });
}
