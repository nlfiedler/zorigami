//
// Copyright (c) 2022 Nathan Fiedler
//
import 'package:bloc_test/bloc_test.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:mocktail/mocktail.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/tree.dart';
import 'package:zorigami/core/domain/repositories/snapshot_repository.dart';
import 'package:zorigami/core/domain/repositories/tree_repository.dart';
import 'package:zorigami/core/domain/usecases/get_tree.dart';
import 'package:zorigami/core/domain/usecases/restore_files.dart';
import 'package:zorigami/core/error/failures.dart';
import 'package:zorigami/features/browse/preso/bloc/tree_browser_bloc.dart';

class MockSnapshotRepository extends Mock implements SnapshotRepository {}

class MockTreeRepository extends Mock implements TreeRepository {}

void main() {
  late MockTreeRepository mockTreeRepository;
  late MockSnapshotRepository mockSnapshotRepository;
  late GetTree getTree;
  late RestoreFiles restoreFiles;

  final tTree1 = Tree(
    entries: [
      TreeEntry(
        name: 'file1',
        modTime: DateTime.utc(2018, 5, 7, 3, 52, 44),
        reference: const TreeReference(
          type: EntryType.file,
          value: 'blake3-8c983bd',
        ),
      ),
      TreeEntry(
        name: 'folder1',
        modTime: DateTime.utc(2017, 5, 7, 3, 52, 44),
        reference: const TreeReference(
          type: EntryType.tree,
          value: 'sha1-cafed00d',
        ),
      ),
    ],
  );

  final tTree2 = Tree(
    entries: [
      TreeEntry(
        name: 'file2',
        modTime: DateTime.utc(2019, 5, 7, 3, 52, 44),
        reference: const TreeReference(
          type: EntryType.file,
          value: 'blake3-89c8b3d',
        ),
      ),
      TreeEntry(
        name: 'folder2',
        modTime: DateTime.utc(2017, 5, 7, 3, 52, 44),
        reference: const TreeReference(
          type: EntryType.tree,
          value: 'sha1-deadbeef',
        ),
      ),
    ],
  );

  final tTree3 = Tree(
    entries: [
      TreeEntry(
        name: 'file3',
        modTime: DateTime.utc(2020, 5, 7, 3, 52, 44),
        reference: const TreeReference(
          type: EntryType.file,
          value: 'blake3-98c8bd3',
        ),
      ),
      TreeEntry(
        name: 'folder3',
        modTime: DateTime.utc(2017, 5, 7, 3, 52, 44),
        reference: const TreeReference(
          type: EntryType.tree,
          value: 'sha1-beefcafe',
        ),
      ),
    ],
  );

  group('normal cases', () {
    setUp(() {
      mockTreeRepository = MockTreeRepository();
      mockSnapshotRepository = MockSnapshotRepository();
      getTree = GetTree(mockTreeRepository);
      restoreFiles = RestoreFiles(mockSnapshotRepository);
      when(() => mockTreeRepository.getTree('sha1-cafebabe'))
          .thenAnswer((_) async => Ok(tTree1));
      when(() => mockTreeRepository.getTree('sha1-cafed00d'))
          .thenAnswer((_) async => Ok(tTree2));
      when(() => mockTreeRepository.getTree('sha1-deadbeef'))
          .thenAnswer((_) async => Ok(tTree3));
    });

    blocTest(
      'emits [] when nothing is added',
      build: () => TreeBrowserBloc(
        getTree: getTree,
        restoreFiles: restoreFiles,
      ),
      expect: () => [],
    );

    blocTest(
      'emits updated state when LoadTree is added',
      build: () => TreeBrowserBloc(
        getTree: getTree,
        restoreFiles: restoreFiles,
      ),
      act: (TreeBrowserBloc bloc) =>
          bloc.add(LoadTree(digest: 'sha1-cafebabe')),
      expect: () => [
        Loading(),
        Loaded(tree: tTree1, selections: const [], path: const [])
      ],
    );

    blocTest(
      'selects an entry when SetSelection is added',
      build: () => TreeBrowserBloc(
        getTree: getTree,
        restoreFiles: restoreFiles,
      ),
      act: (TreeBrowserBloc bloc) {
        bloc.add(LoadTree(digest: 'sha1-cafebabe'));
        bloc.add(SetSelection(entry: tTree1.entries[0], selected: true));
        return;
      },
      expect: () => [
        Loading(),
        Loaded(tree: tTree1, selections: const [], path: const []),
        Loaded(tree: tTree1, selections: [tTree1.entries[0]], path: const []),
      ],
    );

    blocTest(
      'toggle entry selection on and off',
      build: () => TreeBrowserBloc(
        getTree: getTree,
        restoreFiles: restoreFiles,
      ),
      act: (TreeBrowserBloc bloc) {
        bloc.add(LoadTree(digest: 'sha1-cafebabe'));
        bloc.add(SetSelection(entry: tTree1.entries[0], selected: true));
        bloc.add(SetSelection(entry: tTree1.entries[0], selected: false));
        return;
      },
      expect: () => [
        Loading(),
        Loaded(tree: tTree1, selections: const [], path: const []),
        Loaded(tree: tTree1, selections: [tTree1.entries[0]], path: const []),
        Loaded(tree: tTree1, selections: const [], path: const []),
      ],
    );

    blocTest(
      'selection emitted once for multiple identical SetSelection events',
      build: () => TreeBrowserBloc(
        getTree: getTree,
        restoreFiles: restoreFiles,
      ),
      act: (TreeBrowserBloc bloc) {
        bloc.add(LoadTree(digest: 'sha1-cafebabe'));
        bloc.add(SetSelection(entry: tTree1.entries[0], selected: true));
        bloc.add(SetSelection(entry: tTree1.entries[0], selected: true));
        bloc.add(SetSelection(entry: tTree1.entries[0], selected: true));
        return;
      },
      expect: () => [
        Loading(),
        Loaded(tree: tTree1, selections: const [], path: const []),
        Loaded(tree: tTree1, selections: [tTree1.entries[0]], path: const []),
      ],
    );

    blocTest(
      'should emit nothing when LoadEntry is added without a tree',
      build: () => TreeBrowserBloc(
        getTree: getTree,
        restoreFiles: restoreFiles,
      ),
      act: (TreeBrowserBloc bloc) =>
          bloc.add(LoadEntry(entry: tTree1.entries[1])),
      expect: () => [],
    );

    blocTest(
      'clears selections when LoadEntry is added',
      build: () => TreeBrowserBloc(
        getTree: getTree,
        restoreFiles: restoreFiles,
      ),
      act: (TreeBrowserBloc bloc) {
        bloc.add(LoadTree(digest: 'sha1-cafebabe'));
        bloc.add(SetSelection(entry: tTree1.entries[0], selected: true));
        bloc.add(LoadEntry(entry: tTree1.entries[1]));
        return;
      },
      expect: () => [
        Loading(),
        Loaded(tree: tTree1, selections: const [], path: const []),
        Loaded(tree: tTree1, selections: [tTree1.entries[0]], path: const []),
        Loading(),
        Loaded(tree: tTree2, selections: const [], path: const ['folder1']),
      ],
      verify: (TreeBrowserBloc bloc) async {
        expect(bloc.path.length, equals(1));
        expect(bloc.history.length, equals(2));
      },
    );

    blocTest(
      'clears state when LoadTree is added',
      build: () => TreeBrowserBloc(
        getTree: getTree,
        restoreFiles: restoreFiles,
      ),
      act: (TreeBrowserBloc bloc) {
        bloc.add(LoadTree(digest: 'sha1-cafebabe'));
        bloc.add(SetSelection(entry: tTree1.entries[0], selected: true));
        bloc.add(LoadTree(digest: 'sha1-cafed00d'));
        return;
      },
      expect: () => [
        Loading(),
        Loaded(tree: tTree1, selections: const [], path: const []),
        Loaded(tree: tTree1, selections: [tTree1.entries[0]], path: const []),
        Loading(),
        Loaded(tree: tTree2, selections: const [], path: const []),
      ],
    );

    blocTest(
      'clears state when ResetTree is added',
      build: () => TreeBrowserBloc(
        getTree: getTree,
        restoreFiles: restoreFiles,
      ),
      act: (TreeBrowserBloc bloc) {
        bloc.add(LoadTree(digest: 'sha1-cafebabe'));
        bloc.add(SetSelection(entry: tTree1.entries[0], selected: true));
        bloc.add(ResetTree());
        return;
      },
      expect: () => [
        Loading(),
        Loaded(tree: tTree1, selections: const [], path: const []),
        Loaded(tree: tTree1, selections: [tTree1.entries[0]], path: const []),
        Empty(),
      ],
    );

    blocTest(
      'should emit nothing when NavigateUpward is added without history',
      build: () => TreeBrowserBloc(
        getTree: getTree,
        restoreFiles: restoreFiles,
      ),
      act: (TreeBrowserBloc bloc) => bloc.add(NavigateUpward()),
      expect: () => [],
    );

    blocTest(
      'should pop history when NavigateUpward is added',
      build: () => TreeBrowserBloc(
        getTree: getTree,
        restoreFiles: restoreFiles,
      ),
      act: (TreeBrowserBloc bloc) {
        bloc.add(LoadTree(digest: 'sha1-cafebabe'));
        bloc.add(LoadEntry(entry: tTree1.entries[1]));
        bloc.add(NavigateUpward());
        return;
      },
      expect: () => [
        Loading(),
        Loaded(tree: tTree1, selections: const [], path: const []),
        Loading(),
        Loaded(tree: tTree2, selections: const [], path: const ['folder1']),
        Loading(),
        Loaded(tree: tTree1, selections: const [], path: const []),
      ],
      verify: (TreeBrowserBloc bloc) async {
        expect(bloc.path.length, equals(0));
        expect(bloc.history.length, equals(1));
      },
    );
  });

  group('file restoration', () {
    setUp(() {
      mockTreeRepository = MockTreeRepository();
      mockSnapshotRepository = MockSnapshotRepository();
      getTree = GetTree(mockTreeRepository);
      restoreFiles = RestoreFiles(mockSnapshotRepository);
      when(() => mockTreeRepository.getTree('sha1-cafebabe'))
          .thenAnswer((_) async => Ok(tTree1));
      when(() => mockSnapshotRepository.restoreFiles(
              'sha1-cafebabe', 'file1', 'file1', 'dataset1'))
          .thenAnswer((_) async => Ok(true));
    });

    blocTest(
      'emits [Loading, Loaded, ...] when RestoreSelections is added',
      build: () => TreeBrowserBloc(
        getTree: getTree,
        restoreFiles: restoreFiles,
      ),
      act: (TreeBrowserBloc bloc) {
        bloc.add(LoadTree(digest: 'sha1-cafebabe'));
        bloc.add(SetSelection(entry: tTree1.entries[0], selected: true));
        bloc.add(RestoreSelections(datasetKey: 'dataset1'));
        return;
      },
      expect: () => [
        Loading(),
        Loaded(tree: tTree1, selections: const [], path: const []),
        Loaded(tree: tTree1, selections: [tTree1.entries[0]], path: const []),
        Loaded(
          tree: tTree1,
          selections: const [],
          path: const [],
        ),
      ],
    );
  });

  group('error cases', () {
    setUp(() {
      mockTreeRepository = MockTreeRepository();
      mockSnapshotRepository = MockSnapshotRepository();
      getTree = GetTree(mockTreeRepository);
      restoreFiles = RestoreFiles(mockSnapshotRepository);
      when(() => mockTreeRepository.getTree(any()))
          .thenAnswer((_) async => Err(ServerFailure('oh no!')));
    });

    blocTest(
      'emits [Loading, Error] when LoadTree is added',
      build: () => TreeBrowserBloc(
        getTree: getTree,
        restoreFiles: restoreFiles,
      ),
      act: (TreeBrowserBloc bloc) =>
          bloc.add(LoadTree(digest: 'sha1-cafebabe')),
      expect: () => [Loading(), Error(message: 'ServerFailure(oh no!)')],
    );
  });
}
