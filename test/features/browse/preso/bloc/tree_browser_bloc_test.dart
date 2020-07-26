//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:bloc_test/bloc_test.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:mockito/mockito.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/tree.dart';
import 'package:zorigami/core/domain/repositories/snapshot_repository.dart';
import 'package:zorigami/core/domain/repositories/tree_repository.dart';
import 'package:zorigami/core/domain/usecases/get_tree.dart';
import 'package:zorigami/core/domain/usecases/restore_file.dart';
import 'package:zorigami/core/error/failures.dart';
import 'package:zorigami/features/browse/preso/bloc/tree_browser_bloc.dart';

class MockTreeRepository extends Mock implements TreeRepository {}

class MockSnapshotRepository extends Mock implements SnapshotRepository {}

void main() {
  MockTreeRepository mockTreeRepository;
  MockSnapshotRepository mockSnapshotRepository;
  GetTree getTree;
  RestoreFile restoreFile;

  final tTree1 = Tree(
    entries: [
      TreeEntry(
        name: 'file1',
        modTime: DateTime.utc(2018, 5, 7, 3, 52, 44),
        reference: TreeReference(
          type: EntryType.file,
          value: 'sha256-8c983bd',
        ),
      ),
      TreeEntry(
        name: 'folder1',
        modTime: DateTime.utc(2017, 5, 7, 3, 52, 44),
        reference: TreeReference(
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
        reference: TreeReference(
          type: EntryType.file,
          value: 'sha256-89c8b3d',
        ),
      ),
      TreeEntry(
        name: 'folder2',
        modTime: DateTime.utc(2017, 5, 7, 3, 52, 44),
        reference: TreeReference(
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
        reference: TreeReference(
          type: EntryType.file,
          value: 'sha256-98c8bd3',
        ),
      ),
      TreeEntry(
        name: 'folder3',
        modTime: DateTime.utc(2017, 5, 7, 3, 52, 44),
        reference: TreeReference(
          type: EntryType.tree,
          value: 'sha1-beefcafe',
        ),
      ),
    ],
  );

  group('normal cases', () {
    setUp(() {
      mockTreeRepository = MockTreeRepository();
      getTree = GetTree(mockTreeRepository);
      when(mockTreeRepository.getTree('sha1-cafebabe'))
          .thenAnswer((_) async => Ok(tTree1));
      when(mockTreeRepository.getTree('sha1-cafed00d'))
          .thenAnswer((_) async => Ok(tTree2));
      when(mockTreeRepository.getTree('sha1-deadbeef'))
          .thenAnswer((_) async => Ok(tTree3));
    });

    blocTest(
      'emits [] when nothing is added',
      build: () => TreeBrowserBloc(getTree: getTree),
      expect: [],
    );

    blocTest(
      'emits updated state when LoadTree is added',
      build: () => TreeBrowserBloc(getTree: getTree),
      act: (bloc) => bloc.add(LoadTree(digest: 'sha1-cafebabe')),
      expect: [Loading(), Loaded(tree: tTree1, selections: [], path: [])],
    );

    blocTest(
      'selects an entry when SetSelection is added',
      build: () => TreeBrowserBloc(getTree: getTree),
      act: (bloc) {
        bloc.add(LoadTree(digest: 'sha1-cafebabe'));
        bloc.add(SetSelection(entry: tTree1.entries[0], selected: true));
        return;
      },
      expect: [
        Loading(),
        Loaded(tree: tTree1, selections: [], path: []),
        Loaded(tree: tTree1, selections: [tTree1.entries[0]], path: []),
      ],
    );

    blocTest(
      'toggle entry selection on and off',
      build: () => TreeBrowserBloc(getTree: getTree),
      act: (bloc) {
        bloc.add(LoadTree(digest: 'sha1-cafebabe'));
        bloc.add(SetSelection(entry: tTree1.entries[0], selected: true));
        bloc.add(SetSelection(entry: tTree1.entries[0], selected: false));
        return;
      },
      expect: [
        Loading(),
        Loaded(tree: tTree1, selections: [], path: []),
        Loaded(tree: tTree1, selections: [tTree1.entries[0]], path: []),
        Loaded(tree: tTree1, selections: [], path: []),
      ],
    );

    blocTest(
      'selection emitted once for multiple identical SetSelection events',
      build: () => TreeBrowserBloc(getTree: getTree),
      act: (bloc) {
        bloc.add(LoadTree(digest: 'sha1-cafebabe'));
        bloc.add(SetSelection(entry: tTree1.entries[0], selected: true));
        bloc.add(SetSelection(entry: tTree1.entries[0], selected: true));
        bloc.add(SetSelection(entry: tTree1.entries[0], selected: true));
        return;
      },
      expect: [
        Loading(),
        Loaded(tree: tTree1, selections: [], path: []),
        Loaded(tree: tTree1, selections: [tTree1.entries[0]], path: []),
      ],
    );

    blocTest(
      'should emit nothing when LoadEntry is added without a tree',
      build: () => TreeBrowserBloc(getTree: getTree),
      act: (bloc) => bloc.add(LoadEntry(entry: tTree1.entries[1])),
      expect: [],
    );

    blocTest(
      'clears selections when LoadEntry is added',
      build: () => TreeBrowserBloc(getTree: getTree),
      act: (bloc) {
        bloc.add(LoadTree(digest: 'sha1-cafebabe'));
        bloc.add(SetSelection(entry: tTree1.entries[0], selected: true));
        bloc.add(LoadEntry(entry: tTree1.entries[1]));
        return;
      },
      expect: [
        Loading(),
        Loaded(tree: tTree1, selections: [], path: []),
        Loaded(tree: tTree1, selections: [tTree1.entries[0]], path: []),
        Loading(),
        Loaded(tree: tTree2, selections: [], path: ['folder1']),
      ],
      verify: (TreeBrowserBloc bloc) async {
        expect(bloc.path.length, equals(1));
        expect(bloc.history.length, equals(2));
      },
    );

    blocTest(
      'clears state when LoadTree is added',
      build: () => TreeBrowserBloc(getTree: getTree),
      act: (bloc) {
        bloc.add(LoadTree(digest: 'sha1-cafebabe'));
        bloc.add(SetSelection(entry: tTree1.entries[0], selected: true));
        bloc.add(LoadTree(digest: 'sha1-cafed00d'));
        return;
      },
      expect: [
        Loading(),
        Loaded(tree: tTree1, selections: [], path: []),
        Loaded(tree: tTree1, selections: [tTree1.entries[0]], path: []),
        Loading(),
        Loaded(tree: tTree2, selections: [], path: []),
      ],
    );

    blocTest(
      'clears state when ResetTree is added',
      build: () => TreeBrowserBloc(getTree: getTree),
      act: (bloc) {
        bloc.add(LoadTree(digest: 'sha1-cafebabe'));
        bloc.add(SetSelection(entry: tTree1.entries[0], selected: true));
        bloc.add(ResetTree());
        return;
      },
      expect: [
        Loading(),
        Loaded(tree: tTree1, selections: [], path: []),
        Loaded(tree: tTree1, selections: [tTree1.entries[0]], path: []),
        Empty(),
      ],
    );

    blocTest(
      'should emit nothing when NavigateUpward is added without history',
      build: () => TreeBrowserBloc(getTree: getTree),
      act: (bloc) => bloc.add(NavigateUpward()),
      expect: [],
    );

    blocTest(
      'should pop history when NavigateUpward is added',
      build: () => TreeBrowserBloc(getTree: getTree),
      act: (bloc) {
        bloc.add(LoadTree(digest: 'sha1-cafebabe'));
        bloc.add(LoadEntry(entry: tTree1.entries[1]));
        bloc.add(NavigateUpward());
        return;
      },
      expect: [
        Loading(),
        Loaded(tree: tTree1, selections: [], path: []),
        Loading(),
        Loaded(tree: tTree2, selections: [], path: ['folder1']),
        Loading(),
        Loaded(tree: tTree1, selections: [], path: []),
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
      restoreFile = RestoreFile(mockSnapshotRepository);
      when(mockTreeRepository.getTree('sha1-cafebabe'))
          .thenAnswer((_) async => Ok(tTree1));
      when(mockSnapshotRepository.restoreFile(
              'sha256-8c983bd', 'file1', 'dataset1'))
          .thenAnswer((_) async => Ok('file1'));
    });

    blocTest(
      'emits [Loading, Loaded, ...] when RestoreSelections is added',
      build: () => TreeBrowserBloc(
        getTree: getTree,
        restoreFile: restoreFile,
      ),
      act: (bloc) {
        bloc.add(LoadTree(digest: 'sha1-cafebabe'));
        bloc.add(SetSelection(entry: tTree1.entries[0], selected: true));
        bloc.add(RestoreSelections(datasetKey: 'dataset1'));
        return;
      },
      expect: [
        Loading(),
        Loaded(tree: tTree1, selections: [], path: []),
        Loaded(tree: tTree1, selections: [tTree1.entries[0]], path: []),
        Loaded(
          tree: tTree1,
          selections: [],
          path: [],
          restoreResult: Ok('file1'),
        ),
      ],
    );
  });

  group('error cases', () {
    setUp(() {
      mockTreeRepository = MockTreeRepository();
      getTree = GetTree(mockTreeRepository);
      when(mockTreeRepository.getTree(any))
          .thenAnswer((_) async => Err(ServerFailure('oh no!')));
    });

    blocTest(
      'emits [Loading, Error] when LoadTree is added',
      build: () => TreeBrowserBloc(getTree: getTree),
      act: (bloc) => bloc.add(LoadTree(digest: 'sha1-cafebabe')),
      expect: [Loading(), Error(message: 'ServerFailure(oh no!)')],
    );
  });
}
