//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:bloc_test/bloc_test.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:mockito/mockito.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/tree.dart';
import 'package:zorigami/core/domain/repositories/tree_repository.dart';
import 'package:zorigami/core/domain/usecases/get_tree.dart';
import 'package:zorigami/features/browse/preso/bloc/tree_browser_bloc.dart';

class MockSnapshotRepository extends Mock implements TreeRepository {}

void main() {
  MockSnapshotRepository mockSnapshotRepository;
  GetTree usecase;

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

  setUp(() {
    mockSnapshotRepository = MockSnapshotRepository();
    usecase = GetTree(mockSnapshotRepository);
    when(mockSnapshotRepository.getTree('sha1-cafebabe'))
        .thenAnswer((_) async => Ok(tTree1));
    when(mockSnapshotRepository.getTree('sha1-cafed00d'))
        .thenAnswer((_) async => Ok(tTree2));
    when(mockSnapshotRepository.getTree('sha1-deadbeef'))
        .thenAnswer((_) async => Ok(tTree3));
  });

  group('TreeBrowserBloc', () {
    blocTest(
      'emits [] when nothing is added',
      build: () async => TreeBrowserBloc(usecase: usecase),
      expect: [],
    );

    blocTest(
      'emits updated state when LoadTree is added',
      build: () async => TreeBrowserBloc(usecase: usecase),
      act: (bloc) => bloc.add(LoadTree(digest: 'sha1-cafebabe')),
      expect: [Loading(), Loaded(tree: tTree1, selections: [], path: [])],
    );

    blocTest(
      'selects an entry when SetSelection is added',
      build: () async => TreeBrowserBloc(usecase: usecase),
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
      build: () async => TreeBrowserBloc(usecase: usecase),
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
      build: () async => TreeBrowserBloc(usecase: usecase),
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
      build: () async => TreeBrowserBloc(usecase: usecase),
      act: (bloc) => bloc.add(LoadEntry(entry: tTree1.entries[1])),
      expect: [],
    );

    blocTest(
      'clears selections when LoadEntry is added',
      build: () async => TreeBrowserBloc(usecase: usecase),
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
      build: () async => TreeBrowserBloc(usecase: usecase),
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
      build: () async => TreeBrowserBloc(usecase: usecase),
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
      build: () async => TreeBrowserBloc(usecase: usecase),
      act: (bloc) => bloc.add(NavigateUpward()),
      expect: [],
    );

    blocTest(
      'should pop history when NavigateUpward is added',
      build: () async => TreeBrowserBloc(usecase: usecase),
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
}
