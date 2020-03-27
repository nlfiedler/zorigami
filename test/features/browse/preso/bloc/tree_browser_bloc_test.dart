//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:bloc_test/bloc_test.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:zorigami/core/domain/entities/tree.dart';
import 'package:zorigami/features/browse/preso/bloc/tree_browser_bloc.dart';

void main() {
  group('TreeBrowserBloc', () {
    final tTreeEntry1 = TreeEntry(
      name: 'subfolder',
      modTime: DateTime.now(),
      reference: TreeReference(
        type: EntryType.tree,
        value: 'sha1-cafebabe',
      ),
    );

    final tTreeEntry2 = TreeEntry(
      name: 'foldersub',
      modTime: DateTime.now(),
      reference: TreeReference(
        type: EntryType.tree,
        value: 'sha1-cafed00d',
      ),
    );

    final tFileEntry = TreeEntry(
      name: 'filename.ext',
      modTime: DateTime.now(),
      reference: TreeReference(
        type: EntryType.file,
        value: 'sha1-deadbeef',
      ),
    );

    blocTest(
      'emits [] when nothing is added',
      build: () async => TreeBrowserBloc(),
      expect: [],
    );

    blocTest(
      'emits updated state when VisitTree is added',
      build: () async => TreeBrowserBloc(),
      act: (bloc) => bloc.add(VisitTree(entry: tTreeEntry1)),
      expect: [
        TreeBrowserState(
          history: ['sha1-cafebabe'],
          path: ['subfolder'],
          selections: [],
        )
      ],
    );

    blocTest(
      'emits updated state when ToggleSelection is added',
      build: () async => TreeBrowserBloc(),
      act: (bloc) => bloc.add(ToggleSelection(entry: tFileEntry)),
      expect: [
        TreeBrowserState(
          history: [],
          path: [],
          selections: [tFileEntry],
        )
      ],
    );

    blocTest(
      'toggles on and off when ToggleSelection is added twice',
      build: () async => TreeBrowserBloc(),
      act: (bloc) {
        bloc.add(ToggleSelection(entry: tFileEntry));
        bloc.add(ToggleSelection(entry: tFileEntry));
        return;
      },
      expect: [
        TreeBrowserState(
          history: [],
          path: [],
          selections: [tFileEntry],
        ),
        TreeBrowserState(
          history: [],
          path: [],
          selections: [],
        )
      ],
    );

    blocTest(
      'clears selections when VisitTree is added',
      build: () async => TreeBrowserBloc(),
      act: (bloc) {
        bloc.add(ToggleSelection(entry: tFileEntry));
        bloc.add(VisitTree(entry: tTreeEntry1));
        return;
      },
      expect: [
        TreeBrowserState(
          history: [],
          path: [],
          selections: [tFileEntry],
        ),
        TreeBrowserState(
          history: ['sha1-cafebabe'],
          path: ['subfolder'],
          selections: [],
        )
      ],
    );

    blocTest(
      'clears state when StartNewTree is added',
      build: () async => TreeBrowserBloc(),
      act: (bloc) {
        bloc.add(VisitTree(entry: tTreeEntry1));
        bloc.add(ToggleSelection(entry: tFileEntry));
        bloc.add(StartNewTree());
        return;
      },
      expect: [
        TreeBrowserState(
          history: ['sha1-cafebabe'],
          path: ['subfolder'],
          selections: [],
        ),
        TreeBrowserState(
          history: ['sha1-cafebabe'],
          path: ['subfolder'],
          selections: [tFileEntry],
        ),
        TreeBrowserState(
          history: [],
          path: [],
          selections: [],
        )
      ],
    );

    blocTest(
      'should emit nothing when NavigateUpward is added without history',
      build: () async => TreeBrowserBloc(),
      act: (bloc) => bloc.add(NavigateUpward()),
      expect: [],
    );

    blocTest(
      'should pop history when NavigateUpward is added',
      build: () async => TreeBrowserBloc(),
      act: (bloc) {
        bloc.add(VisitTree(entry: tTreeEntry1));
        bloc.add(VisitTree(entry: tTreeEntry2));
        bloc.add(NavigateUpward());
        return;
      },
      expect: [
        TreeBrowserState(
          history: ['sha1-cafebabe'],
          path: ['subfolder'],
          selections: [],
        ),
        TreeBrowserState(
          history: ['sha1-cafebabe', 'sha1-cafed00d'],
          path: ['subfolder', 'foldersub'],
          selections: [],
        ),
        TreeBrowserState(
          history: ['sha1-cafebabe'],
          path: ['subfolder'],
          selections: [],
        )
      ],
    );
  });
}
