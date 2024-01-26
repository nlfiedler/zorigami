//
// Copyright (c) 2024 Nathan Fiedler
//
import 'dart:async';
import 'package:bloc/bloc.dart';
import 'package:bloc_concurrency/bloc_concurrency.dart';
import 'package:equatable/equatable.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/tree.dart';
import 'package:zorigami/core/domain/usecases/get_tree.dart' as gt;
import 'package:zorigami/core/domain/usecases/restore_files.dart' as rf;

//
// events
//

abstract class TreeBrowserEvent extends Equatable {
  @override
  List<Object> get props => [];
}

class LoadTree extends TreeBrowserEvent {
  final String digest;

  LoadTree({required this.digest});
}

class LoadEntry extends TreeBrowserEvent {
  final TreeEntry entry;

  LoadEntry({required this.entry});
}

class SetSelection extends TreeBrowserEvent {
  final TreeEntry entry;
  final bool selected;

  SetSelection({required this.entry, required this.selected});
}

class RestoreSelections extends TreeBrowserEvent {
  final String datasetKey;

  RestoreSelections({required this.datasetKey});
}

class NavigateUpward extends TreeBrowserEvent {}

class ResetTree extends TreeBrowserEvent {}

//
// states
//

abstract class TreeBrowserState extends Equatable {
  @override
  List<Object> get props => [];
}

class Empty extends TreeBrowserState {}

class Loading extends TreeBrowserState {}

class Loaded extends TreeBrowserState {
  final Tree tree;
  // path hierarchy (not including the root)
  final List<String> path;
  // list of selected entries
  final List<TreeEntry> selections;
  // true if any restore requests have been enqueued
  final bool restoresEnqueued;

  Loaded({
    required this.tree,
    required selections,
    required path,
    this.restoresEnqueued = false,
  })  : selections = List.unmodifiable(selections),
        path = List.unmodifiable(path);

  @override
  List<Object> get props => [tree, selections];

  @override
  bool get stringify => true;
}

class Error extends TreeBrowserState {
  final String message;

  Error({required this.message});

  @override
  List<Object> get props => [message];

  @override
  bool get stringify => true;
}

//
// bloc
//

class TreeBrowserBloc extends Bloc<TreeBrowserEvent, TreeBrowserState> {
  final gt.GetTree getTree;
  final rf.RestoreFiles restoreFiles;
  // tree checksums in hierarchy order (added on load)
  final List<String> history = [];
  // entry names in hierarchy order ("root" is not included)
  final List<String> path = [];
  // selected tree entries
  final List<TreeEntry> selections = [];

  TreeBrowserBloc({required this.getTree, required this.restoreFiles})
      : super(Empty()) {
    // enforce sequential ordering of event mapping due to the asynchronous
    // nature of this particular bloc
    on<TreeBrowserEvent>(_onEvent, transformer: sequential());
  }

  FutureOr<void> _onEvent(
    TreeBrowserEvent event,
    Emitter<TreeBrowserState> emit,
  ) async {
    if (event is LoadEntry) {
      if (state is Loaded) {
        path.add(event.entry.name);
        return _loadTree(event.entry.reference.value, emit);
      }
    } else if (event is SetSelection) {
      if (state is Loaded) {
        final tree = (state as Loaded).tree;
        // for both adding and removing the selection, start by removing it,
        // then adding it only if selected, ensuring it is added only once
        selections.remove(event.entry);
        if (event.selected) {
          selections.add(event.entry);
        }
        emit(Loaded(tree: tree, selections: selections, path: path));
      }
    } else if (event is NavigateUpward) {
      // The path list has one less entry than the history, as it does not
      // account for the root tree, so if it is not empty then it is still
      // possible to navigate upward. The last entry in the history is the
      // currently viewed tree, which includes the root tree.
      if (state is Loaded && path.isNotEmpty) {
        // remove this tree and the one we are about to load
        history.removeLast();
        final digest = history.removeLast();
        path.removeLast();
        return _loadTree(digest, emit);
      }
    } else if (event is LoadTree) {
      history.clear();
      path.clear();
      return _loadTree(event.digest, emit);
    } else if (event is ResetTree) {
      // Something else has happened (i.e. navigating snapshots) outside of this
      // bloc that requires signaling the consumers of the change.
      emit(Empty());
    } else if (event is RestoreSelections) {
      if (state is Loaded) {
        final tree = (state as Loaded).tree;
        var restoresEnqueued = false;
        while (selections.isNotEmpty) {
          final entry = selections.removeLast();
          final filepath =
              path.isEmpty ? entry.name : '${path.join('/')}/${entry.name}';
          final params = rf.Params(
            tree: history.last,
            entry: entry.name,
            filepath: filepath,
            dataset: event.datasetKey,
          );
          final result = await restoreFiles(params);
          if (result is Ok) {
            restoresEnqueued |= result.unwrap();
          }
        }
        emit(Loaded(
          tree: tree,
          selections: selections,
          path: path,
          restoresEnqueued: restoresEnqueued,
        ));
      }
    }
  }

  Future<void> _loadTree(String digest, Emitter<TreeBrowserState> emit) async {
    selections.clear();
    emit(Loading());
    final result = await getTree(gt.Params(checksum: digest));
    emit(result.mapOrElse(
      (tree) {
        history.add(digest);
        return Loaded(tree: tree, selections: selections, path: path);
      },
      (failure) => Error(message: failure.toString()),
    ));
  }
}
