//
// Copyright (c) 2020 Nathan Fiedler
//
import 'dart:async';
import 'package:bloc/bloc.dart';
import 'package:equatable/equatable.dart';
import 'package:meta/meta.dart';
import 'package:zorigami/core/domain/entities/tree.dart';
import 'package:zorigami/core/domain/usecases/get_tree.dart';

//
// events
//

abstract class TreeBrowserEvent extends Equatable {
  @override
  List<Object> get props => [];
}

class LoadTree extends TreeBrowserEvent {
  final String digest;

  LoadTree({@required this.digest});
}

class LoadEntry extends TreeBrowserEvent {
  final TreeEntry entry;

  LoadEntry({@required this.entry});
}

class ToggleSelection extends TreeBrowserEvent {
  final TreeEntry entry;

  ToggleSelection({@required this.entry});
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

  Loaded({@required this.tree, @required selections, @required path})
      : selections = List.unmodifiable(selections),
        path = List.unmodifiable(path);

  @override
  List<Object> get props => [tree, selections];

  @override
  bool get stringify => true;
}

class Error extends TreeBrowserState {
  final String message;

  Error({@required this.message});

  @override
  List<Object> get props => [message];

  @override
  bool get stringify => true;
}

//
// bloc
//

class TreeBrowserBloc extends Bloc<TreeBrowserEvent, TreeBrowserState> {
  final GetTree usecase;
  // tree checksums in hierarchy order (added on load)
  final List<String> history = [];
  // entry names in hierarchy order ("root" is not included)
  final List<String> path = [];
  // selected tree entries
  final List<TreeEntry> selections = [];

  @override
  TreeBrowserState get initialState => Empty();

  TreeBrowserBloc({this.usecase});

  @override
  Stream<TreeBrowserState> mapEventToState(
    TreeBrowserEvent event,
  ) async* {
    if (event is LoadEntry) {
      if (state is Loaded) {
        path.add(event.entry.name);
        yield* _loadTree(event.entry.reference.value);
      }
    } else if (event is ToggleSelection) {
      if (state is Loaded) {
        final tree = (state as Loaded).tree;
        if (!selections.remove(event.entry)) {
          selections.add(event.entry);
        }
        yield Loaded(tree: tree, selections: selections, path: path);
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
        yield* _loadTree(digest);
      }
    } else if (event is LoadTree) {
      history.clear();
      path.clear();
      yield* _loadTree(event.digest);
    } else if (event is ResetTree) {
      // Something else has happened (i.e. navigating snapshots) outside of this
      // bloc that requires signaling the consumers of the change.
      yield initialState;
    }
  }

  Stream<TreeBrowserState> _loadTree(String digest) async* {
    selections.clear();
    yield Loading();
    final result = await usecase(Params(checksum: digest));
    yield result.mapOrElse(
      (tree) {
        history.add(digest);
        return Loaded(tree: tree, selections: selections, path: path);
      },
      (failure) => Error(message: failure.toString()),
    );
  }
}
