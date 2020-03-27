//
// Copyright (c) 2020 Nathan Fiedler
//
import 'dart:async';
import 'package:bloc/bloc.dart';
import 'package:equatable/equatable.dart';
import 'package:meta/meta.dart';
import 'package:zorigami/core/domain/entities/tree.dart';

//
// events
//

abstract class TreeBrowserEvent extends Equatable {
  @override
  List<Object> get props => [];
}

class VisitTree extends TreeBrowserEvent {
  final TreeEntry entry;

  VisitTree({@required this.entry});
}

class ToggleSelection extends TreeBrowserEvent {
  final TreeEntry entry;

  ToggleSelection({@required this.entry});
}

class NavigateUpward extends TreeBrowserEvent {}

class StartNewTree extends TreeBrowserEvent {}

//
// states
//

class TreeBrowserState extends Equatable {
  // tree checksums in hierarchy order
  final List<String> history;
  // file path hierarchy
  final List<String> path;
  // currently selected tree entries
  final List<TreeEntry> selections;

  TreeBrowserState({
    history = const [],
    path = const [],
    selections = const [],
  })  : history = List.unmodifiable(history),
        path = List.unmodifiable(path),
        selections = List.unmodifiable(selections);

  @override
  List<Object> get props => [history, path, selections];

  @override
  bool get stringify => true;
}

//
// bloc
//

class TreeBrowserBloc extends Bloc<TreeBrowserEvent, TreeBrowserState> {
  @override
  TreeBrowserState get initialState => TreeBrowserState();

  @override
  Stream<TreeBrowserState> mapEventToState(
    TreeBrowserEvent event,
  ) async* {
    if (event is VisitTree) {
      final history = List.from(state.history)
        ..add(event.entry.reference.value);
      final path = List.from(state.path)..add(event.entry.name);
      yield TreeBrowserState(
        history: history,
        path: path,
      );
    } else if (event is ToggleSelection) {
      var selections = List.from(state.selections);
      if (!selections.remove(event.entry)) {
        selections.add(event.entry);
      }
      yield TreeBrowserState(
        history: state.history,
        path: state.path,
        selections: selections,
      );
    } else if (event is NavigateUpward) {
      if (state.history.isNotEmpty && state.path.isNotEmpty) {
        yield TreeBrowserState(
          history: state.history.sublist(0, state.history.length - 1),
          path: state.path.sublist(0, state.path.length - 1),
        );
      }
    } else if (event is StartNewTree) {
      yield initialState;
    }
  }
}
