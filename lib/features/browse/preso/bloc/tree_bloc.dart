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

abstract class TreeEvent extends Equatable {
  @override
  List<Object> get props => [];
}

class LoadTree extends TreeEvent {
  final String digest;

  LoadTree({@required this.digest});
}

//
// states
//

abstract class TreeState extends Equatable {
  @override
  List<Object> get props => [];
}

class Empty extends TreeState {}

class Loading extends TreeState {}

class Loaded extends TreeState {
  final Tree tree;

  Loaded({@required this.tree});

  @override
  List<Object> get props => [tree];
}

class Error extends TreeState {
  final String message;

  Error({@required this.message});

  @override
  List<Object> get props => [message];
}

//
// bloc
//

class TreeBloc extends Bloc<TreeEvent, TreeState> {
  final GetTree usecase;

  TreeBloc({this.usecase}) : super(Empty());

  @override
  Stream<TreeState> mapEventToState(
    TreeEvent event,
  ) async* {
    if (event is LoadTree) {
      yield Loading();
      final result = await usecase(Params(checksum: event.digest));
      yield result.mapOrElse(
        (tree) => Loaded(tree: tree),
        (failure) => Error(message: failure.toString()),
      );
    }
  }
}
