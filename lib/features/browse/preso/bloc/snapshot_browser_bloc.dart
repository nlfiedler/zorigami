//
// Copyright (c) 2020 Nathan Fiedler
//
import 'dart:async';
import 'package:bloc/bloc.dart';
import 'package:equatable/equatable.dart';
import 'package:meta/meta.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/snapshot.dart';
import 'package:zorigami/core/domain/usecases/get_snapshot.dart';

//
// events
//

abstract class SnapshotBrowserEvent extends Equatable {
  @override
  List<Object> get props => [];
}

class LoadSnapshot extends SnapshotBrowserEvent {
  final String digest;

  LoadSnapshot({@required this.digest});
}

class LoadParent extends SnapshotBrowserEvent {}

class LoadSubsequent extends SnapshotBrowserEvent {}

//
// states
//

abstract class SnapshotBrowserState extends Equatable {
  @override
  List<Object> get props => [];
}

class Empty extends SnapshotBrowserState {}

class Loading extends SnapshotBrowserState {}

class Loaded extends SnapshotBrowserState {
  final Snapshot snapshot;
  final bool hasSubsequent;

  Loaded({@required this.snapshot, this.hasSubsequent = false});

  @override
  List<Object> get props => [snapshot, hasSubsequent];

  @override
  bool get stringify => true;
}

class Error extends SnapshotBrowserState {
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

class SnapshotBrowserBloc
    extends Bloc<SnapshotBrowserEvent, SnapshotBrowserState> {
  final GetSnapshot usecase;
  final List<String> history = [];

  SnapshotBrowserBloc({this.usecase}) : super(Empty());

  @override
  Stream<SnapshotBrowserState> mapEventToState(
    SnapshotBrowserEvent event,
  ) async* {
    if (event is LoadSnapshot) {
      yield* _loadSnapshot(event.digest);
    } else if (event is LoadParent) {
      if (state is Loaded) {
        final current = (state as Loaded).snapshot;
        if (current.parent is Some) {
          history.add(current.checksum);
          yield* _loadSnapshot(current.parent.unwrap());
        }
      }
    } else if (event is LoadSubsequent) {
      if (history.isNotEmpty) {
        final digest = history.removeLast();
        yield* _loadSnapshot(digest);
      }
    }
  }

  Stream<SnapshotBrowserState> _loadSnapshot(String digest) async* {
    yield Loading();
    final result = await usecase(Params(checksum: digest));
    yield result.mapOrElse(
      (snapshot) => Loaded(
        snapshot: snapshot,
        hasSubsequent: history.isNotEmpty,
      ),
      (failure) => Error(message: failure.toString()),
    );
  }
}
