//
// Copyright (c) 2020 Nathan Fiedler
//
import 'dart:async';
import 'package:bloc/bloc.dart';
import 'package:equatable/equatable.dart';
import 'package:meta/meta.dart';
import 'package:zorigami/core/domain/entities/snapshot.dart';
import 'package:zorigami/core/domain/usecases/get_snapshot.dart';

//
// events
//

abstract class SnapshotEvent extends Equatable {
  @override
  List<Object> get props => [];
}

class LoadSnapshot extends SnapshotEvent {
  final String digest;

  LoadSnapshot({@required this.digest});
}

//
// states
//

abstract class SnapshotState extends Equatable {
  @override
  List<Object> get props => [];
}

class Empty extends SnapshotState {}

class Loading extends SnapshotState {}

class Loaded extends SnapshotState {
  final Snapshot snapshot;

  Loaded({@required this.snapshot});

  @override
  List<Object> get props => [snapshot];
}

class Error extends SnapshotState {
  final String message;

  Error({@required this.message});

  @override
  List<Object> get props => [message];
}

//
// bloc
//

class SnapshotBloc extends Bloc<SnapshotEvent, SnapshotState> {
  final GetSnapshot usecase;

  SnapshotBloc({this.usecase}) : super(Empty());

  @override
  Stream<SnapshotState> mapEventToState(
    SnapshotEvent event,
  ) async* {
    if (event is LoadSnapshot) {
      yield Loading();
      final result = await usecase(Params(checksum: event.digest));
      yield result.mapOrElse(
        (snapshot) => Loaded(snapshot: snapshot),
        (failure) => Error(message: failure.toString()),
      );
    }
  }
}
