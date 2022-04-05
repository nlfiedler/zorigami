//
// Copyright (c) 2022 Nathan Fiedler
//
import 'dart:async';
import 'package:bloc/bloc.dart';
import 'package:equatable/equatable.dart';
import 'package:zorigami/core/domain/entities/request.dart';
import 'package:zorigami/core/domain/usecases/cancel_restore.dart' as cr;
import 'package:zorigami/core/domain/usecases/get_restores.dart' as gr;
import 'package:zorigami/core/domain/usecases/usecase.dart';

//
// events
//

abstract class RestoresEvent extends Equatable {
  @override
  List<Object> get props => [];
}

class LoadRequests extends RestoresEvent {}

class CancelRequest extends RestoresEvent {
  final String tree;
  final String entry;
  final String filepath;
  final String dataset;

  CancelRequest({
    required this.tree,
    required this.entry,
    required this.filepath,
    required this.dataset,
  });
}

//
// states
//

abstract class RestoresState extends Equatable {
  @override
  List<Object> get props => [];
}

class Empty extends RestoresState {}

class Loading extends RestoresState {}

class Loaded extends RestoresState {
  // list of file restore requests
  final List<Request> requests;
  // true if a request has been successfully cancelled
  final bool requestCancelled;

  Loaded({
    required requests,
    required this.requestCancelled,
  }) : requests = List.unmodifiable(requests);

  @override
  List<Object> get props => [requests, requestCancelled];

  @override
  bool get stringify => true;
}

class Error extends RestoresState {
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

class RestoresBloc extends Bloc<RestoresEvent, RestoresState> {
  final gr.GetRestores getRestores;
  final cr.CancelRestore cancelRestore;

  RestoresBloc({required this.getRestores, required this.cancelRestore})
      : super(Empty()) {
    on<LoadRequests>((event, emit) async {
      return _loadRequests(emit);
    });
    on<CancelRequest>((event, emit) async {
      if (state is Loaded) {
        final List<Request> requests = List.from((state as Loaded).requests);
        final params = cr.Params(
          tree: event.tree,
          entry: event.entry,
          filepath: event.filepath,
          dataset: event.dataset,
        );
        final result = await cancelRestore(params);
        final cancelled = result.unwrapOr(false);
        if (cancelled) {
          requests.removeWhere(
              (Request r) => r.tree == event.tree && r.entry == event.entry);
        }
        emit(Loaded(
          requests: requests,
          requestCancelled: cancelled,
        ));
      }
    });
  }

  Future<void> _loadRequests(Emitter<RestoresState> emit) async {
    emit(Loading());
    final result = await getRestores(NoParams());
    emit(result.mapOrElse(
      (requests) {
        return Loaded(requests: requests, requestCancelled: false);
      },
      (failure) => Error(message: failure.toString()),
    ));
  }
}
