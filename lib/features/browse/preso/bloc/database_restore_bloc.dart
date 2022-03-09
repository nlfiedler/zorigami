//
// Copyright (c) 2022 Nathan Fiedler
//
import 'package:bloc/bloc.dart';
import 'package:equatable/equatable.dart';
import 'package:zorigami/core/domain/usecases/restore_database.dart' as rd;

//
// events
//

abstract class DatabaseRestoreEvent extends Equatable {
  @override
  List<Object> get props => [];
}

class RestoreDatabase extends DatabaseRestoreEvent {
  final String storeId;

  RestoreDatabase({required this.storeId});
}

//
// states
//

abstract class DatabaseRestoreState extends Equatable {
  @override
  List<Object> get props => [];
}

class Empty extends DatabaseRestoreState {}

class Loading extends DatabaseRestoreState {}

class Loaded extends DatabaseRestoreState {
  final String result;

  Loaded({required this.result});

  @override
  List<Object> get props => [result];

  @override
  bool get stringify => true;
}

class Error extends DatabaseRestoreState {
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

class DatabaseRestoreBloc
    extends Bloc<DatabaseRestoreEvent, DatabaseRestoreState> {
  final rd.RestoreDatabase usecase;

  DatabaseRestoreBloc({required this.usecase}) : super(Empty()) {
    on<RestoreDatabase>((event, emit) async {
      emit(Loading());
      final result = await usecase(rd.Params(storeId: event.storeId));
      emit(result.mapOrElse(
        (result) => Loaded(result: result),
        (failure) => Error(message: failure.toString()),
      ));
    });
  }
}
