//
// Copyright (c) 2022 Nathan Fiedler
//
import 'package:bloc/bloc.dart';
import 'package:equatable/equatable.dart';
import 'package:zorigami/core/domain/entities/pack_store.dart';
import 'package:zorigami/core/domain/usecases/define_pack_store.dart' as dps;

//
// events
//

abstract class CreatePackStoresEvent extends Equatable {
  @override
  List<Object> get props => [];
}

class DefinePackStore extends CreatePackStoresEvent {
  final PackStore store;

  DefinePackStore({required this.store});
}

//
// states
//

abstract class CreatePackStoresState extends Equatable {
  @override
  List<Object> get props => [];
}

class Editing extends CreatePackStoresState {}

class Submitting extends CreatePackStoresState {}

class Submitted extends CreatePackStoresState {}

class Error extends CreatePackStoresState {
  final String message;

  Error({required this.message});

  @override
  List<Object> get props => [message];
}

//
// bloc
//

class CreatePackStoresBloc
    extends Bloc<CreatePackStoresEvent, CreatePackStoresState> {
  final dps.DefinePackStore usecase;

  CreatePackStoresBloc({required this.usecase}) : super(Editing()) {
    on<DefinePackStore>((event, emit) async {
      emit(Submitting());
      final result = await usecase(dps.Params(
        store: event.store,
      ));
      emit(result.mapOrElse(
        (store) => Submitted(),
        (failure) => Error(message: failure.toString()),
      ));
    });
  }
}
