//
// Copyright (c) 2020 Nathan Fiedler
//
import 'dart:async';
import 'package:bloc/bloc.dart';
import 'package:equatable/equatable.dart';
import 'package:zorigami/core/domain/entities/pack_store.dart';
import 'package:zorigami/core/domain/usecases/delete_pack_store.dart' as dps;
import 'package:zorigami/core/domain/usecases/test_pack_store.dart' as tps;
import 'package:zorigami/core/domain/usecases/update_pack_store.dart' as ups;

//
// events
//

abstract class EditPackStoresEvent extends Equatable {
  @override
  List<Object> get props => [];
}

class UpdatePackStore extends EditPackStoresEvent {
  final PackStore store;

  UpdatePackStore({required this.store});
}

class TestPackStore extends EditPackStoresEvent {
  final PackStore store;

  TestPackStore({required this.store});
}

class DeletePackStore extends EditPackStoresEvent {
  final PackStore store;

  DeletePackStore({required this.store});
}

//
// states
//

abstract class EditPackStoresState extends Equatable {
  @override
  List<Object> get props => [];
}

class Editing extends EditPackStoresState {}

class Submitting extends EditPackStoresState {}

class Submitted extends EditPackStoresState {}

class Tested extends EditPackStoresState {
  final String result;

  Tested({required this.result});

  @override
  List<Object> get props => [result];
}

class Error extends EditPackStoresState {
  final String message;

  Error({required this.message});

  @override
  List<Object> get props => [message];
}

//
// bloc
//

class EditPackStoresBloc
    extends Bloc<EditPackStoresEvent, EditPackStoresState> {
  final ups.UpdatePackStore updatePackStore;
  final tps.TestPackStore testPackStore;
  final dps.DeletePackStore deletePackStore;

  EditPackStoresBloc({
    required this.updatePackStore,
    required this.testPackStore,
    required this.deletePackStore,
  }) : super(Editing());

  @override
  Stream<EditPackStoresState> mapEventToState(
    EditPackStoresEvent event,
  ) async* {
    if (event is UpdatePackStore) {
      yield Submitting();
      final result = await updatePackStore(ups.Params(
        store: event.store,
      ));
      yield result.mapOrElse(
        (store) => Submitted(),
        (failure) => Error(message: failure.toString()),
      );
    } else if (event is TestPackStore) {
      yield Submitting();
      final result = await testPackStore(tps.Params(
        store: event.store,
      ));
      yield result.mapOrElse(
        (result) => Tested(result: result),
        (failure) => Error(message: failure.toString()),
      );
    } else if (event is DeletePackStore) {
      yield Submitting();
      final result = await deletePackStore(dps.Params(
        store: event.store,
      ));
      yield result.mapOrElse(
        (store) => Submitted(),
        (failure) => Error(message: failure.toString()),
      );
    }
  }
}
