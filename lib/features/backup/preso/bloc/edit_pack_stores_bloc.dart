//
// Copyright (c) 2020 Nathan Fiedler
//
import 'dart:async';
import 'package:bloc/bloc.dart';
import 'package:meta/meta.dart';
import 'package:equatable/equatable.dart';
import 'package:zorigami/core/domain/usecases/update_pack_store.dart' as ups;

//
// events
//

abstract class EditPackStoresEvent extends Equatable {
  @override
  List<Object> get props => [];
}

class UpdatePackStore extends EditPackStoresEvent {
  final String key;
  final Map<String, dynamic> options;

  UpdatePackStore({@required this.key, @required this.options});
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

class Error extends EditPackStoresState {
  final String message;

  Error({@required this.message});

  @override
  List<Object> get props => [message];
}

//
// bloc
//

class EditPackStoresBloc
    extends Bloc<EditPackStoresEvent, EditPackStoresState> {
  final ups.UpdatePackStore updatePackStore;

  EditPackStoresBloc({this.updatePackStore});

  @override
  EditPackStoresState get initialState => Editing();

  // very helpful for debugging
  // @override
  // void onTransition(
  //   Transition<EditPackStoresEvent, EditPackStoresState> transition,
  // ) {
  //   super.onTransition(transition);
  //   print(transition);
  // }

  @override
  Stream<EditPackStoresState> mapEventToState(
    EditPackStoresEvent event,
  ) async* {
    if (event is UpdatePackStore) {
      yield Submitting();
      final result = await updatePackStore(ups.Params(
        key: event.key,
        options: event.options,
      ));
      yield result.mapOrElse(
        (store) => Submitted(),
        (failure) => Error(message: failure.toString()),
      );
    }
  }
}
