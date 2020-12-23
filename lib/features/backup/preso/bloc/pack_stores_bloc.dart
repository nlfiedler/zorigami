//
// Copyright (c) 2020 Nathan Fiedler
//
import 'dart:async';
import 'package:bloc/bloc.dart';
import 'package:meta/meta.dart';
import 'package:equatable/equatable.dart';
import 'package:zorigami/core/domain/entities/pack_store.dart';
import 'package:zorigami/core/domain/usecases/get_pack_stores.dart';
import 'package:zorigami/core/domain/usecases/usecase.dart';

//
// events
//

abstract class PackStoresEvent extends Equatable {
  @override
  List<Object> get props => [];
}

class LoadAllPackStores extends PackStoresEvent {}

class ReloadPackStores extends PackStoresEvent {}

//
// states
//

abstract class PackStoresState extends Equatable {
  @override
  List<Object> get props => [];
}

class Empty extends PackStoresState {}

class Loading extends PackStoresState {}

class Loaded extends PackStoresState {
  final List<PackStore> stores;

  Loaded({@required this.stores});

  @override
  List<Object> get props => [stores];
}

class Error extends PackStoresState {
  final String message;

  Error({@required this.message});

  @override
  List<Object> get props => [message];
}

//
// bloc
//

class PackStoresBloc extends Bloc<PackStoresEvent, PackStoresState> {
  final GetPackStores usecase;

  PackStoresBloc({this.usecase}) : super(Empty());

  @override
  Stream<PackStoresState> mapEventToState(
    PackStoresEvent event,
  ) async* {
    if (event is LoadAllPackStores) {
      yield Loading();
      final result = await usecase(NoParams());
      yield result.mapOrElse(
        (stores) {
          // put the pack stores in a consistent order
          stores.sort((a, b) => a.key.compareTo(b.key));
          return Loaded(stores: stores);
        },
        (failure) => Error(message: failure.toString()),
      );
    } else if (event is ReloadPackStores) {
      // force an update as something changed elsewhere
      yield Empty();
    }
  }
}
