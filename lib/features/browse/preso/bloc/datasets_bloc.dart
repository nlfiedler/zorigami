//
// Copyright (c) 2020 Nathan Fiedler
//
import 'dart:async';
import 'package:bloc/bloc.dart';
import 'package:meta/meta.dart';
import 'package:equatable/equatable.dart';
import 'package:zorigami/core/domain/entities/data_set.dart';
import 'package:zorigami/core/domain/usecases/get_data_sets.dart';
import 'package:zorigami/core/usecases/usecase.dart';

//
// events
//

abstract class DatasetsEvent extends Equatable {
  @override
  List<Object> get props => [];
}

class LoadAllDataSets extends DatasetsEvent {}

//
// states
//

abstract class DatasetsState extends Equatable {
  @override
  List<Object> get props => [];
}

class Empty extends DatasetsState {}

class Loading extends DatasetsState {}

class Loaded extends DatasetsState {
  final List<DataSet> sets;

  Loaded({@required this.sets});

  @override
  List<Object> get props => [sets];
}

class Error extends DatasetsState {
  final String message;

  Error({@required this.message});

  @override
  List<Object> get props => [message];
}

//
// bloc
//

class DatasetsBloc extends Bloc<DatasetsEvent, DatasetsState> {
  final GetDataSets getDataSets;

  DatasetsBloc({this.getDataSets});

  @override
  DatasetsState get initialState => Empty();

  @override
  Stream<DatasetsState> mapEventToState(
    DatasetsEvent event,
  ) async* {
    if (event is LoadAllDataSets) {
      yield Loading();
      final result = await getDataSets(NoParams());
      yield result.mapOrElse(
        (sets) => Loaded(sets: sets),
        (failure) => Error(message: failure.toString()),
      );
    }
  }
}
