//
// Copyright (c) 2022 Nathan Fiedler
//
import 'package:bloc/bloc.dart';
import 'package:equatable/equatable.dart';
import 'package:zorigami/core/domain/entities/data_set.dart';
import 'package:zorigami/core/domain/usecases/get_data_sets.dart';
import 'package:zorigami/core/domain/usecases/usecase.dart';

//
// events
//

abstract class DataSetsEvent extends Equatable {
  @override
  List<Object> get props => [];
}

class LoadAllDataSets extends DataSetsEvent {}

class ReloadDataSets extends DataSetsEvent {}

//
// states
//

abstract class DataSetsState extends Equatable {
  @override
  List<Object> get props => [];
}

class Empty extends DataSetsState {}

class Loading extends DataSetsState {}

class Loaded extends DataSetsState {
  final List<DataSet> sets;

  Loaded({required this.sets});

  @override
  List<Object> get props => [sets];
}

class Error extends DataSetsState {
  final String message;

  Error({required this.message});

  @override
  List<Object> get props => [message];
}

//
// bloc
//

class DataSetsBloc extends Bloc<DataSetsEvent, DataSetsState> {
  final GetDataSets usecase;

  DataSetsBloc({required this.usecase}) : super(Empty()) {
    on<LoadAllDataSets>((event, emit) async {
      emit(Loading());
      final result = await usecase(NoParams());
      emit(result.mapOrElse(
        (sets) => Loaded(sets: sets),
        (failure) => Error(message: failure.toString()),
      ));
    });
    on<ReloadDataSets>((event, emit) {
      // force an update as something changed elsewhere
      emit(Empty());
    });
  }
}
