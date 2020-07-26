//
// Copyright (c) 2020 Nathan Fiedler
//
import 'dart:async';
import 'package:bloc/bloc.dart';
import 'package:meta/meta.dart';
import 'package:equatable/equatable.dart';
import 'package:zorigami/core/domain/entities/data_set.dart';
import 'package:zorigami/core/domain/usecases/define_data_set.dart' as dps;

//
// events
//

abstract class CreateDataSetsEvent extends Equatable {
  @override
  List<Object> get props => [];
}

class DefineDataSet extends CreateDataSetsEvent {
  final DataSet dataset;

  DefineDataSet({@required this.dataset});
}

//
// states
//

abstract class CreateDataSetsState extends Equatable {
  @override
  List<Object> get props => [];
}

class Editing extends CreateDataSetsState {}

class Submitting extends CreateDataSetsState {}

class Submitted extends CreateDataSetsState {}

class Error extends CreateDataSetsState {
  final String message;

  Error({@required this.message});

  @override
  List<Object> get props => [message];
}

//
// bloc
//

class CreateDataSetsBloc
    extends Bloc<CreateDataSetsEvent, CreateDataSetsState> {
  final dps.DefineDataSet usecase;

  CreateDataSetsBloc({this.usecase}) : super(Editing());

  @override
  Stream<CreateDataSetsState> mapEventToState(
    CreateDataSetsEvent event,
  ) async* {
    if (event is DefineDataSet) {
      yield Submitting();
      final result = await usecase(dps.Params(
        dataset: event.dataset,
      ));
      yield result.mapOrElse(
        (dataset) => Submitted(),
        (failure) => Error(message: failure.toString()),
      );
    }
  }
}
