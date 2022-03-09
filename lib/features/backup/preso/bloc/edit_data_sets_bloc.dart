//
// Copyright (c) 2022 Nathan Fiedler
//
import 'package:bloc/bloc.dart';
import 'package:equatable/equatable.dart';
import 'package:zorigami/core/domain/entities/data_set.dart';
import 'package:zorigami/core/domain/usecases/delete_data_set.dart' as dds;
import 'package:zorigami/core/domain/usecases/update_data_set.dart' as uds;

//
// events
//

abstract class EditDataSetsEvent extends Equatable {
  @override
  List<Object> get props => [];
}

class UpdateDataSet extends EditDataSetsEvent {
  final DataSet dataset;

  UpdateDataSet({required this.dataset});
}

class DeleteDataSet extends EditDataSetsEvent {
  final DataSet dataset;

  DeleteDataSet({required this.dataset});
}

//
// states
//

abstract class EditDataSetsState extends Equatable {
  @override
  List<Object> get props => [];
}

class Editing extends EditDataSetsState {}

class Submitting extends EditDataSetsState {}

class Submitted extends EditDataSetsState {}

class Error extends EditDataSetsState {
  final String message;

  Error({required this.message});

  @override
  List<Object> get props => [message];
}

//
// bloc
//

class EditDataSetsBloc extends Bloc<EditDataSetsEvent, EditDataSetsState> {
  final uds.UpdateDataSet updateDataSet;
  final dds.DeleteDataSet deleteDataSet;

  EditDataSetsBloc({
    required this.updateDataSet,
    required this.deleteDataSet,
  }) : super(Editing()) {
    on<UpdateDataSet>((event, emit) async {
      emit(Submitting());
      final result = await updateDataSet(uds.Params(
        dataset: event.dataset,
      ));
      emit(result.mapOrElse(
        (dataset) => Submitted(),
        (failure) => Error(message: failure.toString()),
      ));
    });
    on<DeleteDataSet>((event, emit) async {
      emit(Submitting());
      final result = await deleteDataSet(dds.Params(
        dataset: event.dataset,
      ));
      emit(result.mapOrElse(
        (dataset) => Submitted(),
        (failure) => Error(message: failure.toString()),
      ));
    });
  }
}
