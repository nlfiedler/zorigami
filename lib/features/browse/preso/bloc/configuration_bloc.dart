//
// Copyright (c) 2022 Nathan Fiedler
//
import 'package:bloc/bloc.dart';
import 'package:equatable/equatable.dart';
import 'package:zorigami/core/domain/entities/configuration.dart';
import 'package:zorigami/core/domain/usecases/get_configuration.dart';
import 'package:zorigami/core/domain/usecases/usecase.dart';

//
// events
//

abstract class ConfigurationEvent extends Equatable {
  @override
  List<Object> get props => [];
}

class LoadConfiguration extends ConfigurationEvent {}

//
// states
//

abstract class ConfigurationState extends Equatable {
  @override
  List<Object> get props => [];
}

class Empty extends ConfigurationState {}

class Loading extends ConfigurationState {}

class Loaded extends ConfigurationState {
  final Configuration config;

  Loaded({required this.config});

  @override
  List<Object> get props => [config];
}

class Error extends ConfigurationState {
  final String message;

  Error({required this.message});

  @override
  List<Object> get props => [message];
}

//
// bloc
//

class ConfigurationBloc extends Bloc<ConfigurationEvent, ConfigurationState> {
  final GetConfiguration usecase;

  ConfigurationBloc({required this.usecase}) : super(Empty()) {
    on<LoadConfiguration>((event, emit) async {
      emit(Loading());
      final result = await usecase(NoParams());
      emit(result.mapOrElse(
        (config) => Loaded(config: config),
        (failure) => Error(message: failure.toString()),
      ));
    });
  }
}
