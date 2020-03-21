//
// Copyright (c) 2020 Nathan Fiedler
//
import 'dart:async';
import 'package:bloc/bloc.dart';
import 'package:equatable/equatable.dart';
import 'package:meta/meta.dart';
import 'package:zorigami/core/domain/entities/configuration.dart';
import 'package:zorigami/core/domain/usecases/get_configuration.dart';
import 'package:zorigami/core/usecases/usecase.dart';

part 'configuration_event.dart';
part 'configuration_state.dart';

class ConfigurationBloc extends Bloc<ConfigurationEvent, ConfigurationState> {
  final GetConfiguration usecase;

  ConfigurationBloc({this.usecase});

  @override
  ConfigurationState get initialState => Empty();

  @override
  Stream<ConfigurationState> mapEventToState(
    ConfigurationEvent event,
  ) async* {
    if (event is LoadConfiguration) {
      yield Loading();
      final result = await usecase(NoParams());
      yield result.mapOrElse(
        (config) => Loaded(config: config),
        (failure) => Error(message: failure.toString()),
      );
    }
  }
}
