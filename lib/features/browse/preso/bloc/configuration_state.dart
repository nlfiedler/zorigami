//
// Copyright (c) 2020 Nathan Fiedler
//
part of 'configuration_bloc.dart';

@immutable
abstract class ConfigurationState extends Equatable {
  @override
  List<Object> get props => [];
}

class Empty extends ConfigurationState {}

class Loading extends ConfigurationState {}

class Loaded extends ConfigurationState {
  final Configuration config;

  Loaded({@required this.config});

  @override
  List<Object> get props => [config];
}

class Error extends ConfigurationState {
  final String message;

  Error({@required this.message});

  @override
  List<Object> get props => [message];
}
