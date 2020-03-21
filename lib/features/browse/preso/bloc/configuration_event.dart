//
// Copyright (c) 2020 Nathan Fiedler
//
part of 'configuration_bloc.dart';

@immutable
abstract class ConfigurationEvent extends Equatable {
  @override
  List<Object> get props => [];
}

class LoadConfiguration extends ConfigurationEvent {}
