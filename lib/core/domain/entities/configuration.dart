//
// Copyright (c) 2024 Nathan Fiedler
//
import 'package:equatable/equatable.dart';

/// A `Configuration` holds information about the system.
class Configuration extends Equatable {
  // Name of the computer being backed up.
  final String hostname;
  // Name of the user using the system.
  final String username;
  // Unique identifier for the system.
  final String computerId;

  const Configuration({
    required this.hostname,
    required this.username,
    required this.computerId,
  });

  @override
  List<Object> get props => [hostname, username, computerId];

  @override
  bool get stringify => true;
}
