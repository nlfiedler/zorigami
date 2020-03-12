//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/configuration.dart';
import 'package:zorigami/core/error/failures.dart';

abstract class ConfigurationRepository {
  /// Retrieve the system configuration.
  Future<Result<Configuration, Failure>> getConfiguration();
}
