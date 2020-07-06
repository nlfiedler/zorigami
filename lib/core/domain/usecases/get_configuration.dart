//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/configuration.dart';
import 'package:zorigami/core/domain/repositories/configuration_repository.dart';
import 'package:zorigami/core/domain/usecases/usecase.dart';
import 'package:zorigami/core/error/failures.dart';

class GetConfiguration implements UseCase<Configuration, NoParams> {
  final ConfigurationRepository repository;

  GetConfiguration(this.repository);

  @override
  Future<Result<Configuration, Failure>> call(NoParams params) async {
    return await repository.getConfiguration();
  }
}
