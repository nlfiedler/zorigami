//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:meta/meta.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/data/sources/configuration_remote_data_source.dart';
import 'package:zorigami/core/domain/entities/configuration.dart';
import 'package:zorigami/core/domain/repositories/configuration_repository.dart';
import 'package:zorigami/core/error/exceptions.dart';
import 'package:zorigami/core/error/failures.dart';

class ConfigurationRepositoryImpl extends ConfigurationRepository {
  final ConfigurationRemoteDataSource remoteDataSource;

  ConfigurationRepositoryImpl({
    @required this.remoteDataSource,
  });

  @override
  Future<Result<Configuration, Failure>> getConfiguration() async {
    try {
      final configuration = await remoteDataSource.getConfiguration();
      if (configuration == null) {
        return Err(ServerFailure('got null result'));
      }
      return Ok(configuration);
    } on ServerException catch (e) {
      return Err(ServerFailure(e.toString()));
    }
  }
}