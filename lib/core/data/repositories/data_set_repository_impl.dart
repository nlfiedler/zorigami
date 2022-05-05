//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/data/sources/data_set_remote_data_source.dart';
import 'package:zorigami/core/domain/entities/data_set.dart';
import 'package:zorigami/core/domain/repositories/data_set_repository.dart';
import 'package:zorigami/core/error/exceptions.dart';
import 'package:zorigami/core/error/failures.dart';

class DataSetRepositoryImpl extends DataSetRepository {
  final DataSetRemoteDataSource remoteDataSource;

  DataSetRepositoryImpl({
    required this.remoteDataSource,
  });

  @override
  Future<Result<List<DataSet>, Failure>> getAllDataSets() async {
    try {
      return Ok(await remoteDataSource.getAllDataSets());
    } on ServerException catch (e) {
      return Err(ServerFailure(e.toString()));
    }
  }

  @override
  Future<Result<DataSet, Failure>> defineDataSet(DataSet input) async {
    try {
      final dataset = await remoteDataSource.defineDataSet(input);
      if (dataset == null) {
        return Err(ServerFailure('got null result for data set'));
      }
      return Ok(dataset);
    } on ServerException catch (e) {
      return Err(ServerFailure(e.toString()));
    }
  }

  @override
  Future<Result<DataSet, Failure>> updateDataSet(DataSet input) async {
    try {
      final dataset = await remoteDataSource.updateDataSet(input);
      if (dataset == null) {
        return Err(ServerFailure('got null result for data set'));
      }
      return Ok(dataset);
    } on ServerException catch (e) {
      return Err(ServerFailure(e.toString()));
    }
  }

  @override
  Future<Result<DataSet, Failure>> deleteDataSet(DataSet input) async {
    try {
      await remoteDataSource.deleteDataSet(input);
      return Ok(input);
    } on ServerException catch (e) {
      return Err(ServerFailure(e.toString()));
    }
  }

  @override
  Future<Result<bool, Failure>> startBackup(DataSet input) async {
    try {
      final result = await remoteDataSource.startBackup(input);
      return Ok(result);
    } on ServerException catch (e) {
      return Err(ServerFailure(e.toString()));
    }
  }

  @override
  Future<Result<bool, Failure>> stopBackup(DataSet input) async {
    try {
      final result = await remoteDataSource.stopBackup(input);
      return Ok(result);
    } on ServerException catch (e) {
      return Err(ServerFailure(e.toString()));
    }
  }
}
