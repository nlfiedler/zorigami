//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:meta/meta.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/data/sources/data_set_remote_data_source.dart';
import 'package:zorigami/core/domain/entities/data_set.dart';
import 'package:zorigami/core/domain/repositories/data_set_repository.dart';
import 'package:zorigami/core/error/exceptions.dart';
import 'package:zorigami/core/error/failures.dart';

class DataSetRepositoryImpl extends DataSetRepository {
  final DataSetRemoteDataSource remoteDataSource;

  DataSetRepositoryImpl({
    @required this.remoteDataSource,
  });

  @override
  Future<Result<List<DataSet>, Failure>> getAllDataSets() async {
    try {
      final dataset = await remoteDataSource.getAllDataSets();
      return Ok(dataset);
    } on ServerException catch (e) {
      return Err(ServerFailure(e.toString()));
    }
  }

  @override
  Future<Result<DataSet, Failure>> defineDataSet(DataSet input) async {
    try {
      final dataset = await remoteDataSource.defineDataSet(input);
      return Ok(dataset);
    } on ServerException catch (e) {
      return Err(ServerFailure(e.toString()));
    }
  }

  @override
  Future<Result<DataSet, Failure>> updateDataSet(DataSet input) async {
    try {
      final dataset = await remoteDataSource.updateDataSet(input);
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
}
