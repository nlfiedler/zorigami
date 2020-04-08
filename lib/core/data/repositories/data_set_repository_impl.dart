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
      final DataSet = await remoteDataSource.getAllDataSets();
      return Ok(DataSet);
    } on ServerException catch (e) {
      return Err(ServerFailure(e.toString()));
    }
  }

  @override
  Future<Result<DataSet, Failure>> defineDataSet(DataSet input) async {
    try {
      final DataSet = await remoteDataSource.defineDataSet(input);
      return Ok(DataSet);
    } on ServerException catch (e) {
      return Err(ServerFailure(e.toString()));
    }
  }

  @override
  Future<Result<DataSet, Failure>> updateDataSet(DataSet input) async {
    try {
      final DataSet = await remoteDataSource.updateDataSet(input);
      return Ok(DataSet);
    } on ServerException catch (e) {
      return Err(ServerFailure(e.toString()));
    }
  }

  @override
  Future<Result<DataSet, Failure>> deleteDataSet(DataSet input) async {
    try {
      final DataSet = await remoteDataSource.deleteDataSet(input);
      return Ok(DataSet);
    } on ServerException catch (e) {
      return Err(ServerFailure(e.toString()));
    }
  }
}
