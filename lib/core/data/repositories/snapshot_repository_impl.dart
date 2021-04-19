//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:meta/meta.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/data/sources/snapshot_remote_data_source.dart';
import 'package:zorigami/core/domain/entities/request.dart';
import 'package:zorigami/core/domain/entities/snapshot.dart';
import 'package:zorigami/core/domain/repositories/snapshot_repository.dart';
import 'package:zorigami/core/error/exceptions.dart';
import 'package:zorigami/core/error/failures.dart';

class SnapshotRepositoryImpl extends SnapshotRepository {
  final SnapshotRemoteDataSource remoteDataSource;

  SnapshotRepositoryImpl({
    @required this.remoteDataSource,
  });

  @override
  Future<Result<Snapshot, Failure>> getSnapshot(String checksum) async {
    try {
      final snapshot = await remoteDataSource.getSnapshot(checksum);
      if (snapshot == null) {
        return Err(ServerFailure('got null result for snapshot'));
      }
      return Ok(snapshot);
    } on ServerException catch (e) {
      return Err(ServerFailure(e.toString()));
    }
  }

  @override
  Future<Result<String, Failure>> restoreDatabase(String storeId) async {
    try {
      final result = await remoteDataSource.restoreDatabase(storeId);
      if (result == null) {
        return Err(ServerFailure('got null result for restoreDatabase'));
      }
      return Ok(result);
    } on ServerException catch (e) {
      return Err(ServerFailure(e.toString()));
    }
  }

  @override
  Future<Result<bool, Failure>> restoreFiles(
      String checksum, String filepath, String dataset) async {
    try {
      final result =
          await remoteDataSource.restoreFiles(checksum, filepath, dataset);
      if (result == null) {
        return Err(ServerFailure('got null result for restoreFiles'));
      }
      return Ok(result);
    } on ServerException catch (e) {
      return Err(ServerFailure(e.toString()));
    }
  }

  @override
  Future<Result<List<Request>, Failure>> getAllRestores() async {
    try {
      final result = await remoteDataSource.getAllRestores();
      if (result == null) {
        return Err(ServerFailure('got null result for getAllRestores'));
      }
      return Ok(result);
    } on ServerException catch (e) {
      return Err(ServerFailure(e.toString()));
    }
  }

  @override
  Future<Result<bool, Failure>> cancelRestore(
      String checksum, String filepath, String dataset) async {
    try {
      final result =
          await remoteDataSource.cancelRestore(checksum, filepath, dataset);
      if (result == null) {
        return Err(ServerFailure('got null result for cancelRestore'));
      }
      return Ok(result);
    } on ServerException catch (e) {
      return Err(ServerFailure(e.toString()));
    }
  }
}
