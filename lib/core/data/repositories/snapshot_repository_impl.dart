//
// Copyright (c) 2024 Nathan Fiedler
//
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
    required this.remoteDataSource,
  });

  @override
  Future<Result<Snapshot, Failure>> getSnapshot(String checksum) async {
    try {
      final snapshot = await remoteDataSource.getSnapshot(checksum);
      if (snapshot == null) {
        return const Err(ServerFailure('got null result for snapshot'));
      }
      return Ok(snapshot);
    } on ServerException catch (e) {
      return Err(ServerFailure(e.toString()));
    }
  }

  @override
  Future<Result<String, Failure>> restoreDatabase(String storeId) async {
    try {
      return Ok(await remoteDataSource.restoreDatabase(storeId));
    } on ServerException catch (e) {
      return Err(ServerFailure(e.toString()));
    }
  }

  @override
  Future<Result<bool, Failure>> restoreFiles(
      String tree, String entry, String filepath, String dataset) async {
    try {
      return Ok(
        await remoteDataSource.restoreFiles(tree, entry, filepath, dataset),
      );
    } on ServerException catch (e) {
      return Err(ServerFailure(e.toString()));
    }
  }

  @override
  Future<Result<List<Request>, Failure>> getAllRestores() async {
    try {
      return Ok(await remoteDataSource.getAllRestores());
    } on ServerException catch (e) {
      return Err(ServerFailure(e.toString()));
    }
  }

  @override
  Future<Result<bool, Failure>> cancelRestore(
      String tree, String entry, String filepath, String dataset) async {
    try {
      return Ok(
        await remoteDataSource.cancelRestore(tree, entry, filepath, dataset),
      );
    } on ServerException catch (e) {
      return Err(ServerFailure(e.toString()));
    }
  }
}
