//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:meta/meta.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/data/sources/snapshot_remote_data_source.dart';
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
        return Err(ServerFailure('got null result'));
      }
      return Ok(snapshot);
    } on ServerException {
      return Err(ServerFailure());
    }
  }
}
