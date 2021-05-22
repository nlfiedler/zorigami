//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/data/sources/tree_remote_data_source.dart';
import 'package:zorigami/core/domain/entities/tree.dart';
import 'package:zorigami/core/domain/repositories/tree_repository.dart';
import 'package:zorigami/core/error/exceptions.dart';
import 'package:zorigami/core/error/failures.dart';

class TreeRepositoryImpl extends TreeRepository {
  final TreeRemoteDataSource remoteDataSource;

  TreeRepositoryImpl({
    required this.remoteDataSource,
  });

  @override
  Future<Result<Tree, Failure>> getTree(String checksum) async {
    try {
      final tree = await remoteDataSource.getTree(checksum);
      if (tree == null) {
        return Err(ServerFailure('got null result for tree'));
      }
      return Ok(tree);
    } on ServerException catch (e) {
      return Err(ServerFailure(e.toString()));
    }
  }
}
