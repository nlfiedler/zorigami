//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:meta/meta.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/data/sources/pack_store_remote_data_source.dart';
import 'package:zorigami/core/domain/entities/pack_store.dart';
import 'package:zorigami/core/domain/repositories/pack_store_repository.dart';
import 'package:zorigami/core/error/exceptions.dart';
import 'package:zorigami/core/error/failures.dart';

class PackStoreRepositoryImpl extends PackStoreRepository {
  final PackStoreRemoteDataSource remoteDataSource;

  PackStoreRepositoryImpl({
    @required this.remoteDataSource,
  });

  @override
  Future<Result<List<PackStore>, Failure>> getAllPackStores() async {
    try {
      final packStore = await remoteDataSource.getAllPackStores();
      return Ok(packStore);
    } on ServerException catch (e) {
      return Err(ServerFailure(e.toString()));
    }
  }

  @override
  Future<Result<PackStore, Failure>> definePackStore(PackStore input) async {
    try {
      final packStore = await remoteDataSource.definePackStore(input);
      return Ok(packStore);
    } on ServerException catch (e) {
      return Err(ServerFailure(e.toString()));
    }
  }

  @override
  Future<Result<PackStore, Failure>> updatePackStore(PackStore input) async {
    try {
      final packStore = await remoteDataSource.updatePackStore(input);
      return Ok(packStore);
    } on ServerException catch (e) {
      return Err(ServerFailure(e.toString()));
    }
  }

  @override
  Future<Result<PackStore, Failure>> deletePackStore(PackStore input) async {
    try {
      final packStore = await remoteDataSource.deletePackStore(input);
      return Ok(packStore);
    } on ServerException catch (e) {
      return Err(ServerFailure(e.toString()));
    }
  }
}
