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
    } on ServerException {
      return Err(ServerFailure());
    }
  }

  @override
  Future<Result<PackStore, Failure>> definePackStore(
      String kind, Map<String, dynamic> options) async {
    try {
      final packStore = await remoteDataSource.definePackStore(kind, options);
      return Ok(packStore);
    } on ServerException {
      return Err(ServerFailure());
    }
  }

  @override
  Future<Result<PackStore, Failure>> updatePackStore(
      String key, Map<String, dynamic> options) async {
    try {
      final packStore = await remoteDataSource.updatePackStore(key, options);
      return Ok(packStore);
    } on ServerException {
      return Err(ServerFailure());
    }
  }

  @override
  Future<Result<PackStore, Failure>> deletePackStore(String key) async {
    try {
      final packStore = await remoteDataSource.deletePackStore(key);
      return Ok(packStore);
    } on ServerException {
      return Err(ServerFailure());
    }
  }
}
