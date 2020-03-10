//
// Copyright (c) 2019 Nathan Fiedler
//
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/pack_store.dart';
import 'package:zorigami/core/error/failures.dart';

abstract class PackStoreRepository {
  /// Retrieve all pack stores.
  Future<Result<List<PackStore>, Failure>> getPackStores();

  /// Define a new pack store.
  Future<Result<PackStore, Failure>> definePackStore(
      String kind, String options);

  /// Update an existing pack store.
  Future<Result<PackStore, Failure>> updatePackStore(
      String key, String options);

  /// Remove a pack store.
  Future<Result<String, Failure>> deletePackStore(String key);
}
