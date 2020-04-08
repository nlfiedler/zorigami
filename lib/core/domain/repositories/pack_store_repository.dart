//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/pack_store.dart';
import 'package:zorigami/core/error/failures.dart';

abstract class PackStoreRepository {
  /// Retrieve all pack stores.
  Future<Result<List<PackStore>, Failure>> getAllPackStores();

  /// Define a new pack store.
  Future<Result<PackStore, Failure>> definePackStore(PackStore input);

  /// Update an existing pack store.
  Future<Result<PackStore, Failure>> updatePackStore(PackStore input);

  /// Remove a pack store.
  Future<Result<PackStore, Failure>> deletePackStore(PackStore input);
}
