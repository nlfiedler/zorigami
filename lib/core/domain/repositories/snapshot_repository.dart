//
// Copyright (c) 2022 Nathan Fiedler
//
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/request.dart';
import 'package:zorigami/core/domain/entities/snapshot.dart';
import 'package:zorigami/core/error/failures.dart';

abstract class SnapshotRepository {
  /// Retrieve a snapshot by its hash digset.
  Future<Result<Snapshot, Failure>> getSnapshot(String checksum);

  /// Restore the latest database snapshot from the pack store.
  Future<Result<String, Failure>> restoreDatabase(String storeId);

  /// Restore a single file or an entire directory structure.
  ///
  /// Returns true if the restore request was successfully enqueued.
  Future<Result<bool, Failure>> restoreFiles(
      String tree, String entry, String filepath, String dataset);

  /// Get all file restore requests.
  Future<Result<List<Request>, Failure>> getAllRestores();

  /// Cancel a file restore request.
  ///
  /// Returns true if the cancellation was successfully enqueued.
  Future<Result<bool, Failure>> cancelRestore(
      String tree, String entry, String filepath, String dataset);
}
