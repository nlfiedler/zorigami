//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/snapshot.dart';
import 'package:zorigami/core/error/failures.dart';

abstract class SnapshotRepository {
  /// Retrieve a snapshot by its hash digset.
  Future<Result<Snapshot, Failure>> getSnapshot(String checksum);

  /// Restore the latest database snapshot from the pack store.
  Future<Result<String, Failure>> restoreDatabase(String storeId);

  /// Restore a file, returning the path to the restored file.
  Future<Result<String, Failure>> restoreFile(
      String checksum, String filepath, String dataset);
}
