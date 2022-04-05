//
// Copyright (c) 2022 Nathan Fiedler
//
import 'package:equatable/equatable.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/repositories/snapshot_repository.dart';
import 'package:zorigami/core/domain/usecases/usecase.dart';
import 'package:zorigami/core/error/failures.dart';

class RestoreFiles implements UseCase<bool, Params> {
  final SnapshotRepository repository;

  RestoreFiles(this.repository);

  @override
  Future<Result<bool, Failure>> call(Params params) async {
    return await repository.restoreFiles(
        params.tree, params.entry, params.filepath, params.dataset);
  }
}

class Params extends Equatable {
  /// Digest of the tree containing the entry to restore.
  final String tree;

  /// Name of the entry within the tree to be restored.
  final String entry;

  /// Relative path to which file will be "put back."
  final String filepath;

  /// Identifier of the dataset containing the file.
  final String dataset;

  const Params({
    required this.tree,
    required this.entry,
    required this.filepath,
    required this.dataset,
  });

  @override
  List<Object> get props => [tree, entry];

  @override
  bool get stringify => true;
}
