//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:equatable/equatable.dart';
import 'package:flutter/material.dart';
import 'package:meta/meta.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/repositories/snapshot_repository.dart';
import 'package:zorigami/core/domain/usecases/usecase.dart';
import 'package:zorigami/core/error/failures.dart';

class RestoreFile implements UseCase<String, Params> {
  final SnapshotRepository repository;

  RestoreFile(this.repository);

  @override
  Future<Result<String, Failure>> call(Params params) async {
    return await repository.restoreFile(
        params.digest, params.filepath, params.dataset);
  }
}

class Params extends Equatable {
  /// Digest of the file to be restored.
  final String digest;

  /// Relative path to which file will be "put back."
  final String filepath;

  /// Identifier of the dataset containing the file.
  final String dataset;

  Params(
      {@required this.digest, @required this.filepath, @required this.dataset});

  @override
  List<Object> get props => [digest];

  @override
  bool get stringify => true;
}
