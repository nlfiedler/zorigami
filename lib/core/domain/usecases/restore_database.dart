//
// Copyright (c) 2024 Nathan Fiedler
//
import 'package:equatable/equatable.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/repositories/snapshot_repository.dart';
import 'package:zorigami/core/domain/usecases/usecase.dart';
import 'package:zorigami/core/error/failures.dart';

class RestoreDatabase implements UseCase<String, Params> {
  final SnapshotRepository repository;

  RestoreDatabase(this.repository);

  @override
  Future<Result<String, Failure>> call(Params params) async {
    return await repository.restoreDatabase(params.storeId);
  }
}

class Params extends Equatable {
  /// Identifier of pack store from which to restore database.
  final String storeId;

  const Params({required this.storeId});

  @override
  List<Object> get props => [storeId];

  @override
  bool get stringify => true;
}
