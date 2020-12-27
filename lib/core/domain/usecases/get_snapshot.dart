//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:equatable/equatable.dart';
import 'package:meta/meta.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/snapshot.dart';
import 'package:zorigami/core/domain/repositories/snapshot_repository.dart';
import 'package:zorigami/core/domain/usecases/usecase.dart';
import 'package:zorigami/core/error/failures.dart';

class GetSnapshot implements UseCase<Snapshot, Params> {
  final SnapshotRepository repository;

  GetSnapshot(this.repository);

  @override
  Future<Result<Snapshot, Failure>> call(Params params) async {
    return await repository.getSnapshot(params.checksum);
  }
}

class Params extends Equatable {
  final String checksum;

  Params({@required this.checksum});

  @override
  List<Object> get props => [checksum];

  @override
  bool get stringify => true;
}
