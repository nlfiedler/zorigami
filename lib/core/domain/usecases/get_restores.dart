//
// Copyright (c) 2021 Nathan Fiedler
//
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/request.dart';
import 'package:zorigami/core/domain/repositories/snapshot_repository.dart';
import 'package:zorigami/core/domain/usecases/usecase.dart';
import 'package:zorigami/core/error/failures.dart';

class GetRestores implements UseCase<List<Request>, NoParams> {
  final SnapshotRepository repository;

  GetRestores(this.repository);

  @override
  Future<Result<List<Request>, Failure>> call(NoParams params) async {
    return await repository.getAllRestores();
  }
}
