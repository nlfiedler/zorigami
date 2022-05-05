//
// Copyright (c) 2022 Nathan Fiedler
//
import 'package:equatable/equatable.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/data_set.dart';
import 'package:zorigami/core/domain/repositories/data_set_repository.dart';
import 'package:zorigami/core/domain/usecases/usecase.dart';
import 'package:zorigami/core/error/failures.dart';

class StopBackup implements UseCase<bool, Params> {
  final DataSetRepository repository;

  StopBackup(this.repository);

  @override
  Future<Result<bool, Failure>> call(Params params) async {
    return await repository.stopBackup(params.dataset);
  }
}

class Params extends Equatable {
  final DataSet dataset;

  const Params({required this.dataset});

  @override
  List<Object> get props => [dataset];

  @override
  bool get stringify => true;
}
