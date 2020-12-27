//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:equatable/equatable.dart';
import 'package:meta/meta.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/data_set.dart';
import 'package:zorigami/core/domain/repositories/data_set_repository.dart';
import 'package:zorigami/core/domain/usecases/usecase.dart';
import 'package:zorigami/core/error/failures.dart';

class DefineDataSet implements UseCase<DataSet, Params> {
  final DataSetRepository repository;

  DefineDataSet(this.repository);

  @override
  Future<Result<DataSet, Failure>> call(Params params) async {
    return await repository.defineDataSet(params.dataset);
  }
}

class Params extends Equatable {
  final DataSet dataset;

  Params({@required this.dataset});

  @override
  List<Object> get props => [dataset];

  @override
  bool get stringify => true;
}
