//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/data_set.dart';
import 'package:zorigami/core/domain/repositories/data_set_repository.dart';
import 'package:zorigami/core/domain/usecases/usecase.dart';
import 'package:zorigami/core/error/failures.dart';

class GetDataSets implements UseCase<List<DataSet>, NoParams> {
  final DataSetRepository repository;

  GetDataSets(this.repository);

  @override
  Future<Result<List<DataSet>, Failure>> call(NoParams params) async {
    return await repository.getAllDataSets();
  }
}
