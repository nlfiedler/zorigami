//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:equatable/equatable.dart';
import 'package:meta/meta.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/tree.dart';
import 'package:zorigami/core/domain/repositories/tree_repository.dart';
import 'package:zorigami/core/domain/usecases/usecase.dart';
import 'package:zorigami/core/error/failures.dart';

class GetTree implements UseCase<Tree, Params> {
  final TreeRepository repository;

  GetTree(this.repository);

  @override
  Future<Result<Tree, Failure>> call(Params params) async {
    return await repository.getTree(params.checksum);
  }
}

class Params extends Equatable {
  final String checksum;

  Params({@required this.checksum});

  @override
  List<Object> get props => [checksum];
}
