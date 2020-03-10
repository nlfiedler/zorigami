//
// Copyright (c) 2019 Nathan Fiedler
//
import 'package:equatable/equatable.dart';
import 'package:meta/meta.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/pack_store.dart';
import 'package:zorigami/core/domain/repositories/pack_store_repository.dart';
import 'package:zorigami/core/usecases/usecase.dart';
import 'package:zorigami/core/error/failures.dart';

class DefinePackStore implements UseCase<PackStore, Params> {
  final PackStoreRepository repository;

  DefinePackStore(this.repository);

  @override
  Future<Result<PackStore, Failure>> call(Params params) async {
    return await repository.definePackStore(params.kind, params.options);
  }
}

class Params extends Equatable {
  final String kind;
  final String options;

  Params({@required this.kind, @required this.options});

  @override
  List<Object> get props => [kind, options];
}
