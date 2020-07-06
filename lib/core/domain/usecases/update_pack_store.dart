//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:equatable/equatable.dart';
import 'package:meta/meta.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/pack_store.dart';
import 'package:zorigami/core/domain/repositories/pack_store_repository.dart';
import 'package:zorigami/core/domain/usecases/usecase.dart';
import 'package:zorigami/core/error/failures.dart';

class UpdatePackStore implements UseCase<PackStore, Params> {
  final PackStoreRepository repository;

  UpdatePackStore(this.repository);

  @override
  Future<Result<PackStore, Failure>> call(Params params) async {
    return await repository.updatePackStore(params.store);
  }
}

class Params extends Equatable {
  final PackStore store;

  Params({@required this.store});

  @override
  List<Object> get props => [store];
}
