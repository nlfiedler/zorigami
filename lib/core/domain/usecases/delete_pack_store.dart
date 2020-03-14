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

/// Returns the key of the deleted pack store.
class DeletePackStore implements UseCase<PackStore, Params> {
  final PackStoreRepository repository;

  DeletePackStore(this.repository);

  @override
  Future<Result<PackStore, Failure>> call(Params params) async {
    return await repository.deletePackStore(params.key);
  }
}

class Params extends Equatable {
  final String key;

  Params({@required this.key});

  @override
  List<Object> get props => [key];
}
