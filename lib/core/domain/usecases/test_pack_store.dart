//
// Copyright (c) 2021 Nathan Fiedler
//
import 'package:equatable/equatable.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/pack_store.dart';
import 'package:zorigami/core/domain/repositories/pack_store_repository.dart';
import 'package:zorigami/core/domain/usecases/usecase.dart';
import 'package:zorigami/core/error/failures.dart';

/// Test the given pack store definition.
///
/// Returns either 'ok' or an error message.
class TestPackStore implements UseCase<String, Params> {
  final PackStoreRepository repository;

  TestPackStore(this.repository);

  @override
  Future<Result<String, Failure>> call(Params params) async {
    return await repository.testPackStore(params.store);
  }
}

class Params extends Equatable {
  final PackStore store;

  Params({required this.store});

  @override
  List<Object> get props => [store];

  @override
  bool get stringify => true;
}
