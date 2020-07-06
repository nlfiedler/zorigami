//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/pack_store.dart';
import 'package:zorigami/core/domain/repositories/pack_store_repository.dart';
import 'package:zorigami/core/domain/usecases/usecase.dart';
import 'package:zorigami/core/error/failures.dart';

class GetPackStores implements UseCase<List<PackStore>, NoParams> {
  final PackStoreRepository repository;

  GetPackStores(this.repository);

  @override
  Future<Result<List<PackStore>, Failure>> call(NoParams params) async {
    return await repository.getAllPackStores();
  }
}
