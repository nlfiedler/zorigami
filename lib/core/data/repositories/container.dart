//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:get_it/get_it.dart';
import 'package:zorigami/core/domain/repositories/configuration_repository.dart';
import 'package:zorigami/core/domain/repositories/data_set_repository.dart';
import 'package:zorigami/core/domain/repositories/pack_store_repository.dart';
import 'package:zorigami/core/domain/repositories/snapshot_repository.dart';
import 'package:zorigami/core/domain/repositories/tree_repository.dart';
import 'configuration_repository_impl.dart';
import 'data_set_repository_impl.dart';
import 'pack_store_repository_impl.dart';
import 'snapshot_repository_impl.dart';
import 'tree_repository_impl.dart';

void initRepositories(GetIt getIt) {
  getIt.registerLazySingleton<ConfigurationRepository>(
    () => ConfigurationRepositoryImpl(remoteDataSource: getIt()),
  );
  getIt.registerLazySingleton<DataSetRepository>(
    () => DataSetRepositoryImpl(remoteDataSource: getIt()),
  );
  getIt.registerLazySingleton<PackStoreRepository>(
    () => PackStoreRepositoryImpl(remoteDataSource: getIt()),
  );
  getIt.registerLazySingleton<SnapshotRepository>(
    () => SnapshotRepositoryImpl(remoteDataSource: getIt()),
  );
  getIt.registerLazySingleton<TreeRepository>(
    () => TreeRepositoryImpl(remoteDataSource: getIt()),
  );
}
