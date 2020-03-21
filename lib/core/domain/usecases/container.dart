//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:get_it/get_it.dart';
import 'define_data_set.dart';
import 'define_pack_store.dart';
import 'delete_data_set.dart';
import 'delete_pack_store.dart';
import 'get_configuration.dart';
import 'get_data_sets.dart';
import 'get_pack_stores.dart';
import 'get_snapshot.dart';
import 'get_tree.dart';
import 'update_data_set.dart';
import 'update_pack_store.dart';

void initUseCases(GetIt getIt) {
  getIt.registerLazySingleton(() => DefineDataSet(getIt()));
  getIt.registerLazySingleton(() => DefinePackStore(getIt()));
  getIt.registerLazySingleton(() => DeleteDataSet(getIt()));
  getIt.registerLazySingleton(() => DeletePackStore(getIt()));
  getIt.registerLazySingleton(() => GetConfiguration(getIt()));
  getIt.registerLazySingleton(() => GetDataSets(getIt()));
  getIt.registerLazySingleton(() => GetPackStores(getIt()));
  getIt.registerLazySingleton(() => GetSnapshot(getIt()));
  getIt.registerLazySingleton(() => GetTree(getIt()));
  getIt.registerLazySingleton(() => UpdateDataSet(getIt()));
  getIt.registerLazySingleton(() => UpdatePackStore(getIt()));
}
