//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:get_it/get_it.dart';
import 'configuration_remote_data_source.dart';
import 'data_set_remote_data_source.dart';
import 'pack_store_remote_data_source.dart';
import 'snapshot_remote_data_source.dart';
import 'tree_remote_data_source.dart';

void initDataSources(GetIt getIt) {
  getIt.registerLazySingleton<ConfigurationRemoteDataSource>(
    () => ConfigurationRemoteDataSourceImpl(client: getIt()),
  );
  getIt.registerLazySingleton<DataSetRemoteDataSource>(
    () => DataSetRemoteDataSourceImpl(client: getIt()),
  );
  getIt.registerLazySingleton<PackStoreRemoteDataSource>(
    () => PackStoreRemoteDataSourceImpl(client: getIt()),
  );
  getIt.registerLazySingleton<SnapshotRemoteDataSource>(
    () => SnapshotRemoteDataSourceImpl(client: getIt()),
  );
  getIt.registerLazySingleton<TreeRemoteDataSource>(
    () => TreeRemoteDataSourceImpl(client: getIt()),
  );
}
