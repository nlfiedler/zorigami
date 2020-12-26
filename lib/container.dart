//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:get_it/get_it.dart';
import 'package:graphql/client.dart';
import 'package:zorigami/core/data/repositories/container.dart';
import 'package:zorigami/core/data/sources/container.dart';
import 'package:zorigami/core/domain/usecases/container.dart';
import 'package:zorigami/environment_config.dart';
import 'package:zorigami/features/backup/preso/bloc/create_data_sets_bloc.dart';
import 'package:zorigami/features/backup/preso/bloc/create_pack_stores_bloc.dart';
import 'package:zorigami/features/backup/preso/bloc/edit_data_sets_bloc.dart';
import 'package:zorigami/features/backup/preso/bloc/edit_pack_stores_bloc.dart';
import 'package:zorigami/features/backup/preso/bloc/pack_stores_bloc.dart';
import 'package:zorigami/features/browse/preso/bloc/configuration_bloc.dart';
import 'package:zorigami/features/browse/preso/bloc/data_sets_bloc.dart';
import 'package:zorigami/features/browse/preso/bloc/snapshot_browser_bloc.dart';
import 'package:zorigami/features/browse/preso/bloc/tree_browser_bloc.dart';

final getIt = GetIt.instance;

void init() {
  // bloc
  getIt.registerFactory(
    () => ConfigurationBloc(usecase: getIt()),
  );
  getIt.registerFactory(
    () => DataSetsBloc(usecase: getIt()),
  );
  getIt.registerFactory(
    () => PackStoresBloc(usecase: getIt()),
  );
  getIt.registerFactory(
    () => CreatePackStoresBloc(usecase: getIt()),
  );
  getIt.registerFactory(
    () => CreateDataSetsBloc(usecase: getIt()),
  );
  getIt.registerFactory(
    () => EditDataSetsBloc(
      updateDataSet: getIt(),
      deleteDataSet: getIt(),
    ),
  );
  getIt.registerFactory(
    () => EditPackStoresBloc(
      updatePackStore: getIt(),
      deletePackStore: getIt(),
    ),
  );
  getIt.registerFactory(
    () => SnapshotBrowserBloc(usecase: getIt()),
  );
  getIt.registerFactory(
    () => TreeBrowserBloc(getTree: getIt(), restoreFile: getIt()),
  );

  initUseCases(getIt);
  initRepositories(getIt);
  initDataSources(getIt);

  // external
  getIt.registerLazySingleton(() {
    // seems a relative URL is not supported by the client package
    final uri = '${EnvironmentConfig.base_url}/graphql';
    return GraphQLClient(
      link: HttpLink(uri: uri),
      cache: InMemoryCache(),
    );
  });
}
