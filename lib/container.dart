//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:get_it/get_it.dart';
import 'package:graphql/client.dart';
import 'package:zorigami/core/data/repositories/container.dart';
import 'package:zorigami/core/data/sources/container.dart';
import 'package:zorigami/core/domain/usecases/container.dart';
import 'package:zorigami/core/util/input_converter.dart';
import 'package:zorigami/features/browse/preso/bloc/configuration_bloc.dart';
import 'package:zorigami/features/browse/preso/bloc/datasets_bloc.dart';
import 'package:zorigami/features/browse/preso/bloc/snapshot_bloc.dart';
import 'package:zorigami/features/browse/preso/bloc/snapshot_browser_bloc.dart';
import 'package:zorigami/features/browse/preso/bloc/tree_bloc.dart';
import 'package:zorigami/features/browse/preso/bloc/tree_browser_bloc.dart';

final getIt = GetIt.instance;

void init() {
  // bloc
  getIt.registerFactory(
    () => ConfigurationBloc(usecase: getIt()),
  );
  getIt.registerFactory(
    () => DatasetsBloc(getDataSets: getIt()),
  );
  getIt.registerFactory(
    () => SnapshotBloc(usecase: getIt()),
  );
  getIt.registerFactory(
    () => SnapshotBrowserBloc(usecase: getIt()),
  );
  getIt.registerFactory(
    () => TreeBloc(usecase: getIt()),
  );
  getIt.registerFactory(
    () => TreeBrowserBloc(usecase: getIt()),
  );

  initUseCases(getIt);
  initRepositories(getIt);
  initDataSources(getIt);

  // core
  getIt.registerLazySingleton(() => InputConverter());

  // external
  getIt.registerLazySingleton(() {
    return GraphQLClient(
      link: HttpLink(uri: 'http://127.0.0.1:8080/graphql'),
      cache: InMemoryCache(),
    );
  });
}
