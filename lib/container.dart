//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:get_it/get_it.dart';
import 'package:graphql/client.dart';
import 'package:zorigami/core/data/models/pack_store_model.dart';
import 'package:zorigami/core/data/repositories/container.dart';
import 'package:zorigami/core/data/sources/container.dart';
import 'package:zorigami/core/domain/usecases/container.dart';
import 'package:zorigami/core/util/input_converter.dart';
import 'package:zorigami/features/backup/preso/bloc/edit_pack_stores_bloc.dart';
import 'package:zorigami/features/backup/preso/bloc/pack_stores_bloc.dart';
import 'package:zorigami/features/backup/preso/widgets/pack_store_form.dart';
import 'package:zorigami/features/backup/preso/widgets/store_form_factory.dart';
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
    () => PackStoresBloc(usecase: getIt()),
  );
  getIt.registerFactory(
    () => EditPackStoresBloc(updatePackStore: getIt()),
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

  // widgets
  // (would prefer using PackStore here but get_it rejects it at runtime)
  getIt.registerFactoryParam<PackStoreForm, PackStoreModel, void>(
    (param1, param2) => buildStoreForm(param1, param2),
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
