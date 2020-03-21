//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:get_it/get_it.dart';
import 'package:graphql/client.dart';
import 'package:zorigami/core/data/repositories/container.dart';
import 'package:zorigami/core/data/sources/container.dart';
import 'package:zorigami/core/domain/usecases/container.dart';
import 'package:zorigami/features/browse/preso/bloc/configuration_bloc.dart';
import 'package:zorigami/core/util/input_converter.dart';

final getIt = GetIt.instance;

void init() {
  // bloc
  getIt.registerFactory(
    () => ConfigurationBloc(usecase: getIt()),
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
