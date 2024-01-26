//
// Copyright (c) 2024 Nathan Fiedler
//
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:graphql/client.dart' as gql;
import 'package:zorigami/core/data/repositories/configuration_repository_impl.dart';
import 'package:zorigami/core/data/repositories/data_set_repository_impl.dart';
import 'package:zorigami/core/data/repositories/pack_store_repository_impl.dart';
import 'package:zorigami/core/data/repositories/snapshot_repository_impl.dart';
import 'package:zorigami/core/data/repositories/tree_repository_impl.dart';
import 'package:zorigami/core/data/sources/configuration_remote_data_source.dart';
import 'package:zorigami/core/data/sources/data_set_remote_data_source.dart';
import 'package:zorigami/core/data/sources/pack_store_remote_data_source.dart';
import 'package:zorigami/core/data/sources/snapshot_remote_data_source.dart';
import 'package:zorigami/core/data/sources/tree_remote_data_source.dart';
import 'package:zorigami/core/domain/repositories/configuration_repository.dart';
import 'package:zorigami/core/domain/repositories/data_set_repository.dart';
import 'package:zorigami/core/domain/repositories/pack_store_repository.dart';
import 'package:zorigami/core/domain/repositories/snapshot_repository.dart';
import 'package:zorigami/core/domain/repositories/tree_repository.dart';
import 'package:zorigami/environment_config.dart';

final graphqlProvider = Provider<gql.GraphQLClient>((ref) {
  const uri = '${EnvironmentConfig.base_url}/graphql';
  return gql.GraphQLClient(
    link: gql.HttpLink(uri),
    cache: gql.GraphQLCache(),
  );
});

final configurationDataSourceProvider =
    Provider<ConfigurationRemoteDataSource>((ref) {
  return ConfigurationRemoteDataSourceImpl(
    client: ref.read(graphqlProvider),
  );
});

final configurationRepositoryProvider = Provider<ConfigurationRepository>(
  (ref) => ConfigurationRepositoryImpl(
    remoteDataSource: ref.read(configurationDataSourceProvider),
  ),
);

final datasetDataSourceProvider = Provider<DataSetRemoteDataSource>(
  (ref) => DataSetRemoteDataSourceImpl(
    client: ref.read(graphqlProvider),
  ),
);

final datasetRepositoryProvider = Provider<DataSetRepository>(
  (ref) => DataSetRepositoryImpl(
    remoteDataSource: ref.read(datasetDataSourceProvider),
  ),
);

final packStoreDataSourceProvider = Provider<PackStoreRemoteDataSource>(
  (ref) => PackStoreRemoteDataSourceImpl(
    client: ref.read(graphqlProvider),
  ),
);

final packStoreRepositoryProvider = Provider<PackStoreRepository>(
  (ref) => PackStoreRepositoryImpl(
    remoteDataSource: ref.read(packStoreDataSourceProvider),
  ),
);

final snapshotDataSourceProvider = Provider<SnapshotRemoteDataSource>((ref) {
  return SnapshotRemoteDataSourceImpl(
    client: ref.read(graphqlProvider),
  );
});

final snapshotRepositoryProvider = Provider<SnapshotRepository>(
  (ref) => SnapshotRepositoryImpl(
    remoteDataSource: ref.read(snapshotDataSourceProvider),
  ),
);

final treeDataSourceProvider = Provider<TreeRemoteDataSource>((ref) {
  return TreeRemoteDataSourceImpl(
    client: ref.read(graphqlProvider),
  );
});

final treeRepositoryProvider = Provider<TreeRepository>(
  (ref) => TreeRepositoryImpl(
    remoteDataSource: ref.read(treeDataSourceProvider),
  ),
);
