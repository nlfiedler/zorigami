//
// Copyright (c) 2022 Nathan Fiedler
//
import 'package:graphql/client.dart';
import 'package:zorigami/core/data/models/configuration_model.dart';
import 'package:zorigami/core/error/exceptions.dart' as err;

abstract class ConfigurationRemoteDataSource {
  Future<ConfigurationModel?> getConfiguration();
}

class ConfigurationRemoteDataSourceImpl extends ConfigurationRemoteDataSource {
  final GraphQLClient client;

  ConfigurationRemoteDataSourceImpl({required this.client});

  @override
  Future<ConfigurationModel?> getConfiguration() async {
    const query = r'''
      query {
        configuration {
          hostname
          username
          computerId
        }
      }
    ''';
    final queryOptions = QueryOptions(
      document: gql(query),
      fetchPolicy: FetchPolicy.noCache,
    );
    final QueryResult result = await client.query(queryOptions);
    if (result.hasException) {
      throw err.ServerException(result.exception.toString());
    }
    if (result.data?['configuration'] == null) {
      return null;
    }
    return ConfigurationModel.fromJson(
      result.data?['configuration'] as Map<String, dynamic>,
    );
  }
}
