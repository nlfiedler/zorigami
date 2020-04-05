//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:graphql/client.dart';
import 'package:meta/meta.dart';
import 'package:zorigami/core/data/models/configuration_model.dart';
import 'package:zorigami/core/error/exceptions.dart';

abstract class ConfigurationRemoteDataSource {
  Future<ConfigurationModel> getConfiguration();
}

class ConfigurationRemoteDataSourceImpl extends ConfigurationRemoteDataSource {
  final GraphQLClient client;

  ConfigurationRemoteDataSourceImpl({@required this.client});

  @override
  Future<ConfigurationModel> getConfiguration() async {
    final query = r'''
      query {
        configuration {
          hostname
          username
          computerId
        }
      }
    ''';
    final queryOptions = QueryOptions(
      documentNode: gql(query),
      fetchPolicy: FetchPolicy.noCache,
    );
    final QueryResult result = await client.query(queryOptions);
    if (result.hasException) {
      throw ServerException(result.exception.toString());
    }
    final Map<String, dynamic> object =
        result.data['configuration'] as Map<String, dynamic>;
    return object == null ? null : ConfigurationModel.fromJson(object);
  }
}
