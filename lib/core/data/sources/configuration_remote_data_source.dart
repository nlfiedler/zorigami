//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:graphql/client.dart' as gql;
import 'package:gql/language.dart' as lang;
import 'package:gql/ast.dart' as ast;
import 'package:normalize/utils.dart';
import 'package:zorigami/core/data/models/configuration_model.dart';
import 'package:zorigami/core/error/exceptions.dart';

abstract class ConfigurationRemoteDataSource {
  Future<ConfigurationModel?> getConfiguration();
}

// Work around bug in juniper in which it fails to implement __typename for the
// root query, which is in violation of the GraphQL spec.
//
// c.f. https://github.com/graphql-rust/juniper/issues/372
class AddNestedTypenameVisitor extends AddTypenameVisitor {
  @override
  ast.OperationDefinitionNode visitOperationDefinitionNode(
    ast.OperationDefinitionNode node,
  ) =>
      node;
}

ast.DocumentNode gqlNoTypename(String document) => ast.transform(
      lang.parseString(document),
      [AddNestedTypenameVisitor()],
    );

class ConfigurationRemoteDataSourceImpl extends ConfigurationRemoteDataSource {
  final gql.GraphQLClient client;

  ConfigurationRemoteDataSourceImpl({required this.client});

  @override
  Future<ConfigurationModel?> getConfiguration() async {
    final query = r'''
      query {
        configuration {
          hostname
          username
          computerId
        }
      }
    ''';
    final queryOptions = gql.QueryOptions(
      document: gqlNoTypename(query),
      fetchPolicy: gql.FetchPolicy.noCache,
    );
    final gql.QueryResult result = await client.query(queryOptions);
    if (result.hasException) {
      throw ServerException(result.exception.toString());
    }
    if (result.data?['configuration'] == null) {
      return null;
    }
    return ConfigurationModel.fromJson(
      result.data?['configuration'] as Map<String, dynamic>,
    );
  }
}
