//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:graphql/client.dart' as gql;
import 'package:gql/language.dart' as lang;
import 'package:gql/ast.dart' as ast;
import 'package:normalize/utils.dart';
import 'package:zorigami/core/data/models/tree_model.dart';
import 'package:zorigami/core/error/exceptions.dart';

abstract class TreeRemoteDataSource {
  Future<TreeModel?> getTree(String checksum);
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

class TreeRemoteDataSourceImpl extends TreeRemoteDataSource {
  final gql.GraphQLClient client;

  TreeRemoteDataSourceImpl({required this.client});

  @override
  Future<TreeModel?> getTree(String checksum) async {
    final query = r'''
      query Fetch($checksum: Checksum!) {
        tree(digest: $checksum) {
          entries {
            name
            fstype
            modTime
            reference
          }
        }
      }
    ''';
    final queryOptions = gql.QueryOptions(
      document: gqlNoTypename(query),
      variables: <String, dynamic>{
        'checksum': checksum,
      },
      fetchPolicy: gql.FetchPolicy.noCache,
    );
    final gql.QueryResult result = await client.query(queryOptions);
    if (result.hasException) {
      throw ServerException(result.exception.toString());
    }
    if (result.data?['tree'] == null) {
      return null;
    }
    return TreeModel.fromJson(
      result.data?['tree'] as Map<String, dynamic>,
    );
  }
}
