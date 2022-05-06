//
// Copyright (c) 2022 Nathan Fiedler
//
import 'package:graphql/client.dart';
import 'package:zorigami/core/data/models/tree_model.dart';
import 'package:zorigami/core/error/exceptions.dart' as err;

abstract class TreeRemoteDataSource {
  Future<TreeModel?> getTree(String checksum);
}

class TreeRemoteDataSourceImpl extends TreeRemoteDataSource {
  final GraphQLClient client;

  TreeRemoteDataSourceImpl({required this.client});

  @override
  Future<TreeModel?> getTree(String checksum) async {
    const query = r'''
      query Fetch($checksum: Checksum!) {
        tree(digest: $checksum) {
          entries {
            name
            modTime
            reference
          }
        }
      }
    ''';
    final queryOptions = QueryOptions(
      document: gql(query),
      variables: <String, dynamic>{
        'checksum': checksum,
      },
      fetchPolicy: FetchPolicy.noCache,
    );
    final QueryResult result = await client.query(queryOptions);
    if (result.hasException) {
      throw err.ServerException(result.exception.toString());
    }
    if (result.data?['tree'] == null) {
      return null;
    }
    return TreeModel.fromJson(
      result.data?['tree'] as Map<String, dynamic>,
    );
  }
}
