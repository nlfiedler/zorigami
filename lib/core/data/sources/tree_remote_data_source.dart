//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:graphql/client.dart';
import 'package:meta/meta.dart';
import 'package:zorigami/core/data/models/tree_model.dart';
import 'package:zorigami/core/error/exceptions.dart';

abstract class TreeRemoteDataSource {
  Future<TreeModel> getTree(String checksum);
}

class TreeRemoteDataSourceImpl extends TreeRemoteDataSource {
  final GraphQLClient client;

  TreeRemoteDataSourceImpl({@required this.client});

  @override
  Future<TreeModel> getTree(String checksum) async {
    final getTree = r'''
      query Fetch($checksum: Checksum!) {
        tree(digest: $checksum) {
          name
          fstype
          modTime
          reference
        }
      }
    ''';
    final queryOptions = QueryOptions(
      documentNode: gql(getTree),
      variables: <String, dynamic>{
        'checksum': checksum,
      },
    );
    final QueryResult result = await client.query(queryOptions);
    if (result.hasException) {
      throw ServerException(result.exception.toString());
    }
    final Map<String, dynamic> tree =
        result.data['tree'] as Map<String, dynamic>;
    return TreeModel.fromJson(tree);
  }
}
