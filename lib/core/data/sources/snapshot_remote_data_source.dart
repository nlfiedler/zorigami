//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:graphql/client.dart' as gql;
import 'package:gql/language.dart' as lang;
import 'package:gql/ast.dart' as ast;
import 'package:normalize/utils.dart';
import 'package:zorigami/core/data/models/request_model.dart';
import 'package:zorigami/core/data/models/snapshot_model.dart';
import 'package:zorigami/core/error/exceptions.dart';

abstract class SnapshotRemoteDataSource {
  Future<SnapshotModel?> getSnapshot(String checksum);
  Future<String> restoreDatabase(String storeId);
  Future<bool> restoreFiles(String checksum, String filepath, String dataset);
  Future<List<RequestModel>> getAllRestores();
  Future<bool> cancelRestore(String checksum, String filepath, String dataset);
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

class SnapshotRemoteDataSourceImpl extends SnapshotRemoteDataSource {
  final gql.GraphQLClient client;

  SnapshotRemoteDataSourceImpl({required this.client});

  @override
  Future<SnapshotModel?> getSnapshot(String checksum) async {
    final query = r'''
      query Fetch($checksum: Checksum!) {
        snapshot(digest: $checksum) {
          checksum
          parent
          startTime
          endTime
          fileCount
          tree
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
    if (result.data?['snapshot'] == null) {
      return null;
    }
    return SnapshotModel.fromJson(
      result.data?['snapshot'] as Map<String, dynamic>,
    );
  }

  @override
  Future<String> restoreDatabase(String storeId) async {
    final query = r'''
      mutation Restore($storeId: String!) {
        restoreDatabase(storeId: $storeId)
      }
    ''';
    final mutationOptions = gql.MutationOptions(
      document: gqlNoTypename(query),
      variables: <String, dynamic>{
        'storeId': storeId,
      },
      fetchPolicy: gql.FetchPolicy.noCache,
    );
    final gql.QueryResult result = await client.mutate(mutationOptions);
    if (result.hasException) {
      throw ServerException(result.exception.toString());
    }
    return result.data?['restoreDatabase'] ?? 'ng';
  }

  @override
  Future<bool> restoreFiles(
    String checksum,
    String filepath,
    String dataset,
  ) async {
    final query = r'''
      mutation Restore($digest: Checksum!, $filepath: String!, $dataset: String!) {
        restoreFiles(digest: $digest, filepath: $filepath, dataset: $dataset)
      }
    ''';
    final mutationOptions = gql.MutationOptions(
      document: gqlNoTypename(query),
      variables: <String, dynamic>{
        'digest': checksum,
        'filepath': filepath,
        'dataset': dataset,
      },
      fetchPolicy: gql.FetchPolicy.noCache,
    );
    final gql.QueryResult result = await client.mutate(mutationOptions);
    if (result.hasException) {
      throw ServerException(result.exception.toString());
    }
    return (result.data?['restoreFiles'] ?? false) as bool;
  }

  @override
  Future<List<RequestModel>> getAllRestores() async {
    final query = r'''
      query {
        restores {
          digest
          filepath
          dataset
          finished
          filesRestored
          errorMessage
        }
      }
    ''';
    final queryOptions = gql.QueryOptions(
      document: gqlNoTypename(query),
      variables: <String, dynamic>{},
      fetchPolicy: gql.FetchPolicy.noCache,
    );
    final gql.QueryResult result = await client.query(queryOptions);
    if (result.hasException) {
      throw ServerException(result.exception.toString());
    }
    final List<dynamic> restores =
        (result.data?['restores'] ?? []) as List<dynamic>;
    final List<RequestModel> results = List.from(
      restores.map<RequestModel>((e) {
        return RequestModel.fromJson(e);
      }),
    );
    return results;
  }

  @override
  Future<bool> cancelRestore(
    String checksum,
    String filepath,
    String dataset,
  ) async {
    final query = r'''
      mutation Cancel($digest: Checksum!, $filepath: String!, $dataset: String!) {
        cancelRestore(digest: $digest, filepath: $filepath, dataset: $dataset)
      }
    ''';
    final mutationOptions = gql.MutationOptions(
      document: gqlNoTypename(query),
      variables: <String, dynamic>{
        'digest': checksum,
        'filepath': filepath,
        'dataset': dataset,
      },
      fetchPolicy: gql.FetchPolicy.noCache,
    );
    final gql.QueryResult result = await client.mutate(mutationOptions);
    if (result.hasException) {
      throw ServerException(result.exception.toString());
    }
    return (result.data?['cancelRestore'] ?? false) as bool;
  }
}
