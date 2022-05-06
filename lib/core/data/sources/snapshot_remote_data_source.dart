//
// Copyright (c) 2022 Nathan Fiedler
//
import 'package:graphql/client.dart';
import 'package:zorigami/core/data/models/request_model.dart';
import 'package:zorigami/core/data/models/snapshot_model.dart';
import 'package:zorigami/core/error/exceptions.dart' as err;

abstract class SnapshotRemoteDataSource {
  Future<SnapshotModel?> getSnapshot(String checksum);
  Future<String> restoreDatabase(String storeId);
  Future<bool> restoreFiles(
      String tree, String entry, String filepath, String dataset);
  Future<List<RequestModel>> getAllRestores();
  Future<bool> cancelRestore(
      String tree, String entry, String filepath, String dataset);
}

class SnapshotRemoteDataSourceImpl extends SnapshotRemoteDataSource {
  final GraphQLClient client;

  SnapshotRemoteDataSourceImpl({required this.client});

  @override
  Future<SnapshotModel?> getSnapshot(String checksum) async {
    const query = r'''
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
    if (result.data?['snapshot'] == null) {
      return null;
    }
    return SnapshotModel.fromJson(
      result.data?['snapshot'] as Map<String, dynamic>,
    );
  }

  @override
  Future<String> restoreDatabase(String storeId) async {
    const query = r'''
      mutation Restore($storeId: String!) {
        restoreDatabase(storeId: $storeId)
      }
    ''';
    final mutationOptions = MutationOptions(
      document: gql(query),
      variables: <String, dynamic>{
        'storeId': storeId,
      },
      fetchPolicy: FetchPolicy.noCache,
    );
    final QueryResult result = await client.mutate(mutationOptions);
    if (result.hasException) {
      throw err.ServerException(result.exception.toString());
    }
    return result.data?['restoreDatabase'] ?? 'ng';
  }

  @override
  Future<bool> restoreFiles(
    String tree,
    String entry,
    String filepath,
    String dataset,
  ) async {
    const query = r'''
      mutation Restore($tree: Checksum!, $entry: String!, $filepath: String!, $dataset: String!) {
        restoreFiles(tree: $tree, entry: $entry, filepath: $filepath, dataset: $dataset)
      }
    ''';
    final mutationOptions = MutationOptions(
      document: gql(query),
      variables: <String, dynamic>{
        'tree': tree,
        'entry': entry,
        'filepath': filepath,
        'dataset': dataset,
      },
      fetchPolicy: FetchPolicy.noCache,
    );
    final QueryResult result = await client.mutate(mutationOptions);
    if (result.hasException) {
      throw err.ServerException(result.exception.toString());
    }
    return (result.data?['restoreFiles'] ?? false) as bool;
  }

  @override
  Future<List<RequestModel>> getAllRestores() async {
    const query = r'''
      query {
        restores {
          tree
          entry
          filepath
          dataset
          finished
          filesRestored
          errorMessage
        }
      }
    ''';
    final queryOptions = QueryOptions(
      document: gql(query),
      variables: const <String, dynamic>{},
      fetchPolicy: FetchPolicy.noCache,
    );
    final QueryResult result = await client.query(queryOptions);
    if (result.hasException) {
      throw err.ServerException(result.exception.toString());
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
    String tree,
    String entry,
    String filepath,
    String dataset,
  ) async {
    const query = r'''
      mutation Cancel($tree: Checksum!, $entry: String!, $filepath: String!, $dataset: String!) {
        cancelRestore(tree: $tree, entry: $entry, filepath: $filepath, dataset: $dataset)
      }
    ''';
    final mutationOptions = MutationOptions(
      document: gql(query),
      variables: <String, dynamic>{
        'tree': tree,
        'entry': entry,
        'filepath': filepath,
        'dataset': dataset,
      },
      fetchPolicy: FetchPolicy.noCache,
    );
    final QueryResult result = await client.mutate(mutationOptions);
    if (result.hasException) {
      throw err.ServerException(result.exception.toString());
    }
    return (result.data?['cancelRestore'] ?? false) as bool;
  }
}
