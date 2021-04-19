//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:graphql/client.dart';
import 'package:meta/meta.dart';
import 'package:zorigami/core/data/models/request_model.dart';
import 'package:zorigami/core/data/models/snapshot_model.dart';
import 'package:zorigami/core/error/exceptions.dart';

abstract class SnapshotRemoteDataSource {
  Future<SnapshotModel> getSnapshot(String checksum);
  Future<String> restoreDatabase(String storeId);
  Future<bool> restoreFiles(String checksum, String filepath, String dataset);
  Future<List<RequestModel>> getAllRestores();
  Future<bool> cancelRestore(String checksum, String filepath, String dataset);
}

class SnapshotRemoteDataSourceImpl extends SnapshotRemoteDataSource {
  final GraphQLClient client;

  SnapshotRemoteDataSourceImpl({@required this.client});

  @override
  Future<SnapshotModel> getSnapshot(String checksum) async {
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
    final queryOptions = QueryOptions(
      documentNode: gql(query),
      variables: <String, dynamic>{
        'checksum': checksum,
      },
      fetchPolicy: FetchPolicy.noCache,
    );
    final QueryResult result = await client.query(queryOptions);
    if (result.hasException) {
      throw ServerException(result.exception.toString());
    }
    final Map<String, dynamic> object =
        result.data['snapshot'] as Map<String, dynamic>;
    return object == null ? null : SnapshotModel.fromJson(object);
  }

  @override
  Future<String> restoreDatabase(String storeId) async {
    final query = r'''
      mutation Restore($storeId: String!) {
        restoreDatabase(storeId: $storeId)
      }
    ''';
    final mutationOptions = MutationOptions(
      documentNode: gql(query),
      variables: <String, dynamic>{
        'storeId': storeId,
      },
      fetchPolicy: FetchPolicy.noCache,
    );
    final QueryResult result = await client.mutate(mutationOptions);
    if (result.hasException) {
      throw ServerException(result.exception.toString());
    }
    return result.data['restoreDatabase'];
  }

  @override
  Future<bool> restoreFiles(
      String checksum, String filepath, String dataset) async {
    final query = r'''
      mutation Restore($digest: Checksum!, $filepath: String!, $dataset: String!) {
        restoreFiles(digest: $digest, filepath: $filepath, dataset: $dataset)
      }
    ''';
    final mutationOptions = MutationOptions(
      documentNode: gql(query),
      variables: <String, dynamic>{
        'digest': checksum,
        'filepath': filepath,
        'dataset': dataset,
      },
      fetchPolicy: FetchPolicy.noCache,
    );
    final QueryResult result = await client.mutate(mutationOptions);
    if (result.hasException) {
      throw ServerException(result.exception.toString());
    }
    return result.data['restoreFiles'];
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
    final queryOptions = QueryOptions(
      documentNode: gql(query),
      variables: <String, dynamic>{},
      fetchPolicy: FetchPolicy.noCache,
    );
    final QueryResult result = await client.query(queryOptions);
    if (result.hasException) {
      throw ServerException(result.exception.toString());
    }
    final List<dynamic> restores = result.data['restores'] as List<dynamic>;
    final List<RequestModel> results = List.from(
      restores.map<RequestModel>((e) {
        return RequestModel.fromJson(e);
      }),
    );
    return results;
  }

  @override
  Future<bool> cancelRestore(
      String checksum, String filepath, String dataset) async {
    final query = r'''
      mutation Cancel($digest: Checksum!, $filepath: String!, $dataset: String!) {
        cancelRestore(digest: $digest, filepath: $filepath, dataset: $dataset)
      }
    ''';
    final mutationOptions = MutationOptions(
      documentNode: gql(query),
      variables: <String, dynamic>{
        'digest': checksum,
        'filepath': filepath,
        'dataset': dataset,
      },
      fetchPolicy: FetchPolicy.noCache,
    );
    final QueryResult result = await client.mutate(mutationOptions);
    if (result.hasException) {
      throw ServerException(result.exception.toString());
    }
    return result.data['cancelRestore'];
  }
}
