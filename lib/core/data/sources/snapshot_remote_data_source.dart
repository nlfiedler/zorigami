//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:graphql/client.dart';
import 'package:meta/meta.dart';
import 'package:zorigami/core/data/models/snapshot_model.dart';
import 'package:zorigami/core/error/exceptions.dart';

abstract class SnapshotRemoteDataSource {
  Future<SnapshotModel> getSnapshot(String checksum);
  Future<String> restoreFile(String checksum, String filepath, String dataset);
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
  Future<String> restoreFile(
      String checksum, String filepath, String dataset) async {
    final query = r'''
      mutation Restore($digest: Checksum!, $filepath: String!, $dataset: String!) {
        restoreFile(digest: $digest, filepath: $filepath, dataset: $dataset)
      }
    ''';
    final queryOptions = QueryOptions(
      documentNode: gql(query),
      variables: <String, dynamic>{
        'digest': checksum,
        'filepath': filepath,
        'dataset': dataset,
      },
      fetchPolicy: FetchPolicy.noCache,
    );
    final QueryResult result = await client.query(queryOptions);
    if (result.hasException) {
      throw ServerException(result.exception.toString());
    }
    return result.data['restoreFile'];
  }
}
