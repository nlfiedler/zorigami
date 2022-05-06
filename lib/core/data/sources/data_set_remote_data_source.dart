//
// Copyright (c) 2022 Nathan Fiedler
//
import 'package:graphql/client.dart';
import 'package:zorigami/core/data/models/data_set_model.dart';
import 'package:zorigami/core/domain/entities/data_set.dart';
import 'package:zorigami/core/error/exceptions.dart' as err;

abstract class DataSetRemoteDataSource {
  Future<List<DataSetModel>> getAllDataSets();
  Future<String> deleteDataSet(DataSet input);
  Future<DataSetModel?> defineDataSet(DataSet input);
  Future<DataSetModel?> updateDataSet(DataSet input);
  Future<bool> startBackup(DataSet input);
  Future<bool> stopBackup(DataSet input);
}

const String dataSetFields = '''
  id
  computerId
  basepath
  schedules {
    frequency
    timeRange {
      startTime
      stopTime
    }
    weekOfMonth
    dayOfWeek
    dayOfMonth
  }
  status
  errorMessage
  latestSnapshot {
    checksum
    parent
    startTime
    endTime
    fileCount
    tree
  }
  packSize
  stores
  excludes
''';

class DataSetRemoteDataSourceImpl extends DataSetRemoteDataSource {
  final GraphQLClient client;

  DataSetRemoteDataSourceImpl({required this.client});

  @override
  Future<List<DataSetModel>> getAllDataSets() async {
    const getAllDatasets = '''
      query {
        datasets {
          $dataSetFields
        }
      }
    ''';
    final queryOptions = QueryOptions(
      document: gql(getAllDatasets),
      fetchPolicy: FetchPolicy.noCache,
    );
    final QueryResult result = await client.query(queryOptions);
    if (result.hasException) {
      throw err.ServerException(result.exception.toString());
    }
    final List<dynamic> datasets =
        (result.data?['datasets'] ?? []) as List<dynamic>;
    final List<DataSetModel> results = List.from(
      datasets.map<DataSetModel>((e) {
        return DataSetModel.fromJson(e);
      }),
    );
    return results;
  }

  @override
  Future<String> deleteDataSet(DataSet input) async {
    const deleteDataSet = r'''
      mutation DeleteDataset($id: String!) {
        deleteDataset(id: $id)
      }
    ''';
    final mutationOptions = MutationOptions(
      document: gql(deleteDataSet),
      variables: <String, dynamic>{
        'id': input.key,
      },
    );
    final QueryResult result = await client.mutate(mutationOptions);
    if (result.hasException) {
      throw err.ServerException(result.exception.toString());
    }
    return (result.data?['deleteDataset'] ?? 'null') as String;
  }

  @override
  Future<DataSetModel?> defineDataSet(DataSet input) async {
    const defineStore = '''
      mutation DefineDataset(\$input: DatasetInput!) {
        defineDataset(input: \$input) {
          $dataSetFields
        }
      }
    ''';
    final encoded = DataSetModel.from(input).toJson(input: true);
    final mutationOptions = MutationOptions(
      document: gql(defineStore),
      variables: <String, dynamic>{
        'input': encoded,
      },
    );
    final QueryResult result = await client.mutate(mutationOptions);
    if (result.hasException) {
      throw err.ServerException(result.exception.toString());
    }
    if (result.data?['defineDataset'] == null) {
      return null;
    }
    final dataset = result.data?['defineDataset'] as Map<String, dynamic>;
    return DataSetModel.fromJson(dataset);
  }

  @override
  Future<DataSetModel?> updateDataSet(DataSet input) async {
    const updateStore = '''
      mutation UpdateDataset(\$input: DatasetInput!) {
        updateDataset(input: \$input) {
          $dataSetFields
        }
      }
    ''';
    final encoded = DataSetModel.from(input).toJson(input: true);
    final mutationOptions = MutationOptions(
      document: gql(updateStore),
      variables: <String, dynamic>{
        'input': encoded,
      },
    );
    final QueryResult result = await client.mutate(mutationOptions);
    if (result.hasException) {
      throw err.ServerException(result.exception.toString());
    }
    if (result.data?['updateDataset'] == null) {
      return null;
    }
    final dataset = result.data?['updateDataset'] as Map<String, dynamic>;
    return DataSetModel.fromJson(dataset);
  }

  @override
  Future<bool> startBackup(DataSet input) async {
    const updateStore = '''
      mutation StartBackup(\$id: String!) {
        startBackup(id: \$id)
      }
    ''';
    final mutationOptions = MutationOptions(
      document: gql(updateStore),
      variables: <String, dynamic>{
        'id': input.key,
      },
      fetchPolicy: FetchPolicy.noCache,
    );

    final QueryResult result = await client.mutate(mutationOptions);
    if (result.hasException) {
      throw err.ServerException(result.exception.toString());
    }
    return (result.data?['startBackup'] ?? false) as bool;
  }

  @override
  Future<bool> stopBackup(DataSet input) async {
    const updateStore = '''
      mutation StopBackup(\$id: String!) {
        stopBackup(id: \$id)
      }
    ''';
    final mutationOptions = MutationOptions(
      document: gql(updateStore),
      variables: <String, dynamic>{
        'id': input.key,
      },
      fetchPolicy: FetchPolicy.noCache,
    );
    final QueryResult result = await client.mutate(mutationOptions);
    if (result.hasException) {
      throw err.ServerException(result.exception.toString());
    }
    return (result.data?['stopBackup'] ?? false) as bool;
  }
}
