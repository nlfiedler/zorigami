//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:meta/meta.dart';
import 'package:graphql/client.dart';
import 'package:zorigami/core/data/models/data_set_model.dart';
import 'package:zorigami/core/domain/entities/data_set.dart';
import 'package:zorigami/core/error/exceptions.dart';

abstract class DataSetRemoteDataSource {
  Future<List<DataSetModel>> getAllDataSets();
  Future<String> deleteDataSet(DataSet input);
  Future<DataSetModel> defineDataSet(DataSet input);
  Future<DataSetModel> updateDataSet(DataSet input);
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
''';

class DataSetRemoteDataSourceImpl extends DataSetRemoteDataSource {
  final GraphQLClient client;

  DataSetRemoteDataSourceImpl({@required this.client});

  @override
  Future<List<DataSetModel>> getAllDataSets() async {
    final getAllDatasets = '''
      query {
        datasets {
          $dataSetFields
        }
      }
    ''';
    final queryOptions = QueryOptions(
      documentNode: gql(getAllDatasets),
      fetchPolicy: FetchPolicy.noCache,
    );
    final QueryResult result = await client.query(queryOptions);
    if (result.hasException) {
      throw ServerException(result.exception.toString());
    }
    final List<dynamic> datasets = result.data['datasets'] as List<dynamic>;
    final List<DataSetModel> results = List.from(
      datasets.map<DataSetModel>((e) {
        return DataSetModel.fromJson(e);
      }),
    );
    return results;
  }

  @override
  Future<String> deleteDataSet(DataSet input) async {
    final deleteDataSet = r'''
      mutation DeleteDataset($id: String!) {
        deleteDataset(id: $id)
      }
    ''';
    final mutationOptions = MutationOptions(
      documentNode: gql(deleteDataSet),
      variables: <String, dynamic>{
        'id': input.key,
      },
    );
    final QueryResult result = await client.mutate(mutationOptions);
    if (result.hasException) {
      throw ServerException(result.exception.toString());
    }
    final identifier = result.data['deleteDataset'] as String;
    return identifier;
  }

  @override
  Future<DataSetModel> defineDataSet(DataSet input) async {
    final defineStore = '''
      mutation DefineDataset(\$input: DatasetInput!) {
        defineDataset(input: \$input) {
          $dataSetFields
        }
      }
    ''';
    final encoded = DataSetModel.from(input).toJson(input: true);
    final mutationOptions = MutationOptions(
      documentNode: gql(defineStore),
      variables: <String, dynamic>{
        'input': encoded,
      },
    );
    final QueryResult result = await client.mutate(mutationOptions);
    if (result.hasException) {
      throw ServerException(result.exception.toString());
    }
    final dataset = result.data['defineDataset'] as Map<String, dynamic>;
    return DataSetModel.fromJson(dataset);
  }

  @override
  Future<DataSetModel> updateDataSet(DataSet input) async {
    final updateStore = '''
      mutation UpdateDataset(\$input: DatasetInput!) {
        updateDataset(input: \$input) {
          $dataSetFields
        }
      }
    ''';
    final encoded = DataSetModel.from(input).toJson(input: true);
    final mutationOptions = MutationOptions(
      documentNode: gql(updateStore),
      variables: <String, dynamic>{
        'input': encoded,
      },
    );
    final QueryResult result = await client.mutate(mutationOptions);
    if (result.hasException) {
      throw ServerException(result.exception.toString());
    }
    final dataset = result.data['updateDataset'] as Map<String, dynamic>;
    return DataSetModel.fromJson(dataset);
  }
}
