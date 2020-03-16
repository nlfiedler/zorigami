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
  Future<DataSetModel> getDataSet(String key);
  Future<DataSetModel> deleteDataSet(String key);
  Future<DataSetModel> defineDataSet(DataSet input);
  Future<DataSetModel> updateDataSet(DataSet input);
}

const String dataSetFields = '''
  key
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
          ${dataSetFields}
        }
      }
    ''';
    final queryOptions = QueryOptions(documentNode: gql(getAllDatasets));
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
  Future<DataSetModel> getDataSet(String key) async {
    final getDataset = '''
      query FetchDataset(\$identifier: String!) {
        dataset(key: \$identifier) {
          ${dataSetFields}
        }
      }
    ''';
    final queryOptions = QueryOptions(
      documentNode: gql(getDataset),
      variables: <String, dynamic>{
        'identifier': key,
      },
    );
    final QueryResult result = await client.query(queryOptions);
    if (result.hasException) {
      throw ServerException(result.exception.toString());
    }
    final Map<String, dynamic> dataset =
        result.data['dataset'] as Map<String, dynamic>;
    return DataSetModel.fromJson(dataset);
  }

  @override
  Future<DataSetModel> deleteDataSet(String key) async {
    final deleteDataSet = '''
      mutation DeleteDataset(\$key: String!) {
        deleteDataset(key: \$key) {
          ${dataSetFields}
        }
      }
    ''';
    final mutationOptions = MutationOptions(
      documentNode: gql(deleteDataSet),
      variables: <String, dynamic>{
        'identifier': key,
      },
    );
    final QueryResult result = await client.mutate(mutationOptions);
    if (result.hasException) {
      throw ServerException(result.exception.toString());
    }
    final dataset = result.data['dataset'] as Map<String, dynamic>;
    return DataSetModel.fromJson(dataset);
  }

  @override
  Future<DataSetModel> defineDataSet(DataSet input) async {
    final defineStore = '''
      mutation DefineDataset(\$dataset: InputDataset!) {
        defineDataset(dataset: \$dataset) {
          ${dataSetFields}
        }
      }
    ''';
    final encoded = DataSetModel.from(input).toJson();
    final mutationOptions = MutationOptions(
      documentNode: gql(defineStore),
      variables: <String, dynamic>{
        'dataset': encoded,
      },
    );
    final QueryResult result = await client.mutate(mutationOptions);
    if (result.hasException) {
      throw ServerException(result.exception.toString());
    }
    final dataset = result.data['dataset'] as Map<String, dynamic>;
    return DataSetModel.fromJson(dataset);
  }

  @override
  Future<DataSetModel> updateDataSet(DataSet input) async {
    final updateStore = '''
      mutation UpdateDataset(\$dataset: InputDataset!) {
        updateDataset(dataset: \$dataset) {
          ${dataSetFields}
        }
      }
    ''';
    final encoded = DataSetModel.from(input).toJson();
    final mutationOptions = MutationOptions(
      documentNode: gql(updateStore),
      variables: <String, dynamic>{
        'dataset': encoded,
      },
    );
    final QueryResult result = await client.mutate(mutationOptions);
    if (result.hasException) {
      throw ServerException(result.exception.toString());
    }
    final dataset = result.data['dataset'] as Map<String, dynamic>;
    return DataSetModel.fromJson(dataset);
  }
}