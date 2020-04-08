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
  Future<DataSetModel> deleteDataSet(DataSet input);
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
          ${dataSetFields}
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
  Future<DataSetModel> getDataSet(String key) async {
    final query = '''
      query FetchDataset(\$identifier: String!) {
        dataset(key: \$identifier) {
          ${dataSetFields}
        }
      }
    ''';
    final queryOptions = QueryOptions(
      documentNode: gql(query),
      variables: <String, dynamic>{
        'identifier': key,
      },
      fetchPolicy: FetchPolicy.noCache,
    );
    final QueryResult result = await client.query(queryOptions);
    if (result.hasException) {
      throw ServerException(result.exception.toString());
    }
    final Map<String, dynamic> object =
        result.data['dataset'] as Map<String, dynamic>;
    return object == null ? null : DataSetModel.fromJson(object);
  }

  @override
  Future<DataSetModel> deleteDataSet(DataSet input) async {
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
        'identifier': input.key,
      },
    );
    final QueryResult result = await client.mutate(mutationOptions);
    if (result.hasException) {
      throw ServerException(result.exception.toString());
    }
    final dataset = result.data['deleteDataset'] as Map<String, dynamic>;
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
    final dataset = result.data['defineDataset'] as Map<String, dynamic>;
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
    final dataset = result.data['updateDataset'] as Map<String, dynamic>;
    return DataSetModel.fromJson(dataset);
  }
}
