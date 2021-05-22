//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:graphql/client.dart' as gql;
import 'package:gql/language.dart' as lang;
import 'package:gql/ast.dart' as ast;
import 'package:normalize/utils.dart';
import 'package:zorigami/core/data/models/data_set_model.dart';
import 'package:zorigami/core/domain/entities/data_set.dart';
import 'package:zorigami/core/error/exceptions.dart';

abstract class DataSetRemoteDataSource {
  Future<List<DataSetModel>> getAllDataSets();
  Future<String> deleteDataSet(DataSet input);
  Future<DataSetModel?> defineDataSet(DataSet input);
  Future<DataSetModel?> updateDataSet(DataSet input);
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
  final gql.GraphQLClient client;

  DataSetRemoteDataSourceImpl({required this.client});

  @override
  Future<List<DataSetModel>> getAllDataSets() async {
    final getAllDatasets = '''
      query {
        datasets {
          $dataSetFields
        }
      }
    ''';
    final queryOptions = gql.QueryOptions(
      document: gqlNoTypename(getAllDatasets),
      fetchPolicy: gql.FetchPolicy.noCache,
    );
    final gql.QueryResult result = await client.query(queryOptions);
    if (result.hasException) {
      throw ServerException(result.exception.toString());
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
    final deleteDataSet = r'''
      mutation DeleteDataset($id: String!) {
        deleteDataset(id: $id)
      }
    ''';
    final mutationOptions = gql.MutationOptions(
      document: gqlNoTypename(deleteDataSet),
      variables: <String, dynamic>{
        'id': input.key,
      },
    );
    final gql.QueryResult result = await client.mutate(mutationOptions);
    if (result.hasException) {
      throw ServerException(result.exception.toString());
    }
    return (result.data?['deleteDataset'] ?? 'null') as String;
  }

  @override
  Future<DataSetModel?> defineDataSet(DataSet input) async {
    final defineStore = '''
      mutation DefineDataset(\$input: DatasetInput!) {
        defineDataset(input: \$input) {
          $dataSetFields
        }
      }
    ''';
    final encoded = DataSetModel.from(input).toJson(input: true);
    final mutationOptions = gql.MutationOptions(
      document: gqlNoTypename(defineStore),
      variables: <String, dynamic>{
        'input': encoded,
      },
    );
    final gql.QueryResult result = await client.mutate(mutationOptions);
    if (result.hasException) {
      throw ServerException(result.exception.toString());
    }
    if (result.data?['defineDataset'] == null) {
      return null;
    }
    final dataset = result.data?['defineDataset'] as Map<String, dynamic>;
    return DataSetModel.fromJson(dataset);
  }

  @override
  Future<DataSetModel?> updateDataSet(DataSet input) async {
    final updateStore = '''
      mutation UpdateDataset(\$input: DatasetInput!) {
        updateDataset(input: \$input) {
          $dataSetFields
        }
      }
    ''';
    final encoded = DataSetModel.from(input).toJson(input: true);
    final mutationOptions = gql.MutationOptions(
      document: gqlNoTypename(updateStore),
      variables: <String, dynamic>{
        'input': encoded,
      },
    );
    final gql.QueryResult result = await client.mutate(mutationOptions);
    if (result.hasException) {
      throw ServerException(result.exception.toString());
    }
    if (result.data?['updateDataset'] == null) {
      return null;
    }
    final dataset = result.data?['updateDataset'] as Map<String, dynamic>;
    return DataSetModel.fromJson(dataset);
  }
}
