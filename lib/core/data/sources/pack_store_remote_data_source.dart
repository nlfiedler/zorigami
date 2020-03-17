//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:meta/meta.dart';
import 'package:graphql/client.dart';
import 'package:zorigami/core/data/models/pack_store_model.dart';
import 'package:zorigami/core/error/exceptions.dart';

abstract class PackStoreRemoteDataSource {
  Future<List<PackStoreModel>> getAllPackStores();
  Future<PackStoreModel> getPackStore(String key);
  Future<PackStoreModel> deletePackStore(String key);
  Future<PackStoreModel> definePackStore(
      String kind, Map<String, dynamic> options);
  Future<PackStoreModel> updatePackStore(
      String key, Map<String, dynamic> options);
}

class PackStoreRemoteDataSourceImpl extends PackStoreRemoteDataSource {
  final GraphQLClient client;

  PackStoreRemoteDataSourceImpl({@required this.client});

  @override
  Future<List<PackStoreModel>> getAllPackStores() async {
    final getAllStores = r'''
      query {
        stores {
          key
          label
          kind
          options
        }
      }
    ''';
    final queryOptions = QueryOptions(documentNode: gql(getAllStores));
    final QueryResult result = await client.query(queryOptions);
    if (result.hasException) {
      throw ServerException(result.exception.toString());
    }
    final List<dynamic> stores = result.data['stores'] as List<dynamic>;
    final List<PackStoreModel> results = List.from(
      stores.map<PackStoreModel>((e) {
        return PackStoreModel.fromJson(e);
      }),
    );
    return results;
  }

  @override
  Future<PackStoreModel> getPackStore(String key) async {
    final query = r'''
      query Fetch($identifier: String!) {
        store(key: $identifier) {
          key
          label
          kind
          options
        }
      }
    ''';
    final queryOptions = QueryOptions(
      documentNode: gql(query),
      variables: <String, dynamic>{
        'identifier': key,
      },
    );
    final QueryResult result = await client.query(queryOptions);
    if (result.hasException) {
      throw ServerException(result.exception.toString());
    }
    final Map<String, dynamic> object =
        result.data['store'] as Map<String, dynamic>;
    return object == null ? null : PackStoreModel.fromJson(object);
  }

  @override
  Future<PackStoreModel> deletePackStore(String key) async {
    final getStore = r'''
      mutation DeleteStore($key: String!) {
        deleteStore(key: $key) {
          key
          label
          kind
          options
        }
      }
    ''';
    final mutationOptions = MutationOptions(
      documentNode: gql(getStore),
      variables: <String, dynamic>{
        'identifier': key,
      },
    );
    final QueryResult result = await client.mutate(mutationOptions);
    if (result.hasException) {
      throw ServerException(result.exception.toString());
    }
    final store = result.data['store'] as Map<String, dynamic>;
    return PackStoreModel.fromJson(store);
  }

  @override
  Future<PackStoreModel> definePackStore(
      String kind, Map<String, dynamic> options) async {
    final defineStore = r'''
      mutation DefineStore($typeName: String!, $options: String!) {
        defineStore(typeName: $typeName, options: $options) {
          key
          label
          kind
          options
        }
      }
    ''';
    final encodedOptions = encodeOptions(options);
    final mutationOptions = MutationOptions(
      documentNode: gql(defineStore),
      variables: <String, dynamic>{
        'typeName': kind,
        'options': encodedOptions,
      },
    );
    final QueryResult result = await client.mutate(mutationOptions);
    if (result.hasException) {
      throw ServerException(result.exception.toString());
    }
    final store = result.data['store'] as Map<String, dynamic>;
    return PackStoreModel.fromJson(store);
  }

  @override
  Future<PackStoreModel> updatePackStore(
      String key, Map<String, dynamic> options) async {
    final updateStore = r'''
      mutation UpdateStore($key: String!, $options: String!) {
        updateStore(key: $key, options: $options) {
          key
          label
          kind
          options
        }
      }
    ''';
    final encodedOptions = encodeOptions(options);
    final mutationOptions = MutationOptions(
      documentNode: gql(updateStore),
      variables: <String, dynamic>{
        'key': key,
        'options': encodedOptions,
      },
    );
    final QueryResult result = await client.mutate(mutationOptions);
    if (result.hasException) {
      throw ServerException(result.exception.toString());
    }
    final store = result.data['store'] as Map<String, dynamic>;
    return PackStoreModel.fromJson(store);
  }
}
