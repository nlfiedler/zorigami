//
// Copyright (c) 2022 Nathan Fiedler
//
import 'package:graphql/client.dart';
import 'package:zorigami/core/data/models/pack_store_model.dart';
import 'package:zorigami/core/domain/entities/pack_store.dart';
import 'package:zorigami/core/error/exceptions.dart' as err;

abstract class PackStoreRemoteDataSource {
  Future<List<PackStoreModel>> getAllPackStores();
  Future<String> testPackStore(PackStore input);
  Future<String> deletePackStore(PackStore input);
  Future<PackStoreModel?> definePackStore(PackStore input);
  Future<PackStoreModel?> updatePackStore(PackStore input);
}

class PackStoreRemoteDataSourceImpl extends PackStoreRemoteDataSource {
  final GraphQLClient client;

  PackStoreRemoteDataSourceImpl({required this.client});

  @override
  Future<List<PackStoreModel>> getAllPackStores() async {
    const getAllStores = r'''
      query {
        stores {
          id
          storeType
          label
          properties {
            name
            value
          }
        }
      }
    ''';
    final queryOptions = QueryOptions(
      document: gql(getAllStores),
      fetchPolicy: FetchPolicy.noCache,
    );
    final QueryResult result = await client.query(queryOptions);
    if (result.hasException) {
      throw err.ServerException(result.exception.toString());
    }
    final List<dynamic> stores =
        (result.data?['stores'] ?? []) as List<dynamic>;
    final List<PackStoreModel> results = List.from(
      stores.map<PackStoreModel>((e) {
        return PackStoreModel.fromJson(e);
      }),
    );
    return results;
  }

  @override
  Future<String> testPackStore(PackStore input) async {
    const testStore = r'''
      mutation TestStore($input: StoreInput!) {
        testStore(input: $input)
      }
    ''';
    final storeModel = PackStoreModel.fromStore(input);
    final encodedStore = storeModel.toJson();
    final mutationOptions = MutationOptions(
      document: gql(testStore),
      variables: <String, dynamic>{
        'input': encodedStore,
      },
    );
    final QueryResult result = await client.mutate(mutationOptions);
    if (result.hasException) {
      throw err.ServerException(result.exception.toString());
    }
    return (result.data?['testStore'] ?? 'ng') as String;
  }

  @override
  Future<String> deletePackStore(PackStore input) async {
    const getStore = r'''
      mutation DeleteStore($id: String!) {
        deleteStore(id: $id)
      }
    ''';
    final mutationOptions = MutationOptions(
      document: gql(getStore),
      variables: <String, dynamic>{
        'id': input.key,
      },
    );
    final QueryResult result = await client.mutate(mutationOptions);
    if (result.hasException) {
      throw err.ServerException(result.exception.toString());
    }
    final identifier = (result.data?['deleteStore'] ?? 'ng') as String;
    return identifier;
  }

  @override
  Future<PackStoreModel?> definePackStore(PackStore input) async {
    const defineStore = r'''
      mutation DefineStore($input: StoreInput!) {
        defineStore(input: $input) {
          id
          storeType
          label
          properties {
            name
            value
          }
        }
      }
    ''';
    final storeModel = PackStoreModel.fromStore(input);
    final encodedStore = storeModel.toJson();
    final mutationOptions = MutationOptions(
      document: gql(defineStore),
      variables: <String, dynamic>{
        'input': encodedStore,
      },
    );
    final QueryResult result = await client.mutate(mutationOptions);
    if (result.hasException) {
      throw err.ServerException(result.exception.toString());
    }
    if (result.data?['defineStore'] == null) {
      return null;
    }
    final store = result.data?['defineStore'] as Map<String, dynamic>;
    return PackStoreModel.fromJson(store);
  }

  @override
  Future<PackStoreModel?> updatePackStore(PackStore input) async {
    const updateStore = r'''
      mutation UpdateStore($input: StoreInput!) {
        updateStore(input: $input) {
          id
          storeType
          label
          properties {
            name
            value
          }
        }
      }
    ''';
    final storeModel = PackStoreModel.fromStore(input);
    final encodedStore = storeModel.toJson();
    final mutationOptions = MutationOptions(
      document: gql(updateStore),
      variables: <String, dynamic>{
        'input': encodedStore,
      },
    );
    final QueryResult result = await client.mutate(mutationOptions);
    if (result.hasException) {
      throw err.ServerException(result.exception.toString());
    }
    if (result.data?['updateStore'] == null) {
      return null;
    }
    final store = result.data?['updateStore'] as Map<String, dynamic>;
    return PackStoreModel.fromJson(store);
  }
}
