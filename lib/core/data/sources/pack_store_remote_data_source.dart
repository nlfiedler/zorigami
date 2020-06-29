//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:meta/meta.dart';
import 'package:graphql/client.dart';
import 'package:zorigami/core/data/models/pack_store_model.dart';
import 'package:zorigami/core/domain/entities/pack_store.dart';
import 'package:zorigami/core/error/exceptions.dart';

abstract class PackStoreRemoteDataSource {
  Future<List<PackStoreModel>> getAllPackStores();
  Future<String> deletePackStore(PackStore input);
  Future<PackStoreModel> definePackStore(PackStore input);
  Future<PackStoreModel> updatePackStore(PackStore input);
}

class PackStoreRemoteDataSourceImpl extends PackStoreRemoteDataSource {
  final GraphQLClient client;

  PackStoreRemoteDataSourceImpl({@required this.client});

  @override
  Future<List<PackStoreModel>> getAllPackStores() async {
    final getAllStores = r'''
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
      documentNode: gql(getAllStores),
      fetchPolicy: FetchPolicy.noCache,
    );
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
  Future<String> deletePackStore(PackStore input) async {
    final getStore = r'''
      mutation DeleteStore($identifier: String!) {
        deleteStore(id: $identifier)
      }
    ''';
    final mutationOptions = MutationOptions(
      documentNode: gql(getStore),
      variables: <String, dynamic>{
        'identifier': input.key,
      },
    );
    final QueryResult result = await client.mutate(mutationOptions);
    if (result.hasException) {
      throw ServerException(result.exception.toString());
    }
    final identifier = result.data['deleteStore'] as String;
    return identifier;
  }

  @override
  Future<PackStoreModel> definePackStore(PackStore input) async {
    final defineStore = r'''
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
      documentNode: gql(defineStore),
      variables: <String, dynamic>{
        'input': encodedStore,
      },
    );
    final QueryResult result = await client.mutate(mutationOptions);
    if (result.hasException) {
      throw ServerException(result.exception.toString());
    }
    final store = result.data['defineStore'] as Map<String, dynamic>;
    return PackStoreModel.fromJson(store);
  }

  @override
  Future<PackStoreModel> updatePackStore(PackStore input) async {
    final updateStore = r'''
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
      documentNode: gql(updateStore),
      variables: <String, dynamic>{
        'input': encodedStore,
      },
    );
    final QueryResult result = await client.mutate(mutationOptions);
    if (result.hasException) {
      throw ServerException(result.exception.toString());
    }
    final store = result.data['updateStore'] as Map<String, dynamic>;
    return PackStoreModel.fromJson(store);
  }
}
