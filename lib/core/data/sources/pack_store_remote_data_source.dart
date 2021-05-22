//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:graphql/client.dart' as gql;
import 'package:gql/language.dart' as lang;
import 'package:gql/ast.dart' as ast;
import 'package:normalize/utils.dart';
import 'package:zorigami/core/data/models/pack_store_model.dart';
import 'package:zorigami/core/domain/entities/pack_store.dart';
import 'package:zorigami/core/error/exceptions.dart';

abstract class PackStoreRemoteDataSource {
  Future<List<PackStoreModel>> getAllPackStores();
  Future<String> testPackStore(PackStore input);
  Future<String> deletePackStore(PackStore input);
  Future<PackStoreModel?> definePackStore(PackStore input);
  Future<PackStoreModel?> updatePackStore(PackStore input);
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

class PackStoreRemoteDataSourceImpl extends PackStoreRemoteDataSource {
  final gql.GraphQLClient client;

  PackStoreRemoteDataSourceImpl({required this.client});

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
    final queryOptions = gql.QueryOptions(
      document: gqlNoTypename(getAllStores),
      fetchPolicy: gql.FetchPolicy.noCache,
    );
    final gql.QueryResult result = await client.query(queryOptions);
    if (result.hasException) {
      throw ServerException(result.exception.toString());
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
    final testStore = r'''
      mutation TestStore($input: StoreInput!) {
        testStore(input: $input)
      }
    ''';
    final storeModel = PackStoreModel.fromStore(input);
    final encodedStore = storeModel.toJson();
    final mutationOptions = gql.MutationOptions(
      document: gqlNoTypename(testStore),
      variables: <String, dynamic>{
        'input': encodedStore,
      },
    );
    final gql.QueryResult result = await client.mutate(mutationOptions);
    if (result.hasException) {
      throw ServerException(result.exception.toString());
    }
    return (result.data?['testStore'] ?? 'ng') as String;
  }

  @override
  Future<String> deletePackStore(PackStore input) async {
    final getStore = r'''
      mutation DeleteStore($id: String!) {
        deleteStore(id: $id)
      }
    ''';
    final mutationOptions = gql.MutationOptions(
      document: gqlNoTypename(getStore),
      variables: <String, dynamic>{
        'id': input.key,
      },
    );
    final gql.QueryResult result = await client.mutate(mutationOptions);
    if (result.hasException) {
      throw ServerException(result.exception.toString());
    }
    final identifier = (result.data?['deleteStore'] ?? 'ng') as String;
    return identifier;
  }

  @override
  Future<PackStoreModel?> definePackStore(PackStore input) async {
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
    final mutationOptions = gql.MutationOptions(
      document: gqlNoTypename(defineStore),
      variables: <String, dynamic>{
        'input': encodedStore,
      },
    );
    final gql.QueryResult result = await client.mutate(mutationOptions);
    if (result.hasException) {
      throw ServerException(result.exception.toString());
    }
    if (result.data?['defineStore'] == null) {
      return null;
    }
    final store = result.data?['defineStore'] as Map<String, dynamic>;
    return PackStoreModel.fromJson(store);
  }

  @override
  Future<PackStoreModel?> updatePackStore(PackStore input) async {
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
    final mutationOptions = gql.MutationOptions(
      document: gqlNoTypename(updateStore),
      variables: <String, dynamic>{
        'input': encodedStore,
      },
    );
    final gql.QueryResult result = await client.mutate(mutationOptions);
    if (result.hasException) {
      throw ServerException(result.exception.toString());
    }
    if (result.data?['updateStore'] == null) {
      return null;
    }
    final store = result.data?['updateStore'] as Map<String, dynamic>;
    return PackStoreModel.fromJson(store);
  }
}
