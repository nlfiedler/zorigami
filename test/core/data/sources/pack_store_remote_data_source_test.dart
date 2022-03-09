//
// Copyright (c) 2022 Nathan Fiedler
//
import 'dart:convert';
import 'package:graphql/client.dart' as gql;
import 'package:http/http.dart' as http;
import 'package:mocktail/mocktail.dart';
import 'package:zorigami/core/data/models/pack_store_model.dart';
import 'package:zorigami/core/data/sources/pack_store_remote_data_source.dart';
import 'package:zorigami/core/domain/entities/pack_store.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:zorigami/core/error/exceptions.dart';

class MockHttpClient extends Mock implements http.Client {}

void main() {
  late PackStoreRemoteDataSourceImpl dataSource;
  late MockHttpClient mockHttpClient;

  setUp(() {
    mockHttpClient = MockHttpClient();
    final link = gql.HttpLink(
      'http://example.com',
      httpClient: mockHttpClient,
    );
    final graphQLCient = gql.GraphQLClient(
      link: link,
      cache: gql.GraphQLCache(),
    );
    dataSource = PackStoreRemoteDataSourceImpl(client: graphQLCient);
  });

  setUpAll(() {
    // mocktail needs a fallback for any() that involves custom types
    http.BaseRequest dummyRequest = http.Request(
      'GET',
      Uri(scheme: 'http', host: 'example.com', path: '/'),
    );
    registerFallbackValue(dummyRequest);
  });

  final tPackStoreModel = PackStoreModel(
    key: 'abc123',
    label: 'lstore',
    kind: StoreKind.local,
    options: {'basepath': '/home/user'},
  );

  void setUpMockDeleteGraphQLResponse() {
    final response = {
      'data': {'deleteStore': 'abc123'}
    };
    // graphql client uses the 'send' method
    when(() => mockHttpClient.send(any())).thenAnswer((_) async {
      final bytes = utf8.encode(json.encode(response));
      final stream = http.ByteStream.fromBytes(bytes);
      return http.StreamedResponse(stream, 200);
    });
  }

  void setUpMockTestGraphQLResponse() {
    final response = {
      'data': {'testStore': 'ok'}
    };
    // graphql client uses the 'send' method
    when(() => mockHttpClient.send(any())).thenAnswer((_) async {
      final bytes = utf8.encode(json.encode(response));
      final stream = http.ByteStream.fromBytes(bytes);
      return http.StreamedResponse(stream, 200);
    });
  }

  void setUpMockHttpClientGraphQLResponse(
    String operation,
    List<dynamic> options,
  ) {
    final response = {
      'data': {
        '__typename': 'Store',
        operation: {
          '__typename': 'Store',
          'id': 'abc123',
          'label': 'lstore',
          'storeType': 'local',
          'properties': options,
        }
      }
    };
    // graphql client uses the 'send' method
    when(() => mockHttpClient.send(any())).thenAnswer((_) async {
      final bytes = utf8.encode(json.encode(response));
      final stream = http.ByteStream.fromBytes(bytes);
      return http.StreamedResponse(stream, 200);
    });
  }

  void setUpMockHttpClientGraphQLError() {
    when(() => mockHttpClient.send(any())).thenAnswer((_) async {
      final response = {
        'data': null,
        'errors': [
          {
            'message': 'some kind of error occurred',
            'locations': [
              {'line': 2, 'column': 3}
            ],
            'path': ['store']
          }
        ]
      };
      final bytes = utf8.encode(json.encode(response));
      final stream = http.ByteStream.fromBytes(bytes);
      return http.StreamedResponse(stream, 200);
    });
  }

  void setUpMockHttpClientFailure403() {
    when(() => mockHttpClient.send(any())).thenAnswer((_) async {
      final bytes = <int>[];
      final stream = http.ByteStream.fromBytes(bytes);
      return http.StreamedResponse(stream, 403);
    });
  }

  group('getAllPackStores', () {
    test(
      'should return zero pack stores',
      () async {
        // arrange
        final response = {
          'data': {'stores': []}
        };
        // graphql client uses the 'send' method
        when(() => mockHttpClient.send(any())).thenAnswer((_) async {
          final bytes = utf8.encode(json.encode(response));
          final stream = http.ByteStream.fromBytes(bytes);
          return http.StreamedResponse(stream, 200);
        });
        // act
        final result = await dataSource.getAllPackStores();
        // assert
        expect(result, isList);
        expect(result, hasLength(equals(0)));
      },
    );

    test(
      'should return one pack store',
      () async {
        // arrange
        final response = {
          'data': {
            'stores': [
              {
                'id': 'a1',
                'label': 's1',
                'storeType': 'minio',
                'properties': []
              },
            ]
          }
        };
        // graphql client uses the 'send' method
        when(() => mockHttpClient.send(any())).thenAnswer((_) async {
          final bytes = utf8.encode(json.encode(response));
          final stream = http.ByteStream.fromBytes(bytes);
          return http.StreamedResponse(stream, 200);
        });
        // act
        final result = await dataSource.getAllPackStores();
        // assert
        expect(result, isList);
        expect(result, hasLength(equals(1)));
        final store = PackStoreModel(
          key: 'a1',
          label: 's1',
          kind: StoreKind.minio,
          options: {},
        );
        expect(result, contains(store));
      },
    );

    test(
      'should return all pack stores',
      () async {
        // arrange
        final response = {
          'data': {
            'stores': [
              {
                'id': 'a1',
                'label': 's1',
                'storeType': 'minio',
                'properties': []
              },
              {
                'id': 'b2',
                'label': 's2',
                'storeType': 'local',
                'properties': []
              },
              {
                'id': 'c3',
                'label': 's3',
                'storeType': 'sftp',
                'properties': []
              },
            ]
          }
        };
        // graphql client uses the 'send' method
        when(() => mockHttpClient.send(any())).thenAnswer((_) async {
          final bytes = utf8.encode(json.encode(response));
          final stream = http.ByteStream.fromBytes(bytes);
          return http.StreamedResponse(stream, 200);
        });
        // act
        final result = await dataSource.getAllPackStores();
        // assert
        expect(result, isList);
        expect(result, hasLength(equals(3)));
        final store = PackStoreModel(
          key: 'a1',
          label: 's1',
          kind: StoreKind.minio,
          options: {},
        );
        expect(result, contains(store));
      },
    );

    test(
      'should report failure when response unsuccessful',
      () async {
        // arrange
        setUpMockHttpClientFailure403();
        // act, assert
        try {
          await dataSource.getAllPackStores();
          fail('should have raised an error');
        } catch (e) {
          expect(e, isA<ServerException>());
        }
      },
    );

    test(
      'should raise error when GraphQL server returns an error',
      () async {
        // arrange
        setUpMockHttpClientGraphQLError();
        // act, assert
        try {
          await dataSource.getAllPackStores();
          fail('should have raised an error');
        } catch (e) {
          expect(e, isA<ServerException>());
        }
      },
    );
  });

  group('testPackStore', () {
    test(
      'should test a specific pack store',
      () async {
        // arrange
        setUpMockTestGraphQLResponse();
        // act
        final result = await dataSource.testPackStore(tPackStoreModel);
        // assert
        expect(result, equals('ok'));
      },
    );

    test(
      'should report failure when response unsuccessful',
      () async {
        // arrange
        setUpMockHttpClientFailure403();
        // act, assert
        try {
          await dataSource.testPackStore(tPackStoreModel);
          fail('should have raised an error');
        } catch (e) {
          expect(e, isA<ServerException>());
        }
      },
    );

    test(
      'should raise error when GraphQL server returns an error',
      () async {
        // arrange
        setUpMockHttpClientGraphQLError();
        // act, assert
        try {
          await dataSource.testPackStore(tPackStoreModel);
          fail('should have raised an error');
        } catch (e) {
          expect(e, isA<ServerException>());
        }
      },
    );
  });

  group('deletePackStore', () {
    test(
      'should delete a specific pack store',
      () async {
        // arrange
        setUpMockDeleteGraphQLResponse();
        // act
        await dataSource.deletePackStore(tPackStoreModel);
        // assert: nothing to assert
      },
    );

    test(
      'should report failure when response unsuccessful',
      () async {
        // arrange
        setUpMockHttpClientFailure403();
        // act, assert
        try {
          await dataSource.deletePackStore(tPackStoreModel);
          fail('should have raised an error');
        } catch (e) {
          expect(e, isA<ServerException>());
        }
      },
    );

    test(
      'should raise error when GraphQL server returns an error',
      () async {
        // arrange
        setUpMockHttpClientGraphQLError();
        // act, assert
        try {
          await dataSource.deletePackStore(tPackStoreModel);
          fail('should have raised an error');
        } catch (e) {
          expect(e, isA<ServerException>());
        }
      },
    );
  });

  group('definePackStore', () {
    test(
      'should define a new pack store',
      () async {
        // arrange
        final encodedOptions = encodeQLOptions(tPackStoreModel.options);
        setUpMockHttpClientGraphQLResponse('defineStore', encodedOptions);
        // act
        final result = await dataSource.definePackStore(tPackStoreModel);
        // assert
        expect(result, equals(tPackStoreModel));
      },
    );

    test(
      'should report failure when response unsuccessful',
      () async {
        // arrange
        setUpMockHttpClientFailure403();
        // act, assert
        try {
          await dataSource.definePackStore(tPackStoreModel);
          fail('should have raised an error');
        } catch (e) {
          expect(e, isA<ServerException>());
        }
      },
    );

    test(
      'should raise error when GraphQL server returns an error',
      () async {
        // arrange
        setUpMockHttpClientGraphQLError();
        // act, assert
        try {
          await dataSource.definePackStore(tPackStoreModel);
          fail('should have raised an error');
        } catch (e) {
          expect(e, isA<ServerException>());
        }
      },
    );
  });

  group('updatePackStore', () {
    test(
      'should update an existing pack store',
      () async {
        // arrange
        final encodedOptions = encodeQLOptions(tPackStoreModel.options);
        setUpMockHttpClientGraphQLResponse('updateStore', encodedOptions);
        // act
        final result = await dataSource.updatePackStore(tPackStoreModel);
        // assert
        expect(result, equals(tPackStoreModel));
      },
    );

    test(
      'should report failure when response unsuccessful',
      () async {
        // arrange
        setUpMockHttpClientFailure403();
        // act, assert
        try {
          await dataSource.updatePackStore(tPackStoreModel);
          fail('should have raised an error');
        } catch (e) {
          expect(e, isA<ServerException>());
        }
      },
    );

    test(
      'should raise error when GraphQL server returns an error',
      () async {
        // arrange
        setUpMockHttpClientGraphQLError();
        // act, assert
        try {
          await dataSource.updatePackStore(tPackStoreModel);
          fail('should have raised an error');
        } catch (e) {
          expect(e, isA<ServerException>());
        }
      },
    );
  });
}
