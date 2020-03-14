//
// Copyright (c) 2020 Nathan Fiedler
//
import 'dart:convert';
import 'package:graphql/client.dart';
import 'package:http/http.dart' as http;
import 'package:mockito/mockito.dart';
import 'package:zorigami/core/data/models/pack_store_model.dart';
import 'package:zorigami/core/data/sources/pack_store_remote_data_source.dart';
import 'package:zorigami/core/domain/entities/pack_store.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:zorigami/core/error/exceptions.dart';

class MockHttpClient extends Mock implements http.Client {}

void main() {
  PackStoreRemoteDataSourceImpl dataSource;
  MockHttpClient mockHttpClient;

  setUp(() {
    mockHttpClient = MockHttpClient();
    final link = HttpLink(
      uri: 'http://example.com',
      httpClient: mockHttpClient,
    );
    final graphQLCient = GraphQLClient(
      link: link,
      cache: InMemoryCache(),
    );
    dataSource = PackStoreRemoteDataSourceImpl(client: graphQLCient);
  });

  void setUpMockHttpClientGraphQLError() {
    when(mockHttpClient.send(any)).thenAnswer((_) async {
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
    when(mockHttpClient.send(any)).thenAnswer((_) async {
      final bytes = List<int>();
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
        when(mockHttpClient.send(any)).thenAnswer((_) async {
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
              {'key': 'a1', 'label': 's1', 'kind': 'minio', 'options': ''},
            ]
          }
        };
        // graphql client uses the 'send' method
        when(mockHttpClient.send(any)).thenAnswer((_) async {
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
              {'key': 'a1', 'label': 's1', 'kind': 'minio', 'options': ''},
              {'key': 'b2', 'label': 's2', 'kind': 'local', 'options': ''},
              {'key': 'c3', 'label': 's3', 'kind': 'sftp', 'options': ''},
            ]
          }
        };
        // graphql client uses the 'send' method
        when(mockHttpClient.send(any)).thenAnswer((_) async {
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
          await dataSource.getPackStore('foobar');
          fail('should have raised an error');
        } catch (e) {
          expect(e, isA<ServerException>());
        }
        //
        // wanted to do this, but it failed with an "asynchronous gap" error,
        // tried numerous alternatives to no avail
        //
        // final future = dataSource.getPackStore('foobar');
        // expect(future, completion(throwsA(ServerException)));
      },
    );
  });

  group('getPackStore', () {
    test(
      'should return a specific pack store',
      () async {
        // arrange
        final response = {
          'data': {
            'store': {
              'key': 'abc123',
              'label': 'store1',
              'kind': 'minio',
              'options': ''
            }
          }
        };
        // graphql client uses the 'send' method
        when(mockHttpClient.send(any)).thenAnswer((_) async {
          final bytes = utf8.encode(json.encode(response));
          final stream = http.ByteStream.fromBytes(bytes);
          return http.StreamedResponse(stream, 200);
        });
        // act
        final result = await dataSource.getPackStore('abc123');
        // assert
        final store = PackStoreModel(
          key: 'abc123',
          label: 'store1',
          kind: StoreKind.minio,
          options: {},
        );
        expect(result, equals(store));
      },
    );

    test(
      'should report failure when response unsuccessful',
      () async {
        // arrange
        setUpMockHttpClientFailure403();
        // act, assert
        try {
          await dataSource.getPackStore('foobar');
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
          await dataSource.getPackStore('foobar');
          fail('should have raised an error');
        } catch (e) {
          expect(e, isA<ServerException>());
        }
      },
    );
  });

  group('getPackStore', () {
    test(
      'should delete a specific pack store',
      () async {
        // arrange
        final response = {
          'data': {
            'store': {
              'key': 'abc123',
              'label': 'store1',
              'kind': 'minio',
              'options': ''
            }
          }
        };
        // graphql client uses the 'send' method
        when(mockHttpClient.send(any)).thenAnswer((_) async {
          final bytes = utf8.encode(json.encode(response));
          final stream = http.ByteStream.fromBytes(bytes);
          return http.StreamedResponse(stream, 200);
        });
        // act
        final result = await dataSource.deletePackStore('abc123');
        // assert
        final store = PackStoreModel(
          key: 'abc123',
          label: 'store1',
          kind: StoreKind.minio,
          options: {},
        );
        expect(result, equals(store));
      },
    );

    test(
      'should report failure when response unsuccessful',
      () async {
        // arrange
        setUpMockHttpClientFailure403();
        // act, assert
        try {
          await dataSource.deletePackStore('foobar');
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
          await dataSource.deletePackStore('foobar');
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
        final Map<String, dynamic> tOptions = {'path': '/home/user'};
        final encodedOptions = encodeOptions(tOptions);
        final response = {
          'data': {
            'store': {
              'key': 'abc123',
              'label': 'lstore',
              'kind': 'local',
              'options': encodedOptions
            }
          }
        };
        // graphql client uses the 'send' method
        when(mockHttpClient.send(any)).thenAnswer((_) async {
          final bytes = utf8.encode(json.encode(response));
          final stream = http.ByteStream.fromBytes(bytes);
          return http.StreamedResponse(stream, 200);
        });
        // act
        final result = await dataSource.definePackStore('local', tOptions);
        // assert
        final store = PackStoreModel(
          key: 'abc123',
          label: 'lstore1',
          kind: StoreKind.local,
          options: tOptions,
        );
        expect(result, equals(store));
      },
    );

    test(
      'should report failure when response unsuccessful',
      () async {
        // arrange
        setUpMockHttpClientFailure403();
        // act, assert
        try {
          await dataSource.definePackStore('foobar', {});
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
          await dataSource.definePackStore('foobar', {});
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
        final Map<String, dynamic> tOptions = {'path': '/home/user'};
        final encodedOptions = encodeOptions(tOptions);
        final response = {
          'data': {
            'store': {
              'key': 'abc123',
              'label': 'lstore',
              'kind': 'local',
              'options': encodedOptions
            }
          }
        };
        // graphql client uses the 'send' method
        when(mockHttpClient.send(any)).thenAnswer((_) async {
          final bytes = utf8.encode(json.encode(response));
          final stream = http.ByteStream.fromBytes(bytes);
          return http.StreamedResponse(stream, 200);
        });
        // act
        final result = await dataSource.updatePackStore('abc123', tOptions);
        // assert
        final store = PackStoreModel(
          key: 'abc123',
          label: 'lstore1',
          kind: StoreKind.local,
          options: tOptions,
        );
        expect(result, equals(store));
      },
    );

    test(
      'should report failure when response unsuccessful',
      () async {
        // arrange
        setUpMockHttpClientFailure403();
        // act, assert
        try {
          await dataSource.updatePackStore('foobar', {});
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
          await dataSource.updatePackStore('foobar', {});
          fail('should have raised an error');
        } catch (e) {
          expect(e, isA<ServerException>());
        }
      },
    );
  });
}