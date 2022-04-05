//
// Copyright (c) 2022 Nathan Fiedler
//
import 'dart:convert';
import 'package:graphql/client.dart' as gql;
import 'package:http/http.dart' as http;
import 'package:mocktail/mocktail.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/data/models/request_model.dart';
import 'package:zorigami/core/data/models/snapshot_model.dart';
import 'package:zorigami/core/data/sources/snapshot_remote_data_source.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:zorigami/core/error/exceptions.dart';

class MockHttpClient extends Mock implements http.Client {}

void main() {
  late SnapshotRemoteDataSourceImpl dataSource;
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
    dataSource = SnapshotRemoteDataSourceImpl(client: graphQLCient);
  });

  setUpAll(() {
    // mocktail needs a fallback for any() that involves custom types
    http.BaseRequest dummyRequest = http.Request(
      'GET',
      Uri(scheme: 'http', host: 'example.com', path: '/'),
    );
    registerFallbackValue(dummyRequest);
  });

  final tSnapshotModel = SnapshotModel(
    checksum: 'sha1-a6c930a6f7f9aa4eb8ef67980e9e8e32cd02fa2b',
    parent: Some('sha1-823bb0cf28e72fef2651cf1bb06abfc5fdc51634'),
    startTime: DateTime.parse('2020-03-15T05:36:04.960782134+00:00'),
    endTime: Some(
      DateTime.parse('2020-03-15T05:36:05.141905479+00:00'),
    ),
    fileCount: 125331,
    tree: 'sha1-698058583b2283b8c02ea5e40272c8364a0d6e78',
  );

  void setUpMockHttpClientGraphQLResponse() {
    final response = {
      'data': {
        'snapshot': {
          'checksum': 'sha1-a6c930a6f7f9aa4eb8ef67980e9e8e32cd02fa2b',
          'parent': 'sha1-823bb0cf28e72fef2651cf1bb06abfc5fdc51634',
          'startTime': '2020-03-15T05:36:04.960782134+00:00',
          'endTime': '2020-03-15T05:36:05.141905479+00:00',
          'fileCount': '125331',
          'tree': 'sha1-698058583b2283b8c02ea5e40272c8364a0d6e78'
        },
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
            'path': ['snapshot']
          }
        ]
      };
      final bytes = utf8.encode(json.encode(response));
      final stream = http.ByteStream.fromBytes(bytes);
      return http.StreamedResponse(stream, 200);
    });
  }

  void setUpMockGraphQLNullResponse() {
    final response = {
      'data': {'snapshot': null}
    };
    // graphql client uses the 'send' method
    when(() => mockHttpClient.send(any())).thenAnswer((_) async {
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

  group('getSnapshot', () {
    test(
      'should return a specific data set',
      () async {
        // arrange
        setUpMockHttpClientGraphQLResponse();
        // act
        final result = await dataSource.getSnapshot('sha1-cafebabe');
        // assert
        expect(result, equals(tSnapshotModel));
      },
    );

    test(
      'should report failure when response unsuccessful',
      () async {
        // arrange
        setUpMockHttpClientFailure403();
        // act, assert
        try {
          await dataSource.getSnapshot('sha1-cafebabe');
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
          await dataSource.getSnapshot('sha1-cafebabe');
          fail('should have raised an error');
        } catch (e) {
          expect(e, isA<ServerException>());
        }
      },
    );

    test(
      'should return null when response is null',
      () async {
        // arrange
        setUpMockGraphQLNullResponse();
        // act
        final result = await dataSource.getSnapshot('sha1-cafebabe');
        // assert
        expect(result, isNull);
      },
    );
  });

  group('restoreDatabase', () {
    test('should restore database', () async {
      // arrange
      final response = {
        'data': {
          'restoreDatabase': 'ok',
        }
      };
      when(() => mockHttpClient.send(any())).thenAnswer((_) async {
        final bytes = utf8.encode(json.encode(response));
        final stream = http.ByteStream.fromBytes(bytes);
        return http.StreamedResponse(stream, 200);
      });
      // act
      final result = await dataSource.restoreDatabase('cafebabe');
      // assert
      expect(result, equals('ok'));
    });

    test(
      'should report failure when response unsuccessful',
      () async {
        // arrange
        setUpMockHttpClientFailure403();
        // act, assert
        try {
          await dataSource.restoreDatabase('cafebabe');
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
          await dataSource.restoreDatabase('cafebabe');
          fail('should have raised an error');
        } catch (e) {
          expect(e, isA<ServerException>());
        }
      },
    );

    test('should return null when response is null', () async {
      // arrange
      final response = {
        'data': {'restoreDatabase': null}
      };
      when(() => mockHttpClient.send(any())).thenAnswer((_) async {
        final bytes = utf8.encode(json.encode(response));
        final stream = http.ByteStream.fromBytes(bytes);
        return http.StreamedResponse(stream, 200);
      });
      // act
      final result = await dataSource.restoreDatabase('cafebabe');
      // assert
      expect(result, equals('ng'));
    });
  });

  group('restoreFiles', () {
    test('should enqueue restore request', () async {
      // arrange
      final response = {
        'data': {'restoreFiles': true}
      };
      when(() => mockHttpClient.send(any())).thenAnswer((_) async {
        final bytes = utf8.encode(json.encode(response));
        final stream = http.ByteStream.fromBytes(bytes);
        return http.StreamedResponse(stream, 200);
      });
      // act
      final result = await dataSource.restoreFiles(
        'sha1-cafebabe',
        'entry',
        'file',
        'homura',
      );
      // assert
      expect(result, equals(true));
    });

    test(
      'should report failure when response unsuccessful',
      () async {
        // arrange
        setUpMockHttpClientFailure403();
        // act, assert
        try {
          await dataSource.restoreFiles(
              'sha1-cafebabe', 'entry', 'file', 'homura');
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
          await dataSource.restoreFiles(
              'sha1-cafebabe', 'entry', 'file', 'homura');
          fail('should have raised an error');
        } catch (e) {
          expect(e, isA<ServerException>());
        }
      },
    );

    test('should return null when response is null', () async {
      // arrange
      final response = {
        'data': {'restoreFiles': null}
      };
      when(() => mockHttpClient.send(any())).thenAnswer((_) async {
        final bytes = utf8.encode(json.encode(response));
        final stream = http.ByteStream.fromBytes(bytes);
        return http.StreamedResponse(stream, 200);
      });
      // act
      final result = await dataSource.restoreFiles(
          'sha1-cafebabe', 'entry', 'file', 'homura');
      // assert
      expect(result, equals(false));
    });
  });

  group('getAllRestores', () {
    test(
      'should return zero restores',
      () async {
        // arrange
        final response = {
          'data': {'restores': []}
        };
        // graphql client uses the 'send' method
        when(() => mockHttpClient.send(any())).thenAnswer((_) async {
          final bytes = utf8.encode(json.encode(response));
          final stream = http.ByteStream.fromBytes(bytes);
          return http.StreamedResponse(stream, 200);
        });
        // act
        final result = await dataSource.getAllRestores();
        // assert
        expect(result, isList);
        expect(result, hasLength(equals(0)));
      },
    );

    test(
      'should return one restore request',
      () async {
        // arrange
        final response = {
          'data': {
            'restores': [
              {
                'tree': 'sha1-cafebabe',
                'entry': 'file',
                'filepath': 'dir/file',
                'dataset': 'data123',
                'finished': null,
                'filesRestored': 123,
                'errorMessage': null,
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
        final result = await dataSource.getAllRestores();
        // assert
        expect(result, isList);
        expect(result, hasLength(equals(1)));
        const store = RequestModel(
          tree: 'sha1-cafebabe',
          entry: 'file',
          filepath: 'dir/file',
          dataset: 'data123',
          finished: None(),
          filesRestored: 123,
          errorMessage: None(),
        );
        expect(result, contains(store));
      },
    );

    test(
      'should return all restores',
      () async {
        // arrange
        final response = {
          'data': {
            'restores': [
              {
                'tree': 'sha1-cafebabe',
                'entry': 'file',
                'filepath': 'dir/file',
                'dataset': 'data123',
                'finished': null,
                'filesRestored': 123,
                'errorMessage': null,
              },
              {
                'tree': 'sha1-cafed00d',
                'entry': 'file',
                'filepath': 'dir/dir/file',
                'dataset': 'data123',
                'finished': null,
                'filesRestored': 13,
                'errorMessage': null,
              },
              {
                'tree': 'sha1-deadbeef',
                'entry': 'xfiles',
                'filepath': 'folder/xfiles',
                'dataset': 'data123',
                'finished': '2021-04-09T06:32:16.786716685+00:00',
                'filesRestored': 0,
                'errorMessage': 'oh noes',
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
        final result = await dataSource.getAllRestores();
        // assert
        expect(result, isList);
        expect(result, hasLength(equals(3)));
        const store = RequestModel(
          tree: 'sha1-cafed00d',
          entry: 'file',
          filepath: 'dir/dir/file',
          dataset: 'data123',
          finished: None(),
          filesRestored: 13,
          errorMessage: None(),
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
          await dataSource.getAllRestores();
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
          await dataSource.getAllRestores();
          fail('should have raised an error');
        } catch (e) {
          expect(e, isA<ServerException>());
        }
      },
    );
  });

  group('cancelRestore', () {
    test('should process cancel request', () async {
      // arrange
      final response = {
        'data': {'cancelRestore': true}
      };
      when(() => mockHttpClient.send(any())).thenAnswer((_) async {
        final bytes = utf8.encode(json.encode(response));
        final stream = http.ByteStream.fromBytes(bytes);
        return http.StreamedResponse(stream, 200);
      });
      // act
      final result = await dataSource.cancelRestore(
          'sha1-cafebabe', 'entry', 'file', 'homura');
      // assert
      expect(result, equals(true));
    });

    test(
      'should report failure when response unsuccessful',
      () async {
        // arrange
        setUpMockHttpClientFailure403();
        // act, assert
        try {
          await dataSource.cancelRestore(
              'sha1-cafebabe', 'entry', 'file', 'homura');
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
          await dataSource.cancelRestore(
              'sha1-cafebabe', 'entry', 'file', 'homura');
          fail('should have raised an error');
        } catch (e) {
          expect(e, isA<ServerException>());
        }
      },
    );

    test('should return null when response is null', () async {
      // arrange
      final response = {
        'data': {'cancelRestore': null}
      };
      when(() => mockHttpClient.send(any())).thenAnswer((_) async {
        final bytes = utf8.encode(json.encode(response));
        final stream = http.ByteStream.fromBytes(bytes);
        return http.StreamedResponse(stream, 200);
      });
      // act
      final result = await dataSource.cancelRestore(
          'sha1-cafebabe', 'entry', 'file', 'homura');
      // assert
      expect(result, equals(false));
    });
  });
}
