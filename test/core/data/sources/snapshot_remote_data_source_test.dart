//
// Copyright (c) 2020 Nathan Fiedler
//
import 'dart:convert';
import 'package:graphql/client.dart';
import 'package:http/http.dart' as http;
import 'package:mockito/mockito.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/data/models/snapshot_model.dart';
import 'package:zorigami/core/data/sources/snapshot_remote_data_source.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:zorigami/core/error/exceptions.dart';

class MockHttpClient extends Mock implements http.Client {}

void main() {
  SnapshotRemoteDataSourceImpl dataSource;
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
    dataSource = SnapshotRemoteDataSourceImpl(client: graphQLCient);
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
    when(mockHttpClient.send(any)).thenAnswer((_) async {
      final bytes = utf8.encode(json.encode(response));
      final stream = http.ByteStream.fromBytes(bytes);
      return http.StreamedResponse(stream, 200);
    });
  }

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
    when(mockHttpClient.send(any)).thenAnswer((_) async {
      final bytes = utf8.encode(json.encode(response));
      final stream = http.ByteStream.fromBytes(bytes);
      return http.StreamedResponse(stream, 200);
    });
  }

  void setUpMockHttpClientFailure403() {
    when(mockHttpClient.send(any)).thenAnswer((_) async {
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

  group('restoreFile', () {
    test('should restore a file', () async {
      // arrange
      final response = {
        'data': {
          'restoreFile': '/path/to/file',
        }
      };
      when(mockHttpClient.send(any)).thenAnswer((_) async {
        final bytes = utf8.encode(json.encode(response));
        final stream = http.ByteStream.fromBytes(bytes);
        return http.StreamedResponse(stream, 200);
      });
      // act
      final result =
          await dataSource.restoreFile('sha1-cafebabe', 'file', 'homura');
      // assert
      expect(result, equals('/path/to/file'));
    });

    test(
      'should report failure when response unsuccessful',
      () async {
        // arrange
        setUpMockHttpClientFailure403();
        // act, assert
        try {
          await dataSource.restoreFile('sha1-cafebabe', 'file', 'homura');
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
          await dataSource.restoreFile('sha1-cafebabe', 'file', 'homura');
          fail('should have raised an error');
        } catch (e) {
          expect(e, isA<ServerException>());
        }
      },
    );

    test('should return null when response is null', () async {
      // arrange
      final response = {
        'data': {'restoreFile': null}
      };
      when(mockHttpClient.send(any)).thenAnswer((_) async {
        final bytes = utf8.encode(json.encode(response));
        final stream = http.ByteStream.fromBytes(bytes);
        return http.StreamedResponse(stream, 200);
      });
      // act
      final result =
          await dataSource.restoreFile('sha1-cafebabe', 'file', 'homura');
      // assert
      expect(result, isNull);
    });
  });
}
