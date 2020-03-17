//
// Copyright (c) 2020 Nathan Fiedler
//
import 'dart:convert';
import 'package:graphql/client.dart';
import 'package:http/http.dart' as http;
import 'package:mockito/mockito.dart';
import 'package:zorigami/core/data/models/tree_model.dart';
import 'package:zorigami/core/data/sources/tree_remote_data_source.dart';
import 'package:zorigami/core/domain/entities/tree.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:zorigami/core/error/exceptions.dart';

class MockHttpClient extends Mock implements http.Client {}

void main() {
  TreeRemoteDataSourceImpl dataSource;
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
    dataSource = TreeRemoteDataSourceImpl(client: graphQLCient);
  });

  final tTreeModel = TreeModel(
    entries: [
      TreeEntryModel(
        name: '.apdisk',
        modTime: DateTime.utc(2018, 5, 7, 3, 52, 44),
        reference: TreeReferenceModel(
          type: EntryType.file,
          value:
              'sha256-8c983bd0fac51fa7c6c59dcdd2d3cfd618a60d5b9b66bbe647880a451dd33ab4',
        ),
      ),
      TreeEntryModel(
        name: 'Documents',
        modTime: DateTime.utc(2018, 5, 7, 3, 52, 44),
        reference: TreeReferenceModel(
          type: EntryType.tree,
          value: 'sha1-2e768ea008e28b1d3c8e7ba13ee3a2075ad940a6',
        ),
      ),
    ],
  );

  void setUpMockHttpClientGraphQLResponse() {
    final response = {
      'data': {
        'tree': {
          'entries': [
            {
              'name': '.apdisk',
              'fstype': 'FILE',
              'modTime': '2018-05-07T03:52:44+00:00',
              'reference':
                  'file-sha256-8c983bd0fac51fa7c6c59dcdd2d3cfd618a60d5b9b66bbe647880a451dd33ab4'
            },
            {
              'name': 'Documents',
              'fstype': 'DIR',
              'modTime': '2018-05-07T03:52:44+00:00',
              'reference': 'tree-sha1-2e768ea008e28b1d3c8e7ba13ee3a2075ad940a6'
            }
          ]
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
            'path': ['tree']
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
      'data': {'tree': null}
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
      final bytes = List<int>();
      final stream = http.ByteStream.fromBytes(bytes);
      return http.StreamedResponse(stream, 403);
    });
  }

  group('getTree', () {
    test(
      'should return a specific tree',
      () async {
        // arrange
        setUpMockHttpClientGraphQLResponse();
        // act
        final result = await dataSource.getTree('sha1-cafebabe');
        // assert
        expect(result, equals(tTreeModel));
      },
    );

    test(
      'should report failure when response unsuccessful',
      () async {
        // arrange
        setUpMockHttpClientFailure403();
        // act, assert
        try {
          await dataSource.getTree('sha1-cafebabe');
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
          await dataSource.getTree('sha1-cafebabe');
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
        final result = await dataSource.getTree('sha1-cafebabe');
        // assert
        expect(result, isNull);
      },
    );
  });
}
