//
// Copyright (c) 2024 Nathan Fiedler
//
import 'dart:convert';
import 'package:graphql/client.dart' as gql;
import 'package:http/http.dart' as http;
import 'package:mocktail/mocktail.dart';
import 'package:zorigami/core/data/models/configuration_model.dart';
import 'package:zorigami/core/data/sources/configuration_remote_data_source.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:zorigami/core/error/exceptions.dart';

class MockHttpClient extends Mock implements http.Client {}

void main() {
  late ConfigurationRemoteDataSourceImpl dataSource;
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
    dataSource = ConfigurationRemoteDataSourceImpl(client: graphQLCient);
  });

  setUpAll(() {
    // mocktail needs a fallback for any() that involves custom types
    http.BaseRequest dummyRequest = http.Request(
      'GET',
      Uri(scheme: 'http', host: 'example.com', path: '/'),
    );
    registerFallbackValue(dummyRequest);
  });

  const tConfigurationModel = ConfigurationModel(
    hostname: 'kohaku',
    username: 'zorigami',
    computerId: 'r9c7i5l6VFK5Smt8VkBBsQ',
  );

  void setUpMockHttpClientGraphQLResponse() {
    final response = {
      'data': {
        'configuration': {
          'hostname': 'kohaku',
          'username': 'zorigami',
          'computerId': 'r9c7i5l6VFK5Smt8VkBBsQ'
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

  void setUpMockGraphQLNullResponse() {
    final response = {
      'data': {'configuration': null}
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
            'path': ['configuration']
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

  group('getConfiguration', () {
    test(
      'should return the system configuration',
      () async {
        // arrange
        setUpMockHttpClientGraphQLResponse();
        // act
        final result = await dataSource.getConfiguration();
        // assert
        expect(result, equals(tConfigurationModel));
      },
    );

    test(
      'should report failure when response unsuccessful',
      () async {
        // arrange
        setUpMockHttpClientFailure403();
        // act, assert
        try {
          await dataSource.getConfiguration();
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
          await dataSource.getConfiguration();
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
        final result = await dataSource.getConfiguration();
        // assert
        expect(result, isNull);
      },
    );
  });
}
