//
// Copyright (c) 2022 Nathan Fiedler
//
import 'dart:convert';
import 'package:graphql/client.dart' as gql;
import 'package:http/http.dart' as http;
import 'package:mocktail/mocktail.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/data/models/data_set_model.dart';
import 'package:zorigami/core/data/sources/data_set_remote_data_source.dart';
import 'package:zorigami/core/domain/entities/data_set.dart';
import 'package:zorigami/core/domain/entities/snapshot.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:zorigami/core/error/exceptions.dart';

class MockHttpClient extends Mock implements http.Client {}

void main() {
  late DataSetRemoteDataSourceImpl dataSource;
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
    dataSource = DataSetRemoteDataSourceImpl(client: graphQLCient);
  });

  setUpAll(() {
    // mocktail needs a fallback for any() that involves custom types
    http.BaseRequest dummyRequest = http.Request(
      'GET',
      Uri(scheme: 'http', host: 'example.com', path: '/'),
    );
    registerFallbackValue(dummyRequest);
  });

  final tDataSet = DataSet(
    key: 'setkey1',
    computerId: 'cray-11',
    basepath: '/home/planet',
    schedules: [
      Schedule(
        frequency: Frequency.weekly,
        timeRange: const None(),
        dayOfMonth: const None(),
        dayOfWeek: Some(DayOfWeek.thu),
        weekOfMonth: const None(),
      )
    ],
    packSize: 67108864,
    stores: const ['store/local/setkey1'],
    excludes: const [],
    snapshot: Some(
      Snapshot(
        checksum: 'sha1-a6c930a6f7f9aa4eb8ef67980e9e8e32cd02fa2b',
        parent: Some('sha1-823bb0cf28e72fef2651cf1bb06abfc5fdc51634'),
        startTime: DateTime.parse('2020-03-15T05:36:04.960782134+00:00'),
        endTime: Some(
          DateTime.parse('2020-03-15T05:36:05.141905479+00:00'),
        ),
        fileCount: 125331,
        tree: 'sha1-698058583b2283b8c02ea5e40272c8364a0d6e78',
      ),
    ),
    status: Status.finished,
    errorMsg: const None(),
  );
  final tDataSetModel = DataSetModel.from(tDataSet);

  void setUpMockDeleteGraphQLResponse() {
    final response = {
      'data': {'deleteDataset': 'setkey1'}
    };
    // graphql client uses the 'send' method
    when(() => mockHttpClient.send(any())).thenAnswer((_) async {
      final bytes = utf8.encode(json.encode(response));
      final stream = http.ByteStream.fromBytes(bytes);
      return http.StreamedResponse(stream, 200);
    });
  }

  void setUpMockHttpClientGraphQLResponse(String operation) {
    final response = {
      'data': {
        '__typename': 'Dataset',
        operation: {
          '__typename': 'Dataset',
          'id': 'setkey1',
          'computerId': 'cray-11',
          'basepath': '/home/planet',
          'schedules': [
            {
              '__typename': 'Schedule',
              'frequency': 'WEEKLY',
              'timeRange': {
                '__typename': 'TimeRange',
                'startTime': null,
                'stopTime': null
              },
              'weekOfMonth': null,
              'dayOfWeek': 'THU',
              'dayOfMonth': null
            }
          ],
          'status': null,
          'errorMessage': null,
          'latestSnapshot': {
            '__typename': 'Snapshot',
            'checksum': 'sha1-a6c930a6f7f9aa4eb8ef67980e9e8e32cd02fa2b',
            'parent': 'sha1-823bb0cf28e72fef2651cf1bb06abfc5fdc51634',
            'startTime': '2020-03-15T05:36:04.960782134+00:00',
            'endTime': '2020-03-15T05:36:05.141905479+00:00',
            'fileCount': '125331',
            'tree': 'sha1-698058583b2283b8c02ea5e40272c8364a0d6e78'
          },
          'packSize': '67108864',
          'stores': ['store/local/setkey1'],
          'excludes': [],
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
            'path': ['dataset']
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

  void setUpMockBooleanGraphQLResponse(String field) {
    final response = {
      'data': {field: true}
    };
    // graphql client uses the 'send' method
    when(() => mockHttpClient.send(any())).thenAnswer((_) async {
      final bytes = utf8.encode(json.encode(response));
      final stream = http.ByteStream.fromBytes(bytes);
      return http.StreamedResponse(stream, 200);
    });
  }

  group('getAllDataSets', () {
    test(
      'should return zero data sets',
      () async {
        // arrange
        final response = {
          'data': {'datasets': []}
        };
        when(() => mockHttpClient.send(any())).thenAnswer((_) async {
          final bytes = utf8.encode(json.encode(response));
          final stream = http.ByteStream.fromBytes(bytes);
          return http.StreamedResponse(stream, 200);
        });
        // act
        final result = await dataSource.getAllDataSets();
        // assert
        expect(result, isList);
        expect(result, hasLength(equals(0)));
      },
    );

    test(
      'should return one data set',
      () async {
        // arrange
        final response = {
          'data': {
            '__typename': 'Dataset',
            'datasets': [
              {
                '__typename': 'Dataset',
                'id': 'a1',
                'computerId': 's1',
                'basepath': '/home/planet',
                'schedules': [],
                'packSize': '67108864',
                'stores': ['foo'],
                'excludes': [],
                'latestSnapshot': null,
              },
            ]
          }
        };
        when(() => mockHttpClient.send(any())).thenAnswer((_) async {
          final bytes = utf8.encode(json.encode(response));
          final stream = http.ByteStream.fromBytes(bytes);
          return http.StreamedResponse(stream, 200);
        });
        // act
        final result = await dataSource.getAllDataSets();
        // assert
        expect(result, isList);
        expect(result, hasLength(equals(1)));
        const simpleModel = DataSetModel(
          key: 'a1',
          computerId: 's1',
          basepath: '/home/planet',
          schedules: [],
          packSize: 67108864,
          stores: ['foo'],
          excludes: [],
          snapshot: None(),
          status: Status.none,
          errorMsg: None(),
        );
        expect(result, contains(simpleModel));
      },
    );

    test(
      'should return all data sets',
      () async {
        // arrange
        final response = {
          'data': {
            '__typename': 'Dataset',
            'datasets': [
              {
                '__typename': 'Dataset',
                'id': 'a1',
                'computerId': 's1',
                'basepath': '/home/planet',
                'schedules': [],
                'packSize': '67108864',
                'stores': ['foo'],
                'excludes': [],
                'latestSnapshot': null,
              },
              {
                '__typename': 'Dataset',
                'id': 'a2',
                'computerId': 's2',
                'basepath': '/home/town',
                'schedules': [],
                'packSize': '1024',
                'stores': ['store/local/foo'],
                'excludes': [],
                'latestSnapshot': null,
              },
              {
                '__typename': 'Dataset',
                'id': 'a3',
                'computerId': 's3',
                'basepath': '/home/sweet/home',
                'schedules': [],
                'packSize': '113',
                'stores': ['store/minio/minnie'],
                'excludes': [],
                'latestSnapshot': null,
              },
            ]
          }
        };
        when(() => mockHttpClient.send(any())).thenAnswer((_) async {
          final bytes = utf8.encode(json.encode(response));
          final stream = http.ByteStream.fromBytes(bytes);
          return http.StreamedResponse(stream, 200);
        });
        // act
        final result = await dataSource.getAllDataSets();
        // assert
        expect(result, isList);
        expect(result, hasLength(equals(3)));
        const simpleModel = DataSetModel(
          key: 'a1',
          computerId: 's1',
          basepath: '/home/planet',
          schedules: [],
          packSize: 67108864,
          stores: ['foo'],
          excludes: [],
          snapshot: None(),
          status: Status.none,
          errorMsg: None(),
        );
        expect(result, contains(simpleModel));
      },
    );

    test(
      'should report failure when response unsuccessful',
      () async {
        // arrange
        setUpMockHttpClientFailure403();
        // act, assert
        try {
          await dataSource.getAllDataSets();
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
          await dataSource.getAllDataSets();
          fail('should have raised an error');
        } catch (e) {
          expect(e, isA<ServerException>());
        }
      },
    );
  });

  group('deleteDataSet', () {
    test(
      'should delete a specific data set',
      () async {
        // arrange
        setUpMockDeleteGraphQLResponse();
        // act
        final result = await dataSource.deleteDataSet(tDataSet);
        // assert
        expect(result, equals(tDataSetModel.key));
      },
    );

    test(
      'should report failure when response unsuccessful',
      () async {
        // arrange
        setUpMockHttpClientFailure403();
        // act, assert
        try {
          await dataSource.deleteDataSet(tDataSet);
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
          await dataSource.deleteDataSet(tDataSet);
          fail('should have raised an error');
        } catch (e) {
          expect(e, isA<ServerException>());
        }
      },
    );
  });

  group('defineDataSet', () {
    test(
      'should define a new data set',
      () async {
        // arrange
        setUpMockHttpClientGraphQLResponse('defineDataset');
        // act
        final result = await dataSource.defineDataSet(tDataSet);
        // assert
        expect(result, equals(tDataSetModel));
      },
    );

    test(
      'should report failure when response unsuccessful',
      () async {
        // arrange
        setUpMockHttpClientFailure403();
        // act, assert
        try {
          await dataSource.defineDataSet(tDataSet);
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
          await dataSource.defineDataSet(tDataSet);
          fail('should have raised an error');
        } catch (e) {
          expect(e, isA<ServerException>());
        }
      },
    );
  });

  group('updateDataSet', () {
    test(
      'should update an existing data set',
      () async {
        // arrange
        setUpMockHttpClientGraphQLResponse('updateDataset');
        // act
        final result = await dataSource.updateDataSet(tDataSet);
        // assert
        expect(result, equals(tDataSetModel));
      },
    );

    test(
      'should report failure when response unsuccessful',
      () async {
        // arrange
        setUpMockHttpClientFailure403();
        // act, assert
        try {
          await dataSource.updateDataSet(tDataSet);
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
          await dataSource.updateDataSet(tDataSet);
          fail('should have raised an error');
        } catch (e) {
          expect(e, isA<ServerException>());
        }
      },
    );
  });

  group('startBackup', () {
    test(
      'should return true if successful',
      () async {
        // arrange
        setUpMockBooleanGraphQLResponse('startBackup');
        // act
        final result = await dataSource.startBackup(tDataSet);
        // assert
        expect(result, equals(true));
      },
    );

    test(
      'should report failure when response unsuccessful',
      () async {
        // arrange
        setUpMockHttpClientFailure403();
        // act, assert
        try {
          await dataSource.startBackup(tDataSet);
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
          await dataSource.startBackup(tDataSet);
          fail('should have raised an error');
        } catch (e) {
          expect(e, isA<ServerException>());
        }
      },
    );
  });

  group('stopBackup', () {
    test(
      'should return true if successful',
      () async {
        // arrange
        setUpMockBooleanGraphQLResponse('stopBackup');
        // act
        final result = await dataSource.stopBackup(tDataSet);
        // assert
        expect(result, equals(true));
      },
    );

    test(
      'should report failure when response unsuccessful',
      () async {
        // arrange
        setUpMockHttpClientFailure403();
        // act, assert
        try {
          await dataSource.stopBackup(tDataSet);
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
          await dataSource.stopBackup(tDataSet);
          fail('should have raised an error');
        } catch (e) {
          expect(e, isA<ServerException>());
        }
      },
    );
  });
}
