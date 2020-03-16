//
// Copyright (c) 2020 Nathan Fiedler
//
import 'dart:convert';
import 'package:graphql/client.dart';
import 'package:http/http.dart' as http;
import 'package:mockito/mockito.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/data/models/data_set_model.dart';
import 'package:zorigami/core/data/models/snapshot_model.dart';
import 'package:zorigami/core/data/sources/data_set_remote_data_source.dart';
import 'package:zorigami/core/domain/entities/data_set.dart';
import 'package:zorigami/core/domain/entities/snapshot.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:zorigami/core/error/exceptions.dart';

class MockHttpClient extends Mock implements http.Client {}

void main() {
  DataSetRemoteDataSourceImpl dataSource;
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
    dataSource = DataSetRemoteDataSourceImpl(client: graphQLCient);
  });

  final tDataSet = DataSet(
    key: 'setkey1',
    computerId: 'cray-11',
    basepath: '/home/planet',
    schedules: [
      Schedule(
        frequency: Frequency.weekly,
        timeRange: Option.none(),
        dayOfMonth: Option.none(),
        dayOfWeek: Option.some(DayOfWeek.thu),
        weekOfMonth: Option.none(),
      )
    ],
    packSize: 67108864,
    stores: ['store/local/setkey1'],
    snapshot: Option.some(
      Snapshot(
        checksum: 'sha1-a6c930a6f7f9aa4eb8ef67980e9e8e32cd02fa2b',
        parent: Option.some('sha1-823bb0cf28e72fef2651cf1bb06abfc5fdc51634'),
        startTime: DateTime.parse('2020-03-15T05:36:04.960782134+00:00'),
        endTime: Option.some(
          DateTime.parse('2020-03-15T05:36:05.141905479+00:00'),
        ),
        fileCount: 125331,
        tree: 'sha1-698058583b2283b8c02ea5e40272c8364a0d6e78',
      ),
    ),
  );
  final tDataSetModel = DataSetModel(
    key: 'setkey1',
    computerId: 'cray-11',
    basepath: '/home/planet',
    schedules: [
      ScheduleModel(
        frequency: Frequency.weekly,
        timeRange: Option.none(),
        dayOfMonth: Option.none(),
        dayOfWeek: Option.some(DayOfWeek.thu),
        weekOfMonth: Option.none(),
      )
    ],
    packSize: 67108864,
    stores: ['store/local/setkey1'],
    snapshot: Option.some(
      SnapshotModel(
        checksum: 'sha1-a6c930a6f7f9aa4eb8ef67980e9e8e32cd02fa2b',
        parent: Option.some('sha1-823bb0cf28e72fef2651cf1bb06abfc5fdc51634'),
        startTime: DateTime.parse('2020-03-15T05:36:04.960782134+00:00'),
        endTime: Option.some(
          DateTime.parse('2020-03-15T05:36:05.141905479+00:00'),
        ),
        fileCount: 125331,
        tree: 'sha1-698058583b2283b8c02ea5e40272c8364a0d6e78',
      ),
    ),
  );

  void setUpMockHttpClientGraphQLResponse() {
    final response = {
      'data': {
        'dataset': {
          'key': 'setkey1',
          'computerId': 'cray-11',
          'basepath': '/home/planet',
          'schedules': [
            {
              "frequency": "WEEKLY",
              "timeRange": null,
              "weekOfMonth": null,
              "dayOfWeek": "THU",
              "dayOfMonth": null
            }
          ],
          'packSize': "67108864",
          'stores': ['store/local/setkey1'],
          'snapshot': {
            "checksum": "sha1-a6c930a6f7f9aa4eb8ef67980e9e8e32cd02fa2b",
            "parent": "sha1-823bb0cf28e72fef2651cf1bb06abfc5fdc51634",
            "startTime": "2020-03-15T05:36:04.960782134+00:00",
            "endTime": "2020-03-15T05:36:05.141905479+00:00",
            "fileCount": "125331",
            "tree": "sha1-698058583b2283b8c02ea5e40272c8364a0d6e78"
          },
        }
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
    when(mockHttpClient.send(any)).thenAnswer((_) async {
      final bytes = List<int>();
      final stream = http.ByteStream.fromBytes(bytes);
      return http.StreamedResponse(stream, 403);
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
        when(mockHttpClient.send(any)).thenAnswer((_) async {
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
            'datasets': [
              {
                'key': 'a1',
                'computerId': 's1',
                'basepath': '/home/planet',
                'schedules': [],
                'packSize': "67108864",
                'stores': ['foo'],
                'snapshot': null,
              },
            ]
          }
        };
        when(mockHttpClient.send(any)).thenAnswer((_) async {
          final bytes = utf8.encode(json.encode(response));
          final stream = http.ByteStream.fromBytes(bytes);
          return http.StreamedResponse(stream, 200);
        });
        // act
        final result = await dataSource.getAllDataSets();
        // assert
        expect(result, isList);
        expect(result, hasLength(equals(1)));
        final simpleModel = DataSetModel(
          key: 'a1',
          computerId: 's1',
          basepath: '/home/planet',
          schedules: [],
          packSize: 67108864,
          stores: ['foo'],
          snapshot: Option.none(),
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
            'datasets': [
              {
                'key': 'a1',
                'computerId': 's1',
                'basepath': '/home/planet',
                'schedules': [],
                'packSize': "67108864",
                'stores': ['foo'],
                'snapshot': null,
              },
              {
                'key': 'a2',
                'computerId': 's2',
                'basepath': '/home/town',
                'schedules': [],
                'packSize': "1024",
                'stores': ['store/local/foo'],
                'snapshot': null,
              },
              {
                'key': 'a3',
                'computerId': 's3',
                'basepath': '/home/sweet/home',
                'schedules': [],
                'packSize': "113",
                'stores': ['store/minio/minnie'],
                'snapshot': null,
              },
            ]
          }
        };
        when(mockHttpClient.send(any)).thenAnswer((_) async {
          final bytes = utf8.encode(json.encode(response));
          final stream = http.ByteStream.fromBytes(bytes);
          return http.StreamedResponse(stream, 200);
        });
        // act
        final result = await dataSource.getAllDataSets();
        // assert
        expect(result, isList);
        expect(result, hasLength(equals(3)));
        final simpleModel = DataSetModel(
          key: 'a1',
          computerId: 's1',
          basepath: '/home/planet',
          schedules: [],
          packSize: 67108864,
          stores: ['foo'],
          snapshot: Option.none(),
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
        //
        // wanted to do this, but it failed with an "asynchronous gap" error,
        // tried numerous alternatives to no avail
        //
        // final future = dataSource.getDataSet('foobar');
        // expect(future, completion(throwsA(ServerException)));
      },
    );
  });

  group('getDataSet', () {
    test(
      'should return a specific data set',
      () async {
        // arrange
        setUpMockHttpClientGraphQLResponse();
        // act
        final result = await dataSource.getDataSet('abc123');
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
          await dataSource.getDataSet('foobar');
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
          await dataSource.getDataSet('foobar');
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
        setUpMockHttpClientGraphQLResponse();
        // act
        final result = await dataSource.deleteDataSet('abc123');
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
          await dataSource.deleteDataSet('foobar');
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
          await dataSource.deleteDataSet('foobar');
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
        setUpMockHttpClientGraphQLResponse();
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
        setUpMockHttpClientGraphQLResponse();
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
}
