//
// Copyright (c) 2022 Nathan Fiedler
//
import 'package:flutter_test/flutter_test.dart';
import 'package:mocktail/mocktail.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/error/exceptions.dart';
import 'package:zorigami/core/error/failures.dart';
import 'package:zorigami/core/data/sources/data_set_remote_data_source.dart';
import 'package:zorigami/core/data/models/data_set_model.dart';
import 'package:zorigami/core/data/repositories/data_set_repository_impl.dart';
import 'package:zorigami/core/domain/entities/data_set.dart';

class MockRemoteDataSource extends Mock implements DataSetRemoteDataSource {}

void main() {
  late DataSetRepositoryImpl repository;
  late MockRemoteDataSource mockRemoteDataSource;

  setUp(() {
    mockRemoteDataSource = MockRemoteDataSource();
    repository = DataSetRepositoryImpl(
      remoteDataSource: mockRemoteDataSource,
    );
  });

  final tDataSetModel = DataSetModel(
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
  final tDataSetList = [tDataSetModel];
  final List<DataSet> tDataSets = [tDataSetModel];
  final DataSet tDataSet = tDataSetModel;

  setUpAll(() {
    // mocktail needs a fallback for any() that involves custom types
    registerFallbackValue(tDataSet);
  });

  group('getAllDataSets', () {
    test(
      'should return remote data when the call to remote data source is successful',
      () async {
        // arrange
        when(() => mockRemoteDataSource.getAllDataSets())
            .thenAnswer((_) async => tDataSetList);
        // act
        final result = await repository.getAllDataSets();
        // assert
        verify(() => mockRemoteDataSource.getAllDataSets());
        expect(result.unwrap(), equals(tDataSets));
      },
    );

    test(
      'should return server failure when the call to remote data source is unsuccessful',
      () async {
        // arrange
        when(() => mockRemoteDataSource.getAllDataSets())
            .thenThrow(ServerException());
        // act
        final result = await repository.getAllDataSets();
        // assert
        verify(() => mockRemoteDataSource.getAllDataSets());
        expect(result.err().unwrap(), isA<ServerFailure>());
      },
    );
  });

  group('defineDataSet', () {
    test(
      'should return remote data when the call to remote data source is successful',
      () async {
        // arrange
        when(() => mockRemoteDataSource.defineDataSet(any()))
            .thenAnswer((_) async => tDataSetModel);
        // act
        final result = await repository.defineDataSet(tDataSet);
        // assert
        verify(() => mockRemoteDataSource.defineDataSet(any()));
        expect(result.unwrap(), equals(tDataSet));
      },
    );

    test(
      'should return server failure when the call to remote data source is unsuccessful',
      () async {
        // arrange
        when(() => mockRemoteDataSource.defineDataSet(any()))
            .thenThrow(ServerException());
        // act
        final result = await repository.defineDataSet(tDataSet);
        // assert
        verify(() => mockRemoteDataSource.defineDataSet(any()));
        expect(result.err().unwrap(), isA<ServerFailure>());
      },
    );
  });

  group('updateDataSet', () {
    test(
      'should return remote data when the call to remote data source is successful',
      () async {
        // arrange
        when(() => mockRemoteDataSource.updateDataSet(any()))
            .thenAnswer((_) async => tDataSetModel);
        // act
        final result = await repository.updateDataSet(tDataSet);
        // assert
        verify(() => mockRemoteDataSource.updateDataSet(any()));
        expect(result.unwrap(), equals(tDataSet));
      },
    );

    test(
      'should return server failure when the call to remote data source is unsuccessful',
      () async {
        // arrange
        when(() => mockRemoteDataSource.updateDataSet(any()))
            .thenThrow(ServerException());
        // act
        final result = await repository.updateDataSet(tDataSet);
        // assert
        verify(() => mockRemoteDataSource.updateDataSet(any()));
        expect(result.err().unwrap(), isA<ServerFailure>());
      },
    );
  });

  group('deleteDataSet', () {
    test(
      'should return remote data when the call to remote data source is successful',
      () async {
        // arrange
        when(() => mockRemoteDataSource.deleteDataSet(any()))
            .thenAnswer((_) async => tDataSetModel.key);
        // act
        final result = await repository.deleteDataSet(tDataSet);
        // assert
        verify(() => mockRemoteDataSource.deleteDataSet(any()));
        expect(result.unwrap(), equals(tDataSet));
      },
    );

    test(
      'should return server failure when the call to remote data source is unsuccessful',
      () async {
        // arrange
        when(() => mockRemoteDataSource.deleteDataSet(any()))
            .thenThrow(ServerException());
        // act
        final result = await repository.deleteDataSet(tDataSet);
        // assert
        verify(() => mockRemoteDataSource.deleteDataSet(any()));
        expect(result.err().unwrap(), isA<ServerFailure>());
      },
    );
  });
}
