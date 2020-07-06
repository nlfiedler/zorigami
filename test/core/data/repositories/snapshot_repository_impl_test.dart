//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:flutter_test/flutter_test.dart';
import 'package:mockito/mockito.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/error/exceptions.dart';
import 'package:zorigami/core/error/failures.dart';
import 'package:zorigami/core/data/sources/snapshot_remote_data_source.dart';
import 'package:zorigami/core/data/models/snapshot_model.dart';
import 'package:zorigami/core/data/repositories/snapshot_repository_impl.dart';

class MockRemoteDataSource extends Mock implements SnapshotRemoteDataSource {}

void main() {
  SnapshotRepositoryImpl repository;
  MockRemoteDataSource mockRemoteDataSource;

  setUp(() {
    mockRemoteDataSource = MockRemoteDataSource();
    repository = SnapshotRepositoryImpl(
      remoteDataSource: mockRemoteDataSource,
    );
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

  group('getSnapshot', () {
    test(
      'should return remote data when remote data source returns data',
      () async {
        // arrange
        when(mockRemoteDataSource.getSnapshot(any))
            .thenAnswer((_) async => tSnapshotModel);
        // act
        final result = await repository.getSnapshot('sha1-cafebabe');
        // assert
        verify(mockRemoteDataSource.getSnapshot(any));
        expect(result.unwrap(), equals(tSnapshotModel));
      },
    );

    test(
      'should return failure when remote data source returns null',
      () async {
        // arrange
        when(mockRemoteDataSource.getSnapshot(any))
            .thenAnswer((_) async => null);
        // act
        final result = await repository.getSnapshot('sha1-cafebabe');
        // assert
        verify(mockRemoteDataSource.getSnapshot(any));
        expect(result.err().unwrap(), isA<ServerFailure>());
      },
    );

    test(
      'should return server failure when remote data source is unsuccessful',
      () async {
        // arrange
        when(mockRemoteDataSource.getSnapshot(any))
            .thenThrow(ServerException());
        // act
        final result = await repository.getSnapshot('sha1-cafebabe');
        // assert
        verify(mockRemoteDataSource.getSnapshot(any));
        expect(result.err().unwrap(), isA<ServerFailure>());
      },
    );
  });

  group('restoreFile', () {
    test(
      'should return remote data when remote data source returns data',
      () async {
        // arrange
        when(mockRemoteDataSource.restoreFile(any, any, any))
            .thenAnswer((_) async => '/path/to/file');
        // act
        final result =
            await repository.restoreFile('sha1-cafebabe', 'file', 'homura');
        // assert
        verify(mockRemoteDataSource.restoreFile(any, any, any));
        expect(result.unwrap(), equals('/path/to/file'));
      },
    );

    test(
      'should return failure when remote data source returns null',
      () async {
        // arrange
        when(mockRemoteDataSource.restoreFile(any, any, any))
            .thenAnswer((_) async => null);
        // act
        final result =
            await repository.restoreFile('sha1-cafebabe', 'file', 'homura');
        // assert
        verify(mockRemoteDataSource.restoreFile(any, any, any));
        expect(result.err().unwrap(), isA<ServerFailure>());
      },
    );

    test(
      'should return server failure when remote data source is unsuccessful',
      () async {
        // arrange
        when(mockRemoteDataSource.restoreFile(any, any, any))
            .thenThrow(ServerException());
        // act
        final result =
            await repository.restoreFile('sha1-cafebabe', 'file', 'homura');
        // assert
        verify(mockRemoteDataSource.restoreFile(any, any, any));
        expect(result.err().unwrap(), isA<ServerFailure>());
      },
    );
  });
}
