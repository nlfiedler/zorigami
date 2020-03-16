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
    parent: Option.some('sha1-823bb0cf28e72fef2651cf1bb06abfc5fdc51634'),
    startTime: DateTime.parse('2020-03-15T05:36:04.960782134+00:00'),
    endTime: Option.some(
      DateTime.parse('2020-03-15T05:36:05.141905479+00:00'),
    ),
    fileCount: 125331,
    tree: 'sha1-698058583b2283b8c02ea5e40272c8364a0d6e78',
  );

  group('getSnapshot', () {
    test(
      'should return remote data when the call to remote data source is successful',
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
      'should return server failure when the call to remote data source is unsuccessful',
      () async {
        // arrange
        when(mockRemoteDataSource.getSnapshot(any))
            .thenThrow(ServerException());
        // act
        final result = await repository.getSnapshot('sha1-cafebabe');
        // assert
        verify(mockRemoteDataSource.getSnapshot(any));
        expect(result.err().unwrap(), equals(ServerFailure()));
      },
    );
  });
}
