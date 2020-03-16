//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:flutter_test/flutter_test.dart';
import 'package:mockito/mockito.dart';
import 'package:zorigami/core/error/exceptions.dart';
import 'package:zorigami/core/error/failures.dart';
import 'package:zorigami/core/data/sources/configuration_remote_data_source.dart';
import 'package:zorigami/core/data/models/configuration_model.dart';
import 'package:zorigami/core/data/repositories/configuration_repository_impl.dart';

class MockRemoteDataSource extends Mock
    implements ConfigurationRemoteDataSource {}

void main() {
  ConfigurationRepositoryImpl repository;
  MockRemoteDataSource mockRemoteDataSource;

  setUp(() {
    mockRemoteDataSource = MockRemoteDataSource();
    repository = ConfigurationRepositoryImpl(
      remoteDataSource: mockRemoteDataSource,
    );
  });

  final tConfigurationModel = ConfigurationModel(
    hostname: 'kohaku',
    username: 'zorigami',
    computerId: 'r9c7i5l6VFK5Smt8VkBBsQ',
  );

  group('getConfiguration', () {
    test(
      'should return remote data when the call to remote data source is successful',
      () async {
        // arrange
        when(mockRemoteDataSource.getConfiguration())
            .thenAnswer((_) async => tConfigurationModel);
        // act
        final result = await repository.getConfiguration();
        // assert
        verify(mockRemoteDataSource.getConfiguration());
        expect(result.unwrap(), equals(tConfigurationModel));
      },
    );

    test(
      'should return server failure when the call to remote data source is unsuccessful',
      () async {
        // arrange
        when(mockRemoteDataSource.getConfiguration())
            .thenThrow(ServerException());
        // act
        final result = await repository.getConfiguration();
        // assert
        verify(mockRemoteDataSource.getConfiguration());
        expect(result.err().unwrap(), equals(ServerFailure()));
      },
    );
  });
}
