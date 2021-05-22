//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:flutter_test/flutter_test.dart';
import 'package:mockito/annotations.dart';
import 'package:mockito/mockito.dart';
import 'package:zorigami/core/error/exceptions.dart';
import 'package:zorigami/core/error/failures.dart';
import 'package:zorigami/core/data/sources/configuration_remote_data_source.dart';
import 'package:zorigami/core/data/models/configuration_model.dart';
import 'package:zorigami/core/data/repositories/configuration_repository_impl.dart';
import './configuration_repository_impl_test.mocks.dart';

@GenerateMocks([ConfigurationRemoteDataSource])
void main() {
  late ConfigurationRepositoryImpl repository;
  late MockConfigurationRemoteDataSource mockRemoteDataSource;

  setUp(() {
    mockRemoteDataSource = MockConfigurationRemoteDataSource();
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
      'should return remote data when remote data source returns data',
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
      'should return failure when remote data source returns null',
      () async {
        // arrange
        when(mockRemoteDataSource.getConfiguration())
            .thenAnswer((_) async => null);
        // act
        final result = await repository.getConfiguration();
        // assert
        verify(mockRemoteDataSource.getConfiguration());
        expect(result.err().unwrap(), isA<ServerFailure>());
      },
    );

    test(
      'should return failure when remote data source is unsuccessful',
      () async {
        // arrange
        when(mockRemoteDataSource.getConfiguration())
            .thenThrow(ServerException());
        // act
        final result = await repository.getConfiguration();
        // assert
        verify(mockRemoteDataSource.getConfiguration());
        expect(result.err().unwrap(), isA<ServerFailure>());
      },
    );
  });
}
