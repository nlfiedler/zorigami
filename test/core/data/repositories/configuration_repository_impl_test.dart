//
// Copyright (c) 2024 Nathan Fiedler
//
import 'package:flutter_test/flutter_test.dart';
import 'package:mocktail/mocktail.dart';
import 'package:zorigami/core/error/exceptions.dart';
import 'package:zorigami/core/error/failures.dart';
import 'package:zorigami/core/data/sources/configuration_remote_data_source.dart';
import 'package:zorigami/core/data/models/configuration_model.dart';
import 'package:zorigami/core/data/repositories/configuration_repository_impl.dart';

class MockRemoteDataSource extends Mock
    implements ConfigurationRemoteDataSource {}

void main() {
  late ConfigurationRepositoryImpl repository;
  late MockRemoteDataSource mockRemoteDataSource;

  setUp(() {
    mockRemoteDataSource = MockRemoteDataSource();
    repository = ConfigurationRepositoryImpl(
      remoteDataSource: mockRemoteDataSource,
    );
  });

  const tConfigurationModel = ConfigurationModel(
    hostname: 'kohaku',
    username: 'zorigami',
    computerId: 'r9c7i5l6VFK5Smt8VkBBsQ',
  );

  group('getConfiguration', () {
    test(
      'should return remote data when remote data source returns data',
      () async {
        // arrange
        when(() => mockRemoteDataSource.getConfiguration())
            .thenAnswer((_) async => tConfigurationModel);
        // act
        final result = await repository.getConfiguration();
        // assert
        verify(() => mockRemoteDataSource.getConfiguration());
        expect(result.unwrap(), equals(tConfigurationModel));
      },
    );

    test(
      'should return failure when remote data source returns null',
      () async {
        // arrange
        when(() => mockRemoteDataSource.getConfiguration())
            .thenAnswer((_) async => null);
        // act
        final result = await repository.getConfiguration();
        // assert
        verify(() => mockRemoteDataSource.getConfiguration());
        expect(result.err().unwrap(), isA<ServerFailure>());
      },
    );

    test(
      'should return failure when remote data source is unsuccessful',
      () async {
        // arrange
        when(() => mockRemoteDataSource.getConfiguration())
            .thenThrow(const ServerException());
        // act
        final result = await repository.getConfiguration();
        // assert
        verify(() => mockRemoteDataSource.getConfiguration());
        expect(result.err().unwrap(), isA<ServerFailure>());
      },
    );
  });
}
