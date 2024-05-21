//
// Copyright (c) 2024 Nathan Fiedler
//
import 'package:flutter_test/flutter_test.dart';
import 'package:mocktail/mocktail.dart';
import 'package:zorigami/core/error/exceptions.dart';
import 'package:zorigami/core/error/failures.dart';
import 'package:zorigami/core/data/sources/pack_store_remote_data_source.dart';
import 'package:zorigami/core/data/models/pack_store_model.dart';
import 'package:zorigami/core/data/repositories/pack_store_repository_impl.dart';
import 'package:zorigami/core/domain/entities/pack_store.dart';

class MockRemoteDataSource extends Mock implements PackStoreRemoteDataSource {}

void main() {
  late PackStoreRepositoryImpl repository;
  late MockRemoteDataSource mockRemoteDataSource;

  setUp(() {
    mockRemoteDataSource = MockRemoteDataSource();
    repository = PackStoreRepositoryImpl(
      remoteDataSource: mockRemoteDataSource,
    );
  });

  const tPackStoreModel = PackStoreModel(
    key: 'abc123',
    label: 'Label1',
    kind: StoreKind.local,
    options: {},
  );
  final tPackModelList = [tPackStoreModel];
  final List<PackStore> tPackStores = [tPackStoreModel];
  const PackStore tPackStore = tPackStoreModel;

  setUpAll(() {
    // mocktail needs a fallback for any() that involves custom types
    registerFallbackValue(tPackStore);
  });

  group('getAllPackStores', () {
    test(
      'should return remote data when the call to remote data source is successful',
      () async {
        // arrange
        when(() => mockRemoteDataSource.getAllPackStores())
            .thenAnswer((_) async => tPackModelList);
        // act
        final result = await repository.getAllPackStores();
        // assert
        verify(() => mockRemoteDataSource.getAllPackStores());
        expect(result.unwrap(), equals(tPackStores));
      },
    );

    test(
      'should return server failure when the call to remote data source is unsuccessful',
      () async {
        // arrange
        when(() => mockRemoteDataSource.getAllPackStores())
            .thenThrow(const ServerException());
        // act
        final result = await repository.getAllPackStores();
        // assert
        verify(() => mockRemoteDataSource.getAllPackStores());
        expect(result.err().unwrap(), isA<ServerFailure>());
      },
    );
  });

  group('definePackStore', () {
    test(
      'should return remote data when the call to remote data source is successful',
      () async {
        // arrange
        when(() => mockRemoteDataSource.definePackStore(any()))
            .thenAnswer((_) async => tPackStoreModel);
        // act
        final result = await repository.definePackStore(tPackStore);
        // assert
        verify(() => mockRemoteDataSource.definePackStore(any()));
        expect(result.unwrap(), equals(tPackStore));
      },
    );

    test(
      'should return server failure when the call to remote data source is unsuccessful',
      () async {
        // arrange
        when(() => mockRemoteDataSource.definePackStore(any()))
            .thenThrow(const ServerException());
        // act
        final result = await repository.definePackStore(tPackStore);
        // assert
        verify(() => mockRemoteDataSource.definePackStore(any()));
        expect(result.err().unwrap(), isA<ServerFailure>());
      },
    );
  });

  group('updatePackStore', () {
    test(
      'should return remote data when the call to remote data source is successful',
      () async {
        // arrange
        when(() => mockRemoteDataSource.updatePackStore(any()))
            .thenAnswer((_) async => tPackStoreModel);
        // act
        final result = await repository.updatePackStore(tPackStore);
        // assert
        verify(() => mockRemoteDataSource.updatePackStore(any()));
        expect(result.unwrap(), equals(tPackStore));
      },
    );

    test(
      'should return server failure when the call to remote data source is unsuccessful',
      () async {
        // arrange
        when(() => mockRemoteDataSource.updatePackStore(any()))
            .thenThrow(const ServerException());
        // act
        final result = await repository.updatePackStore(tPackStore);
        // assert
        verify(() => mockRemoteDataSource.updatePackStore(any()));
        expect(result.err().unwrap(), isA<ServerFailure>());
      },
    );
  });

  group('testPackStore', () {
    test(
      'should return remote data when the call to remote data source is successful',
      () async {
        // arrange
        when(() => mockRemoteDataSource.testPackStore(any()))
            .thenAnswer((_) async => 'ok');
        // act
        final result = await repository.testPackStore(tPackStore);
        // assert
        verify(() => mockRemoteDataSource.testPackStore(any()));
        expect(result.unwrap(), equals('ok'));
      },
    );

    test(
      'should return server failure when the call to remote data source is unsuccessful',
      () async {
        // arrange
        when(() => mockRemoteDataSource.testPackStore(any()))
            .thenThrow(const ServerException());
        // act
        final result = await repository.testPackStore(tPackStore);
        // assert
        verify(() => mockRemoteDataSource.testPackStore(any()));
        expect(result.err().unwrap(), isA<ServerFailure>());
      },
    );
  });

  group('deletePackStore', () {
    test(
      'should return remote data when the call to remote data source is successful',
      () async {
        // arrange
        when(() => mockRemoteDataSource.deletePackStore(any()))
            .thenAnswer((_) async => tPackStoreModel.key);
        // act
        await repository.deletePackStore(tPackStore);
        // assert
        verify(() => mockRemoteDataSource.deletePackStore(any()));
      },
    );

    test(
      'should return server failure when the call to remote data source is unsuccessful',
      () async {
        // arrange
        when(() => mockRemoteDataSource.deletePackStore(any()))
            .thenThrow(const ServerException());
        // act
        final result = await repository.deletePackStore(tPackStore);
        // assert
        verify(() => mockRemoteDataSource.deletePackStore(any()));
        expect(result.err().unwrap(), isA<ServerFailure>());
      },
    );
  });
}
