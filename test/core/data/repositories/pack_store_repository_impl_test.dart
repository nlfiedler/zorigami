//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:flutter_test/flutter_test.dart';
import 'package:mockito/mockito.dart';
import 'package:zorigami/core/error/exceptions.dart';
import 'package:zorigami/core/error/failures.dart';
import 'package:zorigami/core/data/sources/pack_store_remote_data_source.dart';
import 'package:zorigami/core/data/models/pack_store_model.dart';
import 'package:zorigami/core/data/repositories/pack_store_repository_impl.dart';
import 'package:zorigami/core/domain/entities/pack_store.dart';

class MockRemoteDataSource extends Mock implements PackStoreRemoteDataSource {}

void main() {
  PackStoreRepositoryImpl repository;
  MockRemoteDataSource mockRemoteDataSource;

  setUp(() {
    mockRemoteDataSource = MockRemoteDataSource();
    repository = PackStoreRepositoryImpl(
      remoteDataSource: mockRemoteDataSource,
    );
  });

  final tPackStoreModel = PackStoreModel(
    key: 'abc123',
    label: 'Label1',
    kind: StoreKind.local,
    options: {},
  );
  final tPackModelList = [tPackStoreModel];
  final List<PackStore> tPackStores = [tPackStoreModel];
  final PackStore tPackStore = tPackStoreModel;

  group('getAllPackStores', () {
    test(
      'should return remote data when the call to remote data source is successful',
      () async {
        // arrange
        when(mockRemoteDataSource.getAllPackStores())
            .thenAnswer((_) async => tPackModelList);
        // act
        final result = await repository.getAllPackStores();
        // assert
        verify(mockRemoteDataSource.getAllPackStores());
        expect(result.unwrap(), equals(tPackStores));
      },
    );

    test(
      'should return server failure when the call to remote data source is unsuccessful',
      () async {
        // arrange
        when(mockRemoteDataSource.getAllPackStores())
            .thenThrow(ServerException());
        // act
        final result = await repository.getAllPackStores();
        // assert
        verify(mockRemoteDataSource.getAllPackStores());
        expect(result.err().unwrap(), isA<ServerFailure>());
      },
    );
  });

  group('definePackStore', () {
    test(
      'should return remote data when the call to remote data source is successful',
      () async {
        // arrange
        when(mockRemoteDataSource.definePackStore(any))
            .thenAnswer((_) async => tPackStoreModel);
        // act
        final result = await repository.definePackStore(tPackStore);
        // assert
        verify(mockRemoteDataSource.definePackStore(any));
        expect(result.unwrap(), equals(tPackStore));
      },
    );

    test(
      'should return server failure when the call to remote data source is unsuccessful',
      () async {
        // arrange
        when(mockRemoteDataSource.definePackStore(any))
            .thenThrow(ServerException());
        // act
        final result = await repository.definePackStore(tPackStore);
        // assert
        verify(mockRemoteDataSource.definePackStore(any));
        expect(result.err().unwrap(), isA<ServerFailure>());
      },
    );
  });

  group('updatePackStore', () {
    test(
      'should return remote data when the call to remote data source is successful',
      () async {
        // arrange
        when(mockRemoteDataSource.updatePackStore(any))
            .thenAnswer((_) async => tPackStoreModel);
        // act
        final result = await repository.updatePackStore(tPackStore);
        // assert
        verify(mockRemoteDataSource.updatePackStore(any));
        expect(result.unwrap(), equals(tPackStore));
      },
    );

    test(
      'should return server failure when the call to remote data source is unsuccessful',
      () async {
        // arrange
        when(mockRemoteDataSource.updatePackStore(any))
            .thenThrow(ServerException());
        // act
        final result = await repository.updatePackStore(tPackStore);
        // assert
        verify(mockRemoteDataSource.updatePackStore(any));
        expect(result.err().unwrap(), isA<ServerFailure>());
      },
    );
  });

  group('deletePackStore', () {
    test(
      'should return remote data when the call to remote data source is successful',
      () async {
        // arrange
        when(mockRemoteDataSource.deletePackStore(any))
            .thenAnswer((_) async => tPackStoreModel);
        // act
        final result = await repository.deletePackStore(tPackStore);
        // assert
        verify(mockRemoteDataSource.deletePackStore(any));
        expect(result.unwrap(), equals(tPackStore));
      },
    );

    test(
      'should return server failure when the call to remote data source is unsuccessful',
      () async {
        // arrange
        when(mockRemoteDataSource.deletePackStore(any))
            .thenThrow(ServerException());
        // act
        final result = await repository.deletePackStore(tPackStore);
        // assert
        verify(mockRemoteDataSource.deletePackStore(any));
        expect(result.err().unwrap(), isA<ServerFailure>());
      },
    );
  });
}
