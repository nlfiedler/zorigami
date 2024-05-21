//
// Copyright (c) 2024 Nathan Fiedler
//
import 'package:flutter_test/flutter_test.dart';
import 'package:mocktail/mocktail.dart';
import 'package:zorigami/core/error/exceptions.dart';
import 'package:zorigami/core/error/failures.dart';
import 'package:zorigami/core/data/sources/tree_remote_data_source.dart';
import 'package:zorigami/core/data/models/tree_model.dart';
import 'package:zorigami/core/data/repositories/tree_repository_impl.dart';
import 'package:zorigami/core/domain/entities/tree.dart';

class MockRemoteDataSource extends Mock implements TreeRemoteDataSource {}

void main() {
  late TreeRepositoryImpl repository;
  late MockRemoteDataSource mockRemoteDataSource;

  setUp(() {
    mockRemoteDataSource = MockRemoteDataSource();
    repository = TreeRepositoryImpl(
      remoteDataSource: mockRemoteDataSource,
    );
  });

  final tTreeModel = TreeModel(
    entries: [
      TreeEntryModel(
        name: '.apdisk',
        modTime: DateTime.utc(2018, 5, 7, 3, 52, 44),
        reference: const TreeReferenceModel(
          type: EntryType.file,
          value:
              'blake3-8c983bd0fac51fa7c6c59dcdd2d3cfd618a60d5b9b66bbe647880a451dd33ab4',
        ),
      ),
      TreeEntryModel(
        name: 'Documents',
        modTime: DateTime.utc(2018, 5, 7, 3, 52, 44),
        reference: const TreeReferenceModel(
          type: EntryType.tree,
          value: 'sha1-2e768ea008e28b1d3c8e7ba13ee3a2075ad940a6',
        ),
      ),
    ],
  );

  group('getTree', () {
    test(
      'should return remote data when the call to remote data source is successful',
      () async {
        // arrange
        when(() => mockRemoteDataSource.getTree(any()))
            .thenAnswer((_) async => tTreeModel);
        // act
        final result = await repository.getTree('sha1-cafebabe');
        // assert
        verify(() => mockRemoteDataSource.getTree(any()));
        expect(result.unwrap(), equals(tTreeModel));
      },
    );

    test(
      'should return failure when remote data source returns null',
      () async {
        // arrange
        when(() => mockRemoteDataSource.getTree(any()))
            .thenAnswer((_) async => null);
        // act
        final result = await repository.getTree('sha1-cafebabe');
        // assert
        verify(() => mockRemoteDataSource.getTree(any()));
        expect(result.err().unwrap(), isA<ServerFailure>());
      },
    );

    test(
      'should return server failure when the call to remote data source is unsuccessful',
      () async {
        // arrange
        when(() => mockRemoteDataSource.getTree(any()))
            .thenThrow(const ServerException());
        // act
        final result = await repository.getTree('sha1-cafebabe');
        // assert
        verify(() => mockRemoteDataSource.getTree(any()));
        expect(result.err().unwrap(),
            equals(const ServerFailure('ServerException')));
      },
    );
  });
}
