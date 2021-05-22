//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:flutter_test/flutter_test.dart';
import 'package:mockito/annotations.dart';
import 'package:mockito/mockito.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/tree.dart';
import 'package:zorigami/core/domain/repositories/tree_repository.dart';
import 'package:zorigami/core/domain/usecases/get_tree.dart';
import './get_tree_test.mocks.dart';

@GenerateMocks([TreeRepository])
void main() {
  late GetTree usecase;
  late MockTreeRepository mockTreeRepository;

  setUp(() {
    mockTreeRepository = MockTreeRepository();
    usecase = GetTree(mockTreeRepository);
  });

  final tTreeReference = TreeReference(type: EntryType.file, value: 'cafebabe');
  final tTreeEntry = TreeEntry(
    name: 'filename.txt',
    reference: tTreeReference,
    modTime: DateTime.now(),
  );
  final tTree = Tree(entries: [tTreeEntry]);

  test(
    'should get a tree from the repository',
    () async {
      // arrange
      when(mockTreeRepository.getTree(any)).thenAnswer((_) async => Ok(tTree));
      // act
      final result = await usecase(Params(checksum: 'deadbeef'));
      // assert
      expect(result, Ok(tTree));
      verify(mockTreeRepository.getTree(any));
      verifyNoMoreInteractions(mockTreeRepository);
    },
  );
}
