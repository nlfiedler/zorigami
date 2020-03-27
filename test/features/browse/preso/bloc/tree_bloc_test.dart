//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:bloc_test/bloc_test.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:mockito/mockito.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/tree.dart';
import 'package:zorigami/core/domain/repositories/tree_repository.dart';
import 'package:zorigami/core/domain/usecases/get_tree.dart';
import 'package:zorigami/features/browse/preso/bloc/tree_bloc.dart';

class MockTreeRepository extends Mock implements TreeRepository {}

void main() {
  MockTreeRepository mockTreeRepository;
  GetTree usecase;

  final tTreeReference = TreeReference(type: EntryType.file, value: 'cafebabe');
  final tTreeEntry = TreeEntry(
    name: 'filename.txt',
    reference: tTreeReference,
    modTime: DateTime.now(),
  );
  final tTree = Tree(entries: [tTreeEntry]);

  setUp(() {
    mockTreeRepository = MockTreeRepository();
    usecase = GetTree(mockTreeRepository);
    when(mockTreeRepository.getTree(any)).thenAnswer((_) async => Ok(tTree));
  });

  group('TreeBloc', () {
    blocTest(
      'emits [] when nothing is added',
      build: () async => TreeBloc(usecase: usecase),
      expect: [],
    );

    blocTest(
      'emits [Loading, Loaded] when LoadAllDataSets is added',
      build: () async => TreeBloc(usecase: usecase),
      act: (bloc) => bloc.add(LoadTree(digest: 'cafebabe')),
      expect: [Loading(), Loaded(tree: tTree)],
    );
  });
}
