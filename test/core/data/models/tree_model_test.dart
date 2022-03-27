//
// Copyright (c) 2022 Nathan Fiedler
//
import 'package:zorigami/core/data/models/tree_model.dart';
import 'package:zorigami/core/domain/entities/tree.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  group('TreeModel', () {
    final jsonSource = {
      'entries': [
        {
          'name': '.CFUserTextEncoding',
          'modTime': '2021-09-29T16:24:15+00:00',
          'reference': 'small-MHgwOjB4MA=='
        },
        {
          'name': '.DS_Store',
          'modTime': '2022-01-17T22:20:52+00:00',
          'reference':
              'file-sha256-aa95ab743d4a5ed969de85bd574c57aa332772559264b944b0af7cc078f66131'
        },
        {
          'name': '.Trash',
          'modTime': '2022-03-24T02:20:29+00:00',
          'reference': 'tree-sha1-da39a3ee5e6b4b0d3255bfef95601890afd80709'
        },
        {
          'name': 'flutter_sdk',
          'modTime': '2021-04-25T16:42:22+00:00',
          'reference': 'link-L1VzZXJzL25maWVkbGVyL2Z2bS92ZXJzaW9ucy9zdGFibGU='
        },
      ]
    };
    final tTreeModel = TreeModel(
      entries: [
        TreeEntryModel(
          name: '.CFUserTextEncoding',
          modTime: DateTime.utc(2021, 9, 29, 16, 24, 15),
          reference: const TreeReferenceModel(
            type: EntryType.small,
            value: 'MHgwOjB4MA==',
          ),
        ),
        TreeEntryModel(
          name: '.DS_Store',
          modTime: DateTime.utc(2022, 1, 17, 22, 20, 52),
          reference: const TreeReferenceModel(
            type: EntryType.file,
            value:
                'sha256-aa95ab743d4a5ed969de85bd574c57aa332772559264b944b0af7cc078f66131',
          ),
        ),
        TreeEntryModel(
          name: '.Trash',
          modTime: DateTime.utc(2022, 3, 24, 2, 20, 29),
          reference: const TreeReferenceModel(
            type: EntryType.tree,
            value: 'sha1-da39a3ee5e6b4b0d3255bfef95601890afd80709',
          ),
        ),
        TreeEntryModel(
          name: 'flutter_sdk',
          modTime: DateTime.utc(2021, 4, 25, 16, 42, 22),
          reference: const TreeReferenceModel(
            type: EntryType.link,
            value: 'L1VzZXJzL25maWVkbGVyL2Z2bS92ZXJzaW9ucy9zdGFibGU=',
          ),
        ),
      ],
    );

    test(
      'should be a subclass of Tree entity',
      () {
        // assert
        expect(tTreeModel, isA<Tree>());
      },
    );

    test(
      'should convert to and from JSON',
      () {
        expect(
          TreeModel.fromJson(tTreeModel.toJson()),
          equals(tTreeModel),
        );
        expect(
          TreeModel.fromJson(jsonSource),
          equals(tTreeModel),
        );
      },
    );
  });
}
