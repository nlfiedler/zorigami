//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:zorigami/core/data/models/tree_model.dart';
import 'package:zorigami/core/domain/entities/tree.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  group('SnapshotModel', () {
    final jsonSource = {
      'entries': [
        {
          'name': '.apdisk',
          'fstype': 'FILE',
          'modTime': '2018-05-07T03:52:44+00:00',
          'reference':
              'file-sha256-8c983bd0fac51fa7c6c59dcdd2d3cfd618a60d5b9b66bbe647880a451dd33ab4'
        },
        {
          'name': 'Documents',
          'fstype': 'DIR',
          'modTime': '2018-05-07T03:52:44+00:00',
          'reference': 'tree-sha1-2e768ea008e28b1d3c8e7ba13ee3a2075ad940a6'
        }
      ]
    };
    final tTreeModel = TreeModel(
      entries: [
        TreeEntryModel(
          name: '.apdisk',
          modTime: DateTime.utc(2018, 5, 7, 3, 52, 44),
          reference: TreeReferenceModel(
            type: EntryType.file,
            value:
                'sha256-8c983bd0fac51fa7c6c59dcdd2d3cfd618a60d5b9b66bbe647880a451dd33ab4',
          ),
        ),
        TreeEntryModel(
          name: 'Documents',
          modTime: DateTime.utc(2018, 5, 7, 3, 52, 44),
          reference: TreeReferenceModel(
            type: EntryType.tree,
            value: 'sha1-2e768ea008e28b1d3c8e7ba13ee3a2075ad940a6',
          ),
        ),
      ],
    );

    test(
      'should be a subclass of Snapshot entity',
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
