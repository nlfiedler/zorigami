//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:zorigami/core/domain/entities/tree.dart';

class TreeReferenceModel extends TreeReference {
  TreeReferenceModel({
    required EntryType type,
    required String value,
  }) : super(
          type: type,
          value: value,
        );

  // Feed the entire tree entry to get the reference.
  factory TreeReferenceModel.fromJson(Map<String, dynamic> json) {
    final fstype = decodeEntryType(json['fstype']);
    final value = decodeReference(json['reference']);
    return TreeReferenceModel(
      type: fstype,
      value: value,
    );
  }

  @override
  String toString() {
    switch (type) {
      case EntryType.file:
        return 'file-' + value;
      case EntryType.tree:
        return 'tree-' + value;
      case EntryType.link:
        return 'link-' + value;
      case EntryType.error:
        return 'error-' + value;
    }
  }
}

class TreeEntryModel extends TreeEntry {
  TreeEntryModel({
    required String name,
    required DateTime modTime,
    required TreeReference reference,
  }) : super(
          name: name,
          modTime: modTime,
          reference: reference,
        );

  factory TreeEntryModel.from(TreeEntry entry) {
    return TreeEntryModel(
      name: entry.name,
      modTime: entry.modTime,
      reference: entry.reference,
    );
  }

  factory TreeEntryModel.fromJson(Map<String, dynamic> json) {
    final reference = TreeReferenceModel.fromJson(json);
    return TreeEntryModel(
      name: json['name'],
      modTime: DateTime.parse(json['modTime']),
      reference: reference,
    );
  }

  // Transforms the entry and the embedded reference.
  Map<String, dynamic> toJson() {
    return {
      'name': name,
      'fstype': encodeEntryType(reference.type),
      'modTime': modTime.toIso8601String(),
      'reference': reference.toString(),
    };
  }
}

class TreeModel extends Tree {
  TreeModel({
    required List<TreeEntry> entries,
  }) : super(entries: entries);

  factory TreeModel.fromJson(Map<String, dynamic> json) {
    final List<TreeEntryModel> entries = List.from(
      json['entries'].map((v) => TreeEntryModel.fromJson(v)),
    );
    return TreeModel(entries: entries);
  }

  Map<String, dynamic> toJson() {
    final entries = List.from(
      this.entries.map((v) => TreeEntryModel.from(v).toJson()),
    );
    return {'entries': entries};
  }
}

EntryType decodeEntryType(String fstype) {
  switch (fstype) {
    case 'FILE':
      return EntryType.file;
    case 'DIR':
      return EntryType.tree;
    case 'LINK':
      return EntryType.link;
    case 'ERROR':
      return EntryType.error;
  }
  throw ArgumentError('unrecognized type: ' + fstype);
}

String encodeEntryType(EntryType type) {
  switch (type) {
    case EntryType.file:
      return 'FILE';
    case EntryType.tree:
      return 'DIR';
    case EntryType.link:
      return 'LINK';
    case EntryType.error:
      return 'ERROR';
  }
}

String decodeReference(String reference) {
  if (reference.startsWith('file-')) {
    return reference.substring(5);
  } else if (reference.startsWith('tree-')) {
    return reference.substring(5);
  } else if (reference.startsWith('link-')) {
    return reference.substring(5);
  }
  throw ArgumentError('unrecognized reference: ' + reference);
}
