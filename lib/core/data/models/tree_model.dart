//
// Copyright (c) 2024 Nathan Fiedler
//
import 'package:zorigami/core/domain/entities/tree.dart';

class TreeReferenceModel extends TreeReference {
  const TreeReferenceModel({
    required EntryType type,
    required String value,
  }) : super(
          type: type,
          value: value,
        );

  // Feed the entire tree entry to get the reference.
  factory TreeReferenceModel.fromJson(Map<String, dynamic> json) {
    final type = decodeEntryType(json['reference']);
    final value = decodeReference(json['reference']);
    return TreeReferenceModel(
      type: type,
      value: value,
    );
  }

  @override
  String toString() {
    switch (type) {
      case EntryType.file:
        return 'file-$value';
      case EntryType.tree:
        return 'tree-$value';
      case EntryType.link:
        return 'link-$value';
      case EntryType.small:
        return 'small-$value';
    }
  }
}

class TreeEntryModel extends TreeEntry {
  const TreeEntryModel({
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
      'modTime': modTime.toIso8601String(),
      'reference': reference.toString(),
    };
  }
}

class TreeModel extends Tree {
  const TreeModel({
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

EntryType decodeEntryType(String reference) {
  if (reference.startsWith('file-')) {
    return EntryType.file;
  } else if (reference.startsWith('tree-')) {
    return EntryType.tree;
  } else if (reference.startsWith('link-')) {
    return EntryType.link;
  } else if (reference.startsWith('small-')) {
    return EntryType.small;
  }
  throw ArgumentError('unrecognized type: $reference');
}

String encodeEntryType(EntryType type) {
  switch (type) {
    case EntryType.file:
      return 'FILE';
    case EntryType.tree:
      return 'DIR';
    case EntryType.link:
      return 'LINK';
    case EntryType.small:
      return 'SMALL';
  }
}

String decodeReference(String reference) {
  if (reference.startsWith('file-')) {
    return reference.substring(5);
  } else if (reference.startsWith('tree-')) {
    return reference.substring(5);
  } else if (reference.startsWith('link-')) {
    return reference.substring(5);
  } else if (reference.startsWith('small-')) {
    return reference.substring(6);
  }
  throw ArgumentError('unrecognized reference: $reference');
}
