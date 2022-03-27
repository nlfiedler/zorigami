//
// Copyright (c) 2022 Nathan Fiedler
//
import 'package:equatable/equatable.dart';

enum EntryType { tree, file, link, small }

/// A tree reference captures the type and value of an entry in a tree. It can
/// either be a file, a directory (tree), a symbolic link, or it is an error
/// (i.e. backup failed for this entry). The [value] is a hash digest for files
/// and trees, the encoded symbolic link itself for links, and the message for
/// errors.
class TreeReference extends Equatable {
  final EntryType type;
  final String value;

  const TreeReference({
    required this.type,
    required this.value,
  });

  @override
  List<Object> get props => [type, value];

  @override
  bool get stringify => true;
}

/// A tree entry consists of a (file) name, a modified time, and the
/// [TreeReference] which contains the type and reference for the entry.
class TreeEntry extends Equatable {
  final String name;
  final DateTime modTime;
  final TreeReference reference;

  const TreeEntry({
    required this.name,
    required this.modTime,
    required this.reference,
  });

  @override
  List<Object> get props => [name, modTime, reference];

  @override
  bool get stringify => true;
}

/// A `Tree` may have zero or more instances of [TreeEntry].
class Tree extends Equatable {
  final List<TreeEntry> entries;

  const Tree({required this.entries});

  @override
  List<Object> get props => [entries];

  @override
  bool get stringify => true;
}
