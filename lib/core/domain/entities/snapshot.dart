//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:equatable/equatable.dart';
import 'package:meta/meta.dart';
import 'package:oxidized/oxidized.dart';

/// A `Snapshot` holds details about a specific backup.
class Snapshot extends Equatable {
  // Computed checksum of the snapshot.
  final String checksum;
  // The snapshot before this one, if any.
  final Option<String> parent;
  // Time when the snapshot was first created.
  final DateTime startTime;
  // Time when the snapshot completely finished.
  final Option<DateTime> endTime;
  // Total number of files contained in this snapshot.
  final int fileCount;
  // Reference to the tree containing all of the files.
  final String tree;

  Snapshot({
    @required this.checksum,
    @required this.parent,
    @required this.startTime,
    @required this.endTime,
    @required this.fileCount,
    @required this.tree,
  });

  @override
  List<Object> get props => [checksum];

  @override
  bool get stringify => true;
}
