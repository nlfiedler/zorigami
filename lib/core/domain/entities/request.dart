//
// Copyright (c) 2022 Nathan Fiedler
//
import 'package:equatable/equatable.dart';
import 'package:oxidized/oxidized.dart';

/// A `Request` holds details regarding a file restore.
class Request extends Equatable {
  // Digest of the tree containing the entry to restore.
  final String tree;
  // Name of the entry within the tree to be restored.
  final String entry;
  // Relative path where file/tree will be restored.
  final String filepath;
  // Identifier of the dataset containing the data.
  final String dataset;
  // The datetime when the request was completed.
  final Option<DateTime> finished;
  // Number of files restored so far during the restoration.
  final int filesRestored;
  // Error message if request processing failed.
  final Option<String> errorMessage;

  const Request({
    required this.tree,
    required this.entry,
    required this.filepath,
    required this.dataset,
    required this.finished,
    required this.filesRestored,
    required this.errorMessage,
  });

  @override
  List<Object> get props => [tree, entry];

  @override
  bool get stringify => true;
}
