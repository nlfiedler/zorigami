//
// Copyright (c) 2021 Nathan Fiedler
//
import 'package:equatable/equatable.dart';
import 'package:oxidized/oxidized.dart';

/// A `Request` holds details regarding a file restore.
class Request extends Equatable {
  // Digest of either a file or a tree to restore.
  final String digest;
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

  Request({
    required this.digest,
    required this.filepath,
    required this.dataset,
    required this.finished,
    required this.filesRestored,
    required this.errorMessage,
  });

  @override
  List<Object> get props => [digest, filepath];

  @override
  bool get stringify => true;
}
