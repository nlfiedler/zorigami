//
// Copyright (c) 2024 Nathan Fiedler
//
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/request.dart';

class RequestModel extends Request {
  const RequestModel({
    required super.tree,
    required super.entry,
    required super.filepath,
    required super.dataset,
    required super.finished,
    required super.filesRestored,
    required super.errorMessage,
  });

  factory RequestModel.from(Request request) {
    return RequestModel(
      tree: request.tree,
      entry: request.entry,
      filepath: request.filepath,
      dataset: request.dataset,
      finished: request.finished,
      filesRestored: request.filesRestored,
      errorMessage: request.errorMessage,
    );
  }

  factory RequestModel.fromJson(Map<String, dynamic> json) {
    final finished = Option.from(json['finished']).map(
      (v) => DateTime.parse(v as String),
    );
    return RequestModel(
      tree: json['tree'],
      entry: json['entry'],
      filepath: json['filepath'],
      dataset: json['dataset'],
      finished: finished,
      // limiting file count to 2^53 (in JavaScript) is acceptable
      filesRestored: json['filesRestored'],
      errorMessage: Option.from(json['errorMessage']),
    );
  }

  Map<String, dynamic> toJson() {
    return {
      'tree': tree,
      'entry': entry,
      'filepath': filepath,
      'dataset': dataset,
      'finished': finished.mapOr((v) => v.toIso8601String(), null),
      'filesRestored': filesRestored,
      'errorMessage': errorMessage.toNullable(),
    };
  }
}
