//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/request.dart';

class RequestModel extends Request {
  RequestModel({
    required String digest,
    required String filepath,
    required String dataset,
    required Option<DateTime> finished,
    required int filesRestored,
    required Option<String> errorMessage,
  }) : super(
          digest: digest,
          filepath: filepath,
          dataset: dataset,
          finished: finished,
          filesRestored: filesRestored,
          errorMessage: errorMessage,
        );

  factory RequestModel.from(Request request) {
    return RequestModel(
      digest: request.digest,
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
      digest: json['digest'],
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
      'digest': digest,
      'filepath': filepath,
      'dataset': dataset,
      'finished': finished.mapOr((v) => v.toIso8601String(), null),
      'filesRestored': filesRestored,
      'errorMessage': errorMessage.toNullable(),
    };
  }
}
