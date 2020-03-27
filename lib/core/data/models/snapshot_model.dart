//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:meta/meta.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/snapshot.dart';

class SnapshotModel extends Snapshot {
  SnapshotModel({
    @required String checksum,
    @required Option<String> parent,
    @required DateTime startTime,
    @required Option<DateTime> endTime,
    @required int fileCount,
    @required String tree,
  }) : super(
          checksum: checksum,
          parent: parent,
          startTime: startTime,
          endTime: endTime,
          fileCount: fileCount,
          tree: tree,
        );

  factory SnapshotModel.from(Snapshot snapshot) {
    return SnapshotModel(
      checksum: snapshot.checksum,
      parent: snapshot.parent,
      startTime: snapshot.startTime,
      endTime: snapshot.endTime,
      fileCount: snapshot.fileCount,
      tree: snapshot.tree,
    );
  }

  factory SnapshotModel.fromJson(Map<String, dynamic> json) {
    final startTime = DateTime.parse(json['startTime']);
    final endTime = Option.some(json['endTime']).map((v) => DateTime.parse(v));
    return SnapshotModel(
      checksum: json['checksum'],
      parent: Option.some(json['parent']),
      startTime: startTime,
      endTime: endTime,
      // limiting file count to 2^53 (in JavaScript) is acceptable
      fileCount: int.parse(json['fileCount']),
      tree: json['tree'],
    );
  }

  Map<String, dynamic> toJson() {
    return {
      'checksum': checksum,
      'parent': parent.mapOr((v) => v, null),
      'startTime': startTime.toIso8601String(),
      'endTime': endTime.mapOr((v) => v.toIso8601String(), null),
      'fileCount': fileCount.toString(),
      'tree': tree,
    };
  }
}
