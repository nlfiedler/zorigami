//
// Copyright (c) 2024 Nathan Fiedler
//
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/snapshot.dart';

class SnapshotModel extends Snapshot {
  const SnapshotModel({
    required super.checksum,
    required super.parent,
    required super.startTime,
    required super.endTime,
    required super.fileCount,
    required super.tree,
  });

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
    final endTime = Option.from(json['endTime']).map(
      (v) => DateTime.parse(v as String),
    );
    return SnapshotModel(
      checksum: json['checksum'],
      parent: Option.from(json['parent']),
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
