//
// Copyright (c) 2020 Nathan Fiedler
//
import 'dart:convert';
import 'package:meta/meta.dart';
import 'package:zorigami/core/domain/entities/pack_store.dart';

class PackStoreModel extends PackStore {
  PackStoreModel({
    @required String key,
    @required String label,
    @required StoreKind kind,
    @required Map<String, dynamic> options,
  }) : super(
          key: key,
          label: label,
          kind: kind,
          options: options,
        );

  factory PackStoreModel.fromJson(Map<String, dynamic> json) {
    final kind = decodeKind(json['kind']);
    final options = decodeOptions(json['options']);
    return PackStoreModel(
      key: json['key'],
      label: json['label'],
      kind: kind,
      options: options,
    );
  }

  Map<String, dynamic> toJson() {
    final kind = encodeKind(this.kind);
    final encodedOptions = encodeOptions(options);
    return {
      'key': key,
      'label': label,
      'kind': kind,
      'options': encodedOptions,
    };
  }
}

StoreKind decodeKind(String kind) {
  if (kind == 'local') {
    return StoreKind.local;
  } else if (kind == 'minio') {
    return StoreKind.minio;
  } else if (kind == 'sftp') {
    return StoreKind.sftp;
  } else {
    throw ArgumentError('kind is not recognized');
  }
}

String encodeKind(StoreKind kind) {
  switch (kind) {
    case StoreKind.local:
      return 'local';
    case StoreKind.minio:
      return 'minio';
    case StoreKind.sftp:
      return 'sftp';
    default:
      throw ArgumentError('kind is not recognized');
  }
}

Map<String, dynamic> decodeOptions(String options) {
  if (options.isEmpty) {
    return Map<String, dynamic>();
  }
  return json.decode(utf8.decode(base64Url.decode(options)));
}

String encodeOptions(Map<String, dynamic> options) {
  if (options.isEmpty) {
    return '';
  }
  return base64Url.encode(utf8.encode(json.encode(options)));
}
