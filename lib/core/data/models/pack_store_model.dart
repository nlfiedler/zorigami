//
// Copyright (c) 2024 Nathan Fiedler
//
import 'package:zorigami/core/domain/entities/pack_store.dart';

class PackStoreModel extends PackStore {
  const PackStoreModel({
    required super.key,
    required super.label,
    required super.kind,
    required super.options,
  });

  factory PackStoreModel.fromStore(PackStore store) {
    return PackStoreModel(
      key: store.key,
      label: store.label,
      kind: store.kind,
      options: store.options,
    );
  }

  factory PackStoreModel.fromJson(Map<String, dynamic> json) {
    final kind = decodeKind(json['storeType']);
    final options = decodeOptions(json['properties']);
    return PackStoreModel(
      key: json['id'],
      kind: kind,
      label: json['label'],
      options: options,
    );
  }

  Map<String, dynamic> toJson() {
    final kind = encodeKind(this.kind);
    final options = encodeOptions(this.options);
    return {
      'id': key,
      'storeType': kind,
      'label': label,
      'properties': options,
    };
  }
}

StoreKind decodeKind(String kind) {
  if (kind == 'amazon') {
    return StoreKind.amazon;
  } else if (kind == 'azure') {
    return StoreKind.azure;
  } else if (kind == 'google') {
    return StoreKind.google;
  } else if (kind == 'local') {
    return StoreKind.local;
  } else if (kind == 'minio') {
    return StoreKind.minio;
  } else if (kind == 'sftp') {
    return StoreKind.sftp;
  } else {
    throw ArgumentError('kind "$kind" is not recognized');
  }
}

String encodeKind(StoreKind kind) {
  switch (kind) {
    case StoreKind.amazon:
      return 'amazon';
    case StoreKind.azure:
      return 'azure';
    case StoreKind.google:
      return 'google';
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

Map<String, dynamic> decodeOptions(List<dynamic> options) {
  if (options.isEmpty) {
    return <String, dynamic>{};
  }
  final Map<String, dynamic> results = {};
  for (var e in options) {
    results[e['name']] = e['value'];
  }
  return results;
}

List<Map<String, dynamic>> encodeOptions(Map<String, dynamic> options) {
  if (options.isEmpty) {
    return [];
  }
  final List<Map<String, dynamic>> results = [];
  options.forEach((key, value) => results.add({'name': key, 'value': value}));
  return results;
}

List<Map<String, dynamic>> encodeQLOptions(Map<String, dynamic> options) {
  if (options.isEmpty) {
    return [];
  }
  final List<Map<String, dynamic>> results = [];
  options.forEach((key, value) => results.add({
        '__typename': 'Property',
        'name': key,
        'value': value,
      }));
  return results;
}
