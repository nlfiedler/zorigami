//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:equatable/equatable.dart';

enum StoreKind { google, local, minio, sftp }

class PackStore extends Equatable {
  /// The `key` is unique among all pack stores.
  final String key;

  /// The `label` is user-defined and can be anything.
  final String label;

  /// The kind of store this represents.
  final StoreKind kind;

  /// Map of names and values for configuring this pack store.
  final Map<String, dynamic> options;

  PackStore({
    required this.key,
    required this.label,
    required this.kind,
    required this.options,
  });

  @override
  List<Object> get props => [key, label, kind, options];

  @override
  bool get stringify => true;
}

String packStoreTitle(PackStore store) {
  return store.label + ' :: ' + prettyKind(store.kind);
}

String packStoreSubtitle(PackStore store) {
  switch (store.kind) {
    case StoreKind.google:
      return store.options['project'];
    case StoreKind.local:
      return store.options['basepath'];
    case StoreKind.minio:
      return store.options['endpoint'];
    case StoreKind.sftp:
      return store.options['remote_addr'];
    default:
      throw ArgumentError('kind is not recognized');
  }
}

String prettyKind(StoreKind kind) {
  switch (kind) {
    case StoreKind.local:
      return 'local disk';
    case StoreKind.google:
      return 'remote google';
    case StoreKind.minio:
      return 'remote minio';
    case StoreKind.sftp:
      return 'remote SFTP';
    default:
      throw ArgumentError('kind is not recognized');
  }
}
