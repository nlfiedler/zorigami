//
// Copyright (c) 2019 Nathan Fiedler
//
import 'package:equatable/equatable.dart';
import 'package:meta/meta.dart';

enum StoreKind { local, minio, sftp }

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
    @required this.key,
    @required this.label,
    @required this.kind,
    @required this.options,
  });

  @override
  List<Object> get props => [key, label, kind, options];
}
