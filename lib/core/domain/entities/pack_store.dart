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

  /// JSON encoded options for configuring this pack store.
  final String options;

  PackStore({
    @required this.key,
    @required this.label,
    @required this.kind,
    @required this.options,
  });

  @override
  List<Object> get props => [key];
}
