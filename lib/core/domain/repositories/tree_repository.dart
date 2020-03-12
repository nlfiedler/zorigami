//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/tree.dart';
import 'package:zorigami/core/error/failures.dart';

abstract class TreeRepository {
  /// Retrieve a tree by its hash digset.
  Future<Result<Tree, Failure>> getTree(String checksum);
}
