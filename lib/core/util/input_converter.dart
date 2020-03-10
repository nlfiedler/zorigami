//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/error/failures.dart';

class InputConverter {
  Result<int, Failure> stringToUnsignedInteger(String str) {
    try {
      final integer = int.parse(str);
      if (integer < 0) throw FormatException();
      return Result.ok(integer);
    } on FormatException {
      return Result.err(InvalidInputFailure());
    }
  }
}

class InvalidInputFailure extends Failure {}
