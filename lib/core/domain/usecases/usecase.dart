//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:equatable/equatable.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/error/failures.dart';

abstract class UseCase<Type extends Object, Params> {
  Future<Result<Type, Failure>> call(Params params);
}

class NoParams extends Equatable {
  @override
  List<Object> get props => [];
}
