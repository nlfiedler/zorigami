//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:zorigami/core/util/input_converter.dart';
import 'package:oxidized/oxidized.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  InputConverter inputConverter;

  setUp(() {
    inputConverter = InputConverter();
  });

  group('stringToUnsignedInteger', () {
    test(
      'should return an integer when the string represents an unsigned integer',
      () async {
        // arrange
        final str = '123';
        // act
        final result = inputConverter.stringToUnsignedInteger(str);
        // assert
        expect(result, Result.ok(123));
      },
    );

    test(
      'should return a Failure when the string is not an integer',
      () async {
        // arrange
        final str = 'abc';
        // act
        final result = inputConverter.stringToUnsignedInteger(str);
        // assert
        expect(result, Result.err(InvalidInputFailure()));
      },
    );

    test(
      'should return a Failure when the string is a negative integer',
      () async {
        // arrange
        final str = '-123';
        // act
        final result = inputConverter.stringToUnsignedInteger(str);
        // assert
        expect(result, Result.err(InvalidInputFailure()));
      },
    );
  });
}
