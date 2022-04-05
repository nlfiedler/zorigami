//
// Copyright (c) 2022 Nathan Fiedler
//
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/data/models/request_model.dart';
import 'package:zorigami/core/domain/entities/request.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  group('RequestModel', () {
    const tRequestModel = RequestModel(
      tree: 'sha1-cafebabe',
      entry: 'file',
      filepath: 'dir/file',
      dataset: 'data123',
      finished: None(),
      filesRestored: 123,
      errorMessage: None(),
    );
    test(
      'should be a subclass of Request entity',
      () {
        // assert
        expect(tRequestModel, isA<Request>());
      },
    );

    test(
      'should convert to and from JSON',
      () {
        expect(
          RequestModel.fromJson(tRequestModel.toJson()),
          equals(tRequestModel),
        );
        final actual = RequestModel(
          tree: 'sha1-cafebabe',
          entry: 'file',
          filepath: 'dir/file',
          dataset: 'data123',
          finished: Some(DateTime.now()),
          filesRestored: 1234567890,
          errorMessage: Some('oh noes'),
        );
        final encoded = actual.toJson();
        final decoded = RequestModel.fromJson(encoded);
        expect(decoded, equals(actual));
        // compare everything else not listed in props
        expect(decoded.dataset, equals(actual.dataset));
        expect(decoded.finished, equals(actual.finished));
        expect(decoded.filesRestored, equals(actual.filesRestored));
        expect(decoded.errorMessage, equals(actual.errorMessage));
      },
    );
  });
}
