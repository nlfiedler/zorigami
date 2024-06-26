//
// Copyright (c) 2024 Nathan Fiedler
//
import 'dart:convert';
import 'package:zorigami/core/data/models/pack_store_model.dart';
import 'package:zorigami/core/domain/entities/pack_store.dart';
import 'package:flutter_test/flutter_test.dart';
import '../../../fixtures/fixture_reader.dart';

void main() {
  const tPackStoreModel = PackStoreModel(
    key: '123',
    label: 'Label',
    kind: StoreKind.local,
    options: {'basepath': '/home/users'},
  );

  test(
    'should be a subclass of PackStore entity',
    () async {
      // assert
      expect(tPackStoreModel, isA<PackStore>());
    },
  );

  group('fromJson', () {
    test(
      'should return a valid pack store when the JSON is valid',
      () async {
        // arrange
        final Map<String, dynamic> jsonMap =
            json.decode(fixture('pack_store_local.json'));
        // act
        final result = PackStoreModel.fromJson(jsonMap);
        // assert
        expect(result, tPackStoreModel);
      },
    );

    test(
      'should raise an error when store kind is unrecognized',
      () async {
        // arrange
        final Map<String, dynamic> jsonMap =
            json.decode(fixture('pack_store_bad_kind.json'));
        // assert
        fn() => PackStoreModel.fromJson(jsonMap);
        expect(fn, throwsArgumentError);
      },
    );
  });

  group('toJson', () {
    test(
      'should return a JSON map containing the proper data',
      () async {
        // act
        final result = tPackStoreModel.toJson();
        // assert
        final expectedMap = {
          'id': '123',
          'label': 'Label',
          'storeType': 'local',
          'properties': [
            {
              'name': 'basepath',
              'value': '/home/users',
            }
          ]
        };
        expect(result, expectedMap);
      },
    );
  });

  group('toJson and then fromJson', () {
    test('should convert all non-null options', () {
      // arrange
      const model = PackStoreModel(
        key: 'abc123',
        label: 'MyLabel',
        kind: StoreKind.local,
        options: {'basepath': '/home/planet'},
      );
      // act
      final result = PackStoreModel.fromJson(model.toJson());
      // assert
      expect(result, equals(model));
    });

    test('should convert some null options', () {
      // arrange
      const model = PackStoreModel(
        key: 'sftp321',
        label: 'SecureFTP',
        kind: StoreKind.sftp,
        options: {
          'remote_addr': '192.168.1.1',
          'username': 'charlie',
          'password': null,
          'basepath': null,
        },
      );
      // act
      final result = PackStoreModel.fromJson(model.toJson());
      // assert
      expect(result, equals(model));
    });
  });
}
