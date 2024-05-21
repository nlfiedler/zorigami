//
// Copyright (c) 2024 Nathan Fiedler
//
import 'package:zorigami/core/data/models/configuration_model.dart';
import 'package:zorigami/core/domain/entities/configuration.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  group('SnapshotModel', () {
    final jsonSource = {
      'hostname': 'kohaku',
      'username': 'zorigami',
      'computerId': 'r9c7i5l6VFK5Smt8VkBBsQ'
    };
    const tConfigurationModel = ConfigurationModel(
      hostname: 'kohaku',
      username: 'zorigami',
      computerId: 'r9c7i5l6VFK5Smt8VkBBsQ',
    );

    test(
      'should be a subclass of Configuration entity',
      () {
        // assert
        expect(tConfigurationModel, isA<Configuration>());
      },
    );

    test(
      'should convert to and from JSON',
      () {
        expect(
          ConfigurationModel.fromJson(tConfigurationModel.toJson()),
          equals(tConfigurationModel),
        );
        expect(
          ConfigurationModel.fromJson(jsonSource),
          equals(tConfigurationModel),
        );
      },
    );
  });
}
