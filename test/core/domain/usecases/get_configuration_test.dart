//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:flutter_test/flutter_test.dart';
import 'package:mockito/mockito.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/configuration.dart';
import 'package:zorigami/core/domain/repositories/configuration_repository.dart';
import 'package:zorigami/core/domain/usecases/get_configuration.dart';
import 'package:zorigami/core/usecases/usecase.dart';

class MockConfigurationRepository extends Mock implements ConfigurationRepository {}

void main() {
  GetConfiguration usecase;
  MockConfigurationRepository mockConfigurationRepository;

  setUp(() {
    mockConfigurationRepository = MockConfigurationRepository();
    usecase = GetConfiguration(mockConfigurationRepository);
  });

  final tConfiguration = Configuration(
    hostname: 'localhost',
    username: 'charlie',
    computerId: '1642ceb7-02eb-4ada-94f9-27c14320b908',
  );

  test(
    'should get the configuration from the repository',
    () async {
      // arrange
      when(mockConfigurationRepository.getConfiguration())
          .thenAnswer((_) async => Ok(tConfiguration));
      // act
      final result = await usecase(NoParams());
      // assert
      expect(result, Ok(tConfiguration));
      verify(mockConfigurationRepository.getConfiguration());
      verifyNoMoreInteractions(mockConfigurationRepository);
    },
  );
}
