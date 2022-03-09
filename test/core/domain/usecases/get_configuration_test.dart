//
// Copyright (c) 2022 Nathan Fiedler
//
import 'package:flutter_test/flutter_test.dart';
import 'package:mocktail/mocktail.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/configuration.dart';
import 'package:zorigami/core/domain/repositories/configuration_repository.dart';
import 'package:zorigami/core/domain/usecases/get_configuration.dart';
import 'package:zorigami/core/domain/usecases/usecase.dart';

class MockConfigurationRepository extends Mock
    implements ConfigurationRepository {}

void main() {
  late GetConfiguration usecase;
  late MockConfigurationRepository mockConfigurationRepository;

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
      when(() => mockConfigurationRepository.getConfiguration())
          .thenAnswer((_) async => Ok(tConfiguration));
      // act
      final result = await usecase(NoParams());
      // assert
      expect(result, Ok(tConfiguration));
      verify(() => mockConfigurationRepository.getConfiguration());
      verifyNoMoreInteractions(mockConfigurationRepository);
    },
  );
}
