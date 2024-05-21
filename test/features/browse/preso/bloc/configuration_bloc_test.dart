//
// Copyright (c) 2024 Nathan Fiedler
//
import 'package:bloc_test/bloc_test.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:mocktail/mocktail.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/configuration.dart';
import 'package:zorigami/core/domain/repositories/configuration_repository.dart';
import 'package:zorigami/core/domain/usecases/get_configuration.dart';
import 'package:zorigami/core/error/failures.dart';
import 'package:zorigami/features/browse/preso/bloc/configuration_bloc.dart';

class MockConfigurationRepository extends Mock
    implements ConfigurationRepository {}

void main() {
  late MockConfigurationRepository mockConfigurationRepository;
  late GetConfiguration usecase;

  const tConfiguration = Configuration(
    hostname: 'localhost',
    username: 'charlie',
    computerId: '1642ceb7-02eb-4ada-94f9-27c14320b908',
  );

  group('normal cases', () {
    setUp(() {
      mockConfigurationRepository = MockConfigurationRepository();
      usecase = GetConfiguration(mockConfigurationRepository);
      when(() => mockConfigurationRepository.getConfiguration())
          .thenAnswer((_) async => const Ok(tConfiguration));
    });

    blocTest(
      'emits [] when nothing is added',
      build: () => ConfigurationBloc(usecase: usecase),
      expect: () => [],
    );

    blocTest(
      'emits [Loading, Loaded] when LoadConfiguration is added',
      build: () => ConfigurationBloc(usecase: usecase),
      act: (ConfigurationBloc bloc) => bloc.add(LoadConfiguration()),
      expect: () => [Loading(), Loaded(config: tConfiguration)],
    );
  });

  group('error cases', () {
    setUp(() {
      mockConfigurationRepository = MockConfigurationRepository();
      usecase = GetConfiguration(mockConfigurationRepository);
      when(() => mockConfigurationRepository.getConfiguration())
          .thenAnswer((_) async => const Err(ServerFailure('oh no!')));
    });

    blocTest(
      'emits [Loading, Error] when LoadConfiguration is added',
      build: () => ConfigurationBloc(usecase: usecase),
      act: (ConfigurationBloc bloc) => bloc.add(LoadConfiguration()),
      expect: () => [Loading(), Error(message: 'ServerFailure(oh no!)')],
    );
  });
}
