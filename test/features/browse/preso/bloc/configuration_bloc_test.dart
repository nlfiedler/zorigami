//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:bloc_test/bloc_test.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:mockito/mockito.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/configuration.dart';
import 'package:zorigami/core/domain/repositories/configuration_repository.dart';
import 'package:zorigami/core/domain/usecases/get_configuration.dart';
import 'package:zorigami/features/browse/preso/bloc/configuration_bloc.dart';

class MockConfigurationRepository extends Mock
    implements ConfigurationRepository {}

void main() {
  MockConfigurationRepository mockConfigurationRepository;
  GetConfiguration usecase;

  final tConfiguration = Configuration(
    hostname: 'localhost',
    username: 'charlie',
    computerId: '1642ceb7-02eb-4ada-94f9-27c14320b908',
  );

  setUp(() {
    mockConfigurationRepository = MockConfigurationRepository();
    usecase = GetConfiguration(mockConfigurationRepository);
    when(mockConfigurationRepository.getConfiguration())
        .thenAnswer((_) async => Ok(tConfiguration));
  });

  group('ConfigurationBloc', () {
    blocTest(
      'emits [] when nothing is added',
      build: () async => ConfigurationBloc(usecase: usecase),
      expect: [],
    );

    blocTest(
      'emits [Loading, Loaded] when LoadConfiguration is added',
      build: () async => ConfigurationBloc(usecase: usecase),
      act: (bloc) => bloc.add(LoadConfiguration()),
      expect: [Loading(), Loaded(config: tConfiguration)],
    );
  });
}
