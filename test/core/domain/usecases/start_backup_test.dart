//
// Copyright (c) 2024 Nathan Fiedler
//
import 'package:flutter_test/flutter_test.dart';
import 'package:mocktail/mocktail.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/data_set.dart';
import 'package:zorigami/core/domain/repositories/data_set_repository.dart';
import 'package:zorigami/core/domain/usecases/start_backup.dart';
import 'package:zorigami/core/error/failures.dart';

class MockDataSetRepository extends Mock implements DataSetRepository {}

void main() {
  late StartBackup usecase;
  late MockDataSetRepository mockDataSetRepository;

  setUp(() {
    mockDataSetRepository = MockDataSetRepository();
    usecase = StartBackup(mockDataSetRepository);
  });

  const tDataSet = DataSet(
    key: 'cafebabe',
    computerId: 'data1',
    basepath: '',
    schedules: [],
    packSize: 0,
    stores: [],
    excludes: [],
    snapshot: None(),
    status: Status.none,
    backupState: None(),
    errorMsg: None(),
  );

  setUpAll(() {
    // mocktail needs a fallback for any() that involves custom types
    registerFallbackValue(tDataSet);
  });

  test(
    'should initiate a new backup via the repository',
    () async {
      // arrange
      when(() => mockDataSetRepository.startBackup(any()))
          .thenAnswer((_) async => const Ok<bool, Failure>(true));
      // act
      final result = await usecase(const Params(dataset: tDataSet));
      // assert
      expect(result, equals(const Ok<bool, Failure>(true)));
      verify(() => mockDataSetRepository.startBackup(any()));
      verifyNoMoreInteractions(mockDataSetRepository);
    },
  );
}
