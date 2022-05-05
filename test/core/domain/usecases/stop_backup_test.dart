//
// Copyright (c) 2022 Nathan Fiedler
//
import 'package:flutter_test/flutter_test.dart';
import 'package:mocktail/mocktail.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/data_set.dart';
import 'package:zorigami/core/domain/repositories/data_set_repository.dart';
import 'package:zorigami/core/domain/usecases/stop_backup.dart';
import 'package:zorigami/core/error/failures.dart';

class MockDataSetRepository extends Mock implements DataSetRepository {}

void main() {
  late StopBackup usecase;
  late MockDataSetRepository mockDataSetRepository;

  setUp(() {
    mockDataSetRepository = MockDataSetRepository();
    usecase = StopBackup(mockDataSetRepository);
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
    errorMsg: None(),
  );

  setUpAll(() {
    // mocktail needs a fallback for any() that involves custom types
    registerFallbackValue(tDataSet);
  });

  test(
    'should terminate a running backup via the repository',
    () async {
      // arrange
      when(() => mockDataSetRepository.stopBackup(any()))
          .thenAnswer((_) async => Ok<bool, Failure>(true));
      // act
      final result = await usecase(const Params(dataset: tDataSet));
      // assert
      expect(result, equals(Ok<bool, Failure>(true)));
      verify(() => mockDataSetRepository.stopBackup(any()));
      verifyNoMoreInteractions(mockDataSetRepository);
    },
  );
}
