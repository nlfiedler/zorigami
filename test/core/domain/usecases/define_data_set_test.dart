//
// Copyright (c) 2024 Nathan Fiedler
//
import 'package:flutter_test/flutter_test.dart';
import 'package:mocktail/mocktail.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/data_set.dart';
import 'package:zorigami/core/domain/repositories/data_set_repository.dart';
import 'package:zorigami/core/domain/usecases/define_data_set.dart';
import 'package:zorigami/core/error/failures.dart';

class MockDataSetRepository extends Mock implements DataSetRepository {}

void main() {
  late DefineDataSet usecase;
  late MockDataSetRepository mockDataSetRepository;

  setUp(() {
    mockDataSetRepository = MockDataSetRepository();
    usecase = DefineDataSet(mockDataSetRepository);
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
    'should define a data set within the repository',
    () async {
      // arrange
      when(() => mockDataSetRepository.defineDataSet(any()))
          .thenAnswer((_) async => const Ok<DataSet, Failure>(tDataSet));
      // act
      final result = await usecase(const Params(dataset: tDataSet));
      // assert
      expect(result, equals(const Ok<DataSet, Failure>(tDataSet)));
      verify(() => mockDataSetRepository.defineDataSet(any()));
      verifyNoMoreInteractions(mockDataSetRepository);
    },
  );
}
