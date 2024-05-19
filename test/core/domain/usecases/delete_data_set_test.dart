//
// Copyright (c) 2024 Nathan Fiedler
//
import 'package:flutter_test/flutter_test.dart';
import 'package:mocktail/mocktail.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/data_set.dart';
import 'package:zorigami/core/domain/repositories/data_set_repository.dart';
import 'package:zorigami/core/domain/usecases/delete_data_set.dart';
import 'package:zorigami/core/error/failures.dart';

class MockDataSetRepository extends Mock implements DataSetRepository {}

void main() {
  late DeleteDataSet usecase;
  late MockDataSetRepository mockDataSetRepository;

  setUp(() {
    mockDataSetRepository = MockDataSetRepository();
    usecase = DeleteDataSet(mockDataSetRepository);
  });

  const tDataSet = DataSet(
    key: 'cafebabe',
    computerId: 'cray-11',
    basepath: '/home/planet',
    schedules: [],
    stores: ['storytime'],
    excludes: [],
    packSize: 65536,
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
    'should delete a pack store within the repository',
    () async {
      // arrange
      when(() => mockDataSetRepository.deleteDataSet(any()))
          .thenAnswer((_) async => const Ok<DataSet, Failure>(tDataSet));
      // act
      final result = await usecase(const Params(dataset: tDataSet));
      // assert
      expect(result, equals(const Ok<DataSet, Failure>(tDataSet)));
      verify(() => mockDataSetRepository.deleteDataSet(any()));
      verifyNoMoreInteractions(mockDataSetRepository);
    },
  );
}
