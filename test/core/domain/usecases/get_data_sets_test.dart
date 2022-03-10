//
// Copyright (c) 2022 Nathan Fiedler
//
import 'package:flutter_test/flutter_test.dart';
import 'package:mocktail/mocktail.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/data_set.dart';
import 'package:zorigami/core/domain/repositories/data_set_repository.dart';
import 'package:zorigami/core/domain/usecases/get_data_sets.dart';
import 'package:zorigami/core/domain/usecases/usecase.dart';
import 'package:zorigami/core/error/failures.dart';

class MockDataSetRepository extends Mock implements DataSetRepository {}

void main() {
  late GetDataSets usecase;
  late MockDataSetRepository mockDataSetRepository;

  setUp(() {
    mockDataSetRepository = MockDataSetRepository();
    usecase = GetDataSets(mockDataSetRepository);
  });

  final tDataSet = DataSet(
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
  // annotate the type to assist with matching
  final List<DataSet> tDataSets = List.from([tDataSet]);

  test(
    'should get all data sets from the repository',
    () async {
      // arrange
      when(() => mockDataSetRepository.getAllDataSets())
          .thenAnswer((_) async => Ok<List<DataSet>, Failure>(tDataSets));
      // act
      final result = await usecase(NoParams());
      // assert
      expect(result, equals(Ok<List<DataSet>, Failure>(tDataSets)));
      verify(() => mockDataSetRepository.getAllDataSets());
      verifyNoMoreInteractions(mockDataSetRepository);
    },
  );
}
