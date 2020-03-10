//
// Copyright (c) 2019 Nathan Fiedler
//
import 'package:flutter_test/flutter_test.dart';
import 'package:mockito/mockito.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/data_set.dart';
import 'package:zorigami/core/domain/repositories/data_set_repository.dart';
import 'package:zorigami/core/domain/usecases/get_data_sets.dart';
import 'package:zorigami/core/usecases/usecase.dart';

class MockDataSetRepository extends Mock implements DataSetRepository {}

void main() {
  GetDataSets usecase;
  MockDataSetRepository mockDataSetRepository;

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
      stores: []);
  // annotate the type to assist with matching
  final List<DataSet> tDataSets = List.from([tDataSet]);

  test(
    'should get all data sets from the repository',
    () async {
      // arrange
      when(mockDataSetRepository.getDataSets())
          .thenAnswer((_) async => Result.ok(tDataSets));
      // act
      final result = await usecase(NoParams());
      // assert
      expect(result, Result.ok(tDataSets));
      verify(mockDataSetRepository.getDataSets());
      verifyNoMoreInteractions(mockDataSetRepository);
    },
  );
}
