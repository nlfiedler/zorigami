//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:flutter_test/flutter_test.dart';
import 'package:mockito/mockito.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/data_set.dart';
import 'package:zorigami/core/domain/repositories/data_set_repository.dart';
import 'package:zorigami/core/domain/usecases/delete_data_set.dart';

class MockDataSetRepository extends Mock implements DataSetRepository {}

void main() {
  DeleteDataSet usecase;
  MockDataSetRepository mockDataSetRepository;

  setUp(() {
    mockDataSetRepository = MockDataSetRepository();
    usecase = DeleteDataSet(mockDataSetRepository);
  });

  final tDataSet = DataSet(
    key: 'cafebabe',
    computerId: 'cray-11',
    basepath: '/home/planet',
    schedules: [],
    stores: ['storytime'],
    packSize: 65536,
    snapshot: None(),
    errorMsg: None(),
  );

  test(
    'should delete a pack store within the repository',
    () async {
      // arrange
      when(mockDataSetRepository.deleteDataSet(any))
          .thenAnswer((_) async => Ok(tDataSet));
      // act
      final result = await usecase(Params(dataset: tDataSet));
      // assert
      expect(result, Ok(tDataSet));
      verify(mockDataSetRepository.deleteDataSet(any));
      verifyNoMoreInteractions(mockDataSetRepository);
    },
  );
}
