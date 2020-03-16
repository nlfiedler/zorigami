//
// Copyright (c) 2019 Nathan Fiedler
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

  final key = 'cafebabe';
  final tDataSet = DataSet(
    key: 'cafebabe',
    computerId: 'cray-11',
    basepath: '/home/planet',
    schedules: [],
    stores: ['storytime'],
    packSize: 65536,
    snapshot: Option.none(),
  );

  test(
    'should delete a pack store within the repository',
    () async {
      // arrange
      when(mockDataSetRepository.deleteDataSet(any))
          .thenAnswer((_) async => Result.ok(tDataSet));
      // act
      final result = await usecase(Params(key: key));
      // assert
      expect(result, Result.ok(tDataSet));
      verify(mockDataSetRepository.deleteDataSet(any));
      verifyNoMoreInteractions(mockDataSetRepository);
    },
  );
}
