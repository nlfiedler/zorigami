//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:flutter_test/flutter_test.dart';
import 'package:mockito/annotations.dart';
import 'package:mockito/mockito.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/data_set.dart';
import 'package:zorigami/core/domain/repositories/data_set_repository.dart';
import 'package:zorigami/core/domain/usecases/delete_data_set.dart';
import './delete_data_set_test.mocks.dart';

@GenerateMocks([DataSetRepository])
void main() {
  late DeleteDataSet usecase;
  late MockDataSetRepository mockDataSetRepository;

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
    excludes: [],
    packSize: 65536,
    snapshot: None(),
    status: Status.none,
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
