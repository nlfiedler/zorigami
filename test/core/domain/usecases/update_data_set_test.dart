//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:flutter_test/flutter_test.dart';
import 'package:mockito/annotations.dart';
import 'package:mockito/mockito.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/data_set.dart';
import 'package:zorigami/core/domain/repositories/data_set_repository.dart';
import 'package:zorigami/core/domain/usecases/update_data_set.dart';
import './update_data_set_test.mocks.dart';

@GenerateMocks([DataSetRepository])
void main() {
  late UpdateDataSet usecase;
  late MockDataSetRepository mockDataSetRepository;

  setUp(() {
    mockDataSetRepository = MockDataSetRepository();
    usecase = UpdateDataSet(mockDataSetRepository);
  });

  final tDataSet = DataSet(
    key: 'cafebabe',
    computerId: 'data1',
    basepath: '',
    schedules: [],
    packSize: 0,
    stores: [],
    snapshot: None(),
    status: Status.none,
    errorMsg: None(),
  );

  test(
    'should update an existing pack store within the repository',
    () async {
      // arrange
      when(mockDataSetRepository.updateDataSet(any))
          .thenAnswer((_) async => Ok(tDataSet));
      // act
      final result = await usecase(Params(dataset: tDataSet));
      // assert
      expect(result, Ok(tDataSet));
      verify(mockDataSetRepository.updateDataSet(any));
      verifyNoMoreInteractions(mockDataSetRepository);
    },
  );
}
