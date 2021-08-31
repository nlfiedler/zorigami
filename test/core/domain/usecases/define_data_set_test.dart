//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:flutter_test/flutter_test.dart';
import 'package:mockito/annotations.dart';
import 'package:mockito/mockito.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/data_set.dart';
import 'package:zorigami/core/domain/repositories/data_set_repository.dart';
import 'package:zorigami/core/domain/usecases/define_data_set.dart';
import './define_data_set_test.mocks.dart';

@GenerateMocks([DataSetRepository])
void main() {
  late DefineDataSet usecase;
  late MockDataSetRepository mockDataSetRepository;

  setUp(() {
    mockDataSetRepository = MockDataSetRepository();
    usecase = DefineDataSet(mockDataSetRepository);
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

  test(
    'should define a data set within the repository',
    () async {
      // arrange
      when(mockDataSetRepository.defineDataSet(any))
          .thenAnswer((_) async => Ok(tDataSet));
      // act
      final result = await usecase(Params(dataset: tDataSet));
      // assert
      expect(result, Ok(tDataSet));
      verify(mockDataSetRepository.defineDataSet(any));
      verifyNoMoreInteractions(mockDataSetRepository);
    },
  );
}
