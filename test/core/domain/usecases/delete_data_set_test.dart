//
// Copyright (c) 2019 Nathan Fiedler
//
import 'package:flutter_test/flutter_test.dart';
import 'package:mockito/mockito.dart';
import 'package:oxidized/oxidized.dart';
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

  test(
    'should delete a pack store within the repository',
    () async {
      // arrange
      when(mockDataSetRepository.deleteDataSet(any))
          .thenAnswer((_) async => Result.ok(key));
      // act
      final result = await usecase(Params(key: key));
      // assert
      expect(result, Result.ok(key));
      verify(mockDataSetRepository.deleteDataSet(any));
      verifyNoMoreInteractions(mockDataSetRepository);
    },
  );
}
