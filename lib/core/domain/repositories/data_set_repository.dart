//
// Copyright (c) 2019 Nathan Fiedler
//
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/data_set.dart';
import 'package:zorigami/core/error/failures.dart';

abstract class DataSetRepository {
  /// Retrieve all data sets.
  Future<Result<List<DataSet>, Failure>> getAllDataSets();

  /// Define a new data set.
  Future<Result<DataSet, Failure>> defineDataSet(DataSet input);

  /// Update an existing data set.
  Future<Result<DataSet, Failure>> updateDataSet(DataSet input);

  /// Remove a data set.
  Future<Result<DataSet, Failure>> deleteDataSet(String key);
}
