//
// Copyright (c) 2020 Nathan Fiedler
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
  Future<Result<DataSet, Failure>> deleteDataSet(DataSet input);

  /// Start the backup process for a dataset.
  Future<Result<bool, Failure>> startBackup(DataSet input);

  /// Stop the running backup for a dataset.
  Future<Result<bool, Failure>> stopBackup(DataSet input);
}
