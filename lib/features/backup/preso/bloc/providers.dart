//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:zorigami/core/domain/usecases/providers.dart';
import 'package:zorigami/features/backup/preso/bloc/create_data_sets_bloc.dart';
import 'package:zorigami/features/backup/preso/bloc/create_pack_stores_bloc.dart';
import 'package:zorigami/features/backup/preso/bloc/edit_pack_stores_bloc.dart';
import 'package:zorigami/features/backup/preso/bloc/edit_data_sets_bloc.dart';
import 'package:zorigami/features/backup/preso/bloc/pack_stores_bloc.dart';

final createDatasetsBlocProvider = Provider.autoDispose<CreateDataSetsBloc>(
  (ref) => CreateDataSetsBloc(
    usecase: ref.read(createDataSetsUsecaseProvider),
  ),
);

final editDataSetsBlocProvider = Provider.autoDispose<EditDataSetsBloc>(
  (ref) => EditDataSetsBloc(
    updateDataSet: ref.read(updateDataSetUsecaseProvider),
    deleteDataSet: ref.read(deleteDataSetUsecaseProvider),
  ),
);

final packStoresBlocProvider = Provider.autoDispose<PackStoresBloc>(
  (ref) => PackStoresBloc(
    usecase: ref.read(getPackStoresUsecaseProvider),
  ),
);

final createPackStoresBlocProvider = Provider.autoDispose<CreatePackStoresBloc>(
  (ref) => CreatePackStoresBloc(
    usecase: ref.read(createPackStoreUsecaseProvider),
  ),
);

final editPackStoresBlocProvider = Provider.autoDispose<EditPackStoresBloc>(
  (ref) => EditPackStoresBloc(
    updatePackStore: ref.read(updatePackStoreUsecaseProvider),
    deletePackStore: ref.read(deletePackStoreUsecaseProvider),
  ),
);
