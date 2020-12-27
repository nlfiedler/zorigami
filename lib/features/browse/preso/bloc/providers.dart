//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:zorigami/core/domain/usecases/providers.dart';
import 'package:zorigami/features/browse/preso/bloc/configuration_bloc.dart';
import 'package:zorigami/features/browse/preso/bloc/data_sets_bloc.dart';
import 'package:zorigami/features/browse/preso/bloc/snapshot_browser_bloc.dart';
import 'package:zorigami/features/browse/preso/bloc/tree_browser_bloc.dart';

final configurationBlocProvider = Provider.autoDispose<ConfigurationBloc>(
  (ref) => ConfigurationBloc(
    usecase: ref.read(getConfigurationUsecaseProvider),
  ),
);

final datasetsBlocProvider = Provider.autoDispose<DataSetsBloc>(
  (ref) => DataSetsBloc(
    usecase: ref.read(getDataSetsUsecaseProvider),
  ),
);

final snapshotBrowserBlocProvider = Provider.autoDispose<SnapshotBrowserBloc>(
  (ref) => SnapshotBrowserBloc(
    usecase: ref.read(getSnapshotUsecaseProvider),
  ),
);

final treeBrowserBlocProvider = Provider.autoDispose<TreeBrowserBloc>(
  (ref) => TreeBrowserBloc(
    getTree: ref.read(getTreeUsecaseProvider),
    restoreFile: ref.read(restoreFileUsecaseProvider),
  ),
);
