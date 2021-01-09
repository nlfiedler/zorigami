//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:zorigami/container.dart';
import 'package:zorigami/core/domain/usecases/define_data_set.dart' as cds;
import 'package:zorigami/core/domain/usecases/define_pack_store.dart' as dps;
import 'package:zorigami/core/domain/usecases/delete_data_set.dart' as dds;
import 'package:zorigami/core/domain/usecases/delete_pack_store.dart' as rmps;
import 'package:zorigami/core/domain/usecases/update_data_set.dart' as uds;
import 'package:zorigami/core/domain/usecases/update_pack_store.dart' as ups;
import 'package:zorigami/core/domain/usecases/get_configuration.dart' as gc;
import 'package:zorigami/core/domain/usecases/get_pack_stores.dart' as gps;
import 'package:zorigami/core/domain/usecases/get_data_sets.dart' as gds;
import 'package:zorigami/core/domain/usecases/get_snapshot.dart' as gs;
import 'package:zorigami/core/domain/usecases/get_tree.dart' as gt;
import 'package:zorigami/core/domain/usecases/test_pack_store.dart' as tps;
import 'package:zorigami/core/domain/usecases/restore_file.dart' as rf;

final getConfigurationUsecaseProvider = Provider<gc.GetConfiguration>(
  (ref) => gc.GetConfiguration(
    ref.read(configurationRepositoryProvider),
  ),
);

final getPackStoresUsecaseProvider = Provider<gps.GetPackStores>(
  (ref) => gps.GetPackStores(
    ref.read(packStoreRepositoryProvider),
  ),
);

final createPackStoreUsecaseProvider = Provider<dps.DefinePackStore>(
  (ref) => dps.DefinePackStore(
    ref.read(packStoreRepositoryProvider),
  ),
);

final updatePackStoreUsecaseProvider = Provider<ups.UpdatePackStore>(
  (ref) => ups.UpdatePackStore(
    ref.read(packStoreRepositoryProvider),
  ),
);

final testPackStoreUsecaseProvider = Provider<tps.TestPackStore>(
  (ref) => tps.TestPackStore(
    ref.read(packStoreRepositoryProvider),
  ),
);

final deletePackStoreUsecaseProvider = Provider<rmps.DeletePackStore>(
  (ref) => rmps.DeletePackStore(
    ref.read(packStoreRepositoryProvider),
  ),
);

final getDataSetsUsecaseProvider = Provider<gds.GetDataSets>(
  (ref) => gds.GetDataSets(
    ref.read(datasetRepositoryProvider),
  ),
);

final createDataSetsUsecaseProvider = Provider<cds.DefineDataSet>(
  (ref) => cds.DefineDataSet(
    ref.read(datasetRepositoryProvider),
  ),
);

final updateDataSetUsecaseProvider = Provider<uds.UpdateDataSet>(
  (ref) => uds.UpdateDataSet(
    ref.read(datasetRepositoryProvider),
  ),
);

final deleteDataSetUsecaseProvider = Provider<dds.DeleteDataSet>(
  (ref) => dds.DeleteDataSet(
    ref.read(datasetRepositoryProvider),
  ),
);

final getSnapshotUsecaseProvider = Provider<gs.GetSnapshot>(
  (ref) => gs.GetSnapshot(
    ref.read(snapshotRepositoryProvider),
  ),
);

final getTreeUsecaseProvider = Provider<gt.GetTree>(
  (ref) => gt.GetTree(
    ref.read(treeRepositoryProvider),
  ),
);

final restoreFileUsecaseProvider = Provider<rf.RestoreFile>(
  (ref) => rf.RestoreFile(
    ref.read(snapshotRepositoryProvider),
  ),
);
