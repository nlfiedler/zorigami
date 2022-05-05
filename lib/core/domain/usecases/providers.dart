//
// Copyright (c) 2022 Nathan Fiedler
//
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:zorigami/container.dart';
import 'package:zorigami/core/domain/usecases/cancel_restore.dart' as cr;
import 'package:zorigami/core/domain/usecases/define_data_set.dart' as cds;
import 'package:zorigami/core/domain/usecases/define_pack_store.dart' as dps;
import 'package:zorigami/core/domain/usecases/delete_data_set.dart' as dds;
import 'package:zorigami/core/domain/usecases/delete_pack_store.dart' as rmps;
import 'package:zorigami/core/domain/usecases/update_data_set.dart' as uds;
import 'package:zorigami/core/domain/usecases/update_pack_store.dart' as ups;
import 'package:zorigami/core/domain/usecases/get_configuration.dart' as gc;
import 'package:zorigami/core/domain/usecases/get_pack_stores.dart' as gps;
import 'package:zorigami/core/domain/usecases/get_data_sets.dart' as gds;
import 'package:zorigami/core/domain/usecases/get_restores.dart' as gr;
import 'package:zorigami/core/domain/usecases/get_snapshot.dart' as gs;
import 'package:zorigami/core/domain/usecases/get_tree.dart' as gt;
import 'package:zorigami/core/domain/usecases/start_backup.dart' as start;
import 'package:zorigami/core/domain/usecases/stop_backup.dart' as stop;
import 'package:zorigami/core/domain/usecases/test_pack_store.dart' as tps;
import 'package:zorigami/core/domain/usecases/restore_database.dart' as rd;
import 'package:zorigami/core/domain/usecases/restore_files.dart' as rf;

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

final restoreDatabaseUsecaseProvider = Provider<rd.RestoreDatabase>(
  (ref) => rd.RestoreDatabase(
    ref.read(snapshotRepositoryProvider),
  ),
);

final restoreFilesUsecaseProvider = Provider<rf.RestoreFiles>(
  (ref) => rf.RestoreFiles(
    ref.read(snapshotRepositoryProvider),
  ),
);

final getRestoresUsecaseProvider = Provider<gr.GetRestores>(
  (ref) => gr.GetRestores(
    ref.read(snapshotRepositoryProvider),
  ),
);

final cancelRestoreUsecaseProvider = Provider<cr.CancelRestore>(
  (ref) => cr.CancelRestore(
    ref.read(snapshotRepositoryProvider),
  ),
);

final startBackupUsecaseProvider = Provider<start.StartBackup>(
  (ref) => start.StartBackup(
    ref.read(datasetRepositoryProvider),
  ),
);

final stopBackupUsecaseProvider = Provider<stop.StopBackup>(
  (ref) => stop.StopBackup(
    ref.read(datasetRepositoryProvider),
  ),
);
