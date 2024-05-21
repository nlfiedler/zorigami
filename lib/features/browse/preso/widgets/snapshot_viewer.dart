//
// Copyright (c) 2024 Nathan Fiedler
//
import 'package:flutter/material.dart';
import 'package:flutter_bloc/flutter_bloc.dart';
import 'package:intl/intl.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/data_set.dart';
import 'package:zorigami/features/browse/preso/bloc/snapshot_browser_bloc.dart';
import 'package:zorigami/features/browse/preso/bloc/tree_browser_bloc.dart'
    as tbb;
import 'package:zorigami/features/browse/preso/widgets/tree_viewer.dart';

class SnapshotViewer extends StatelessWidget {
  final Loaded state;
  final DataSet dataset;

  const SnapshotViewer({
    super.key,
    required this.state,
    required this.dataset,
  });

  VoidCallback? loadSubsequent(Loaded state, BuildContext context) {
    return state.hasSubsequent
        ? () {
            BlocProvider.of<SnapshotBrowserBloc>(context).add(LoadSubsequent());
            BlocProvider.of<tbb.TreeBrowserBloc>(context).add(tbb.ResetTree());
          }
        : null;
  }

  VoidCallback? loadParent(Loaded state, BuildContext context) {
    return state.snapshot.parent is Some
        ? () {
            BlocProvider.of<SnapshotBrowserBloc>(context).add(LoadParent());
            BlocProvider.of<tbb.TreeBrowserBloc>(context).add(tbb.ResetTree());
          }
        : null;
  }

  @override
  Widget build(BuildContext context) {
    final digest = state.snapshot.checksum;
    final count = state.snapshot.fileCount;
    final started = DateFormat.yMd().add_jm().format(
          state.snapshot.startTime.toLocal(),
        );
    final status = dataset.describeStatus();
    return Column(
      children: <Widget>[
        Card(
          child: ListTile(
            leading: const Icon(Icons.timeline),
            title: Text('Snapshot: $digest'),
            subtitle: Text(
              'Files: $count, Started: $started, Status: $status',
            ),
            isThreeLine: true,
            trailing: Row(
              mainAxisSize: MainAxisSize.min,
              children: <Widget>[
                ElevatedButton(
                  onPressed: loadSubsequent(state, context),
                  child: const Icon(Icons.chevron_left),
                ),
                ElevatedButton(
                  onPressed: loadParent(state, context),
                  child: const Icon(Icons.chevron_right),
                ),
              ],
            ),
          ),
        ),
        Expanded(
          child: TreeViewer(dataset: dataset, rootTree: state.snapshot.tree),
        ),
      ],
    );
  }
}
