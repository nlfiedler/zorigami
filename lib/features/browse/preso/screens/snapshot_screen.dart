//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:flutter/material.dart';
import 'package:flutter_bloc/flutter_bloc.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/container.dart';
import 'package:zorigami/core/domain/entities/data_set.dart';
import 'package:zorigami/features/browse/preso/bloc/snapshot_browser_bloc.dart';

class SnapshotScreen extends StatelessWidget {
  // the data set under inspection
  final DataSet dataset;

  SnapshotScreen({Key key, @required this.dataset}) : super(key: key);

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: Text('SNAPSHOTS'),
      ),
      body: BlocProvider<SnapshotBrowserBloc>(
        create: (_) => getIt<SnapshotBrowserBloc>(),
        child: BlocBuilder<SnapshotBrowserBloc, SnapshotBrowserState>(
          builder: (context, state) {
            if (state is Empty) {
              // kick off the initial remote request
              BlocProvider.of<SnapshotBrowserBloc>(context).add(
                LoadSnapshot(digest: dataset.snapshot.unwrap().checksum),
              );
              return Text('Starting...');
            }
            if (state is Error) {
              return Text('Error: ' + state.message);
            }
            if (state is Loaded) {
              return SnapshotViewer(state: state);
            }
            return Text('Loading...');
          },
        ),
      ),
    );
  }
}

class SnapshotViewer extends StatelessWidget {
  final Loaded state;

  SnapshotViewer({Key key, @required this.state}) : super(key: key);

  @override
  Widget build(BuildContext context) {
    final digest = state.snapshot.checksum;
    final hasParent = state.snapshot.parent is Some;
    final count = state.snapshot.fileCount;
    final started = state.snapshot.startTime.toLocal();
    final ended = state.snapshot.endTime.mapOrElse(
      (e) => e.toLocal().toString(),
      () => 'running...',
    );
    return Column(
      children: <Widget>[
        Row(
          children: <Widget>[
            RaisedButton(
              child: Icon(Icons.chevron_left),
              onPressed: state.hasSubsequent
                  ? () {
                      BlocProvider.of<SnapshotBrowserBloc>(context).add(
                        LoadSubsequent(),
                      );
                    }
                  : null,
            ),
            RaisedButton(
              child: Icon(Icons.chevron_right),
              onPressed: hasParent
                  ? () {
                      BlocProvider.of<SnapshotBrowserBloc>(context).add(
                        LoadParent(),
                      );
                    }
                  : null,
            ),
          ],
        ),
        Card(
          child: ListTile(
            leading: Icon(Icons.history),
            title: Text('Snapshot: ${digest}'),
            subtitle: Text(
              'Files: ${count}, Started: ${started}, Finished: ${ended}',
            ),
          ),
        ),
      ],
    );
  }
}

// tree browser bloc logic:
// --> start
// 1) create tree_browser_bloc above the scaffold so actions can interact with it
// 2) get the tree via tree_bloc
//
// --> onTap for "tree" entry
// 1) fire VisitTree event to tree_browser_bloc
// 2) how does the tree get loaded?
//
// --> onTap for "file" entry
// 1) fire ToggleSelection event to tree_browser_bloc
// 2) set `selected` parameter of the DataRow based on bloc state
//
// --> appbar action navigate-upward
// 1) fire NavigateUpward event to tree_browser_bloc
//
// --> appbar action restore-selection(s)
// 1) already has the tree_browser_bloc state available, uses that to get selections
//
// --> snapshot navigation (eventually)
// 1) fire StartNewTree event to tree_browser_bloc
// 2) how does the tree get loaded?

// tree viewer:
// TODO: build a DataTable based on the entries in the tree
// TODO: use file/folder icons in front of the entry names
// TODO: show the date in concise form after the entry name
// TODO: table rows have onTap that fires VisitTree event
// TODO: show the currently viewed path above the table as breadcrumbs
// TODO: tree browser widget rebuilds as bloc changes
//       (use `condition` property of `BlocBuilder` to control whether to rebuild widgets)
// TODO: clicking on a file entry will marked it as selected
//       maybe use a radio/check button to make the selection clear
//       otherwise you can't click on a folder and restore its contents
// TODO: appbar action to restore selected file(s)
// TODO: appbar action to navigate upward to parent tree (if any)
