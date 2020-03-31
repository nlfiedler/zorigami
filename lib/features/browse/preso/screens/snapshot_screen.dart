//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:flutter/material.dart';
import 'package:flutter_bloc/flutter_bloc.dart';
import 'package:zorigami/container.dart';
import 'package:zorigami/core/domain/entities/data_set.dart';
import 'package:zorigami/features/browse/preso/bloc/snapshot_browser_bloc.dart';
import 'package:zorigami/features/browse/preso/bloc/tree_browser_bloc.dart'
    as tb;
import 'package:zorigami/features/browse/preso/widgets/snapshot_viewer.dart';

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
      body: MultiBlocProvider(
        providers: [
          BlocProvider<SnapshotBrowserBloc>(
            create: (_) => getIt<SnapshotBrowserBloc>(),
          ),
          BlocProvider<tb.TreeBrowserBloc>(
            create: (_) => getIt<tb.TreeBrowserBloc>(),
          ),
        ],
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
