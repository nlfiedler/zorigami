//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:flutter/material.dart';
import 'package:flutter_bloc/flutter_bloc.dart';
import 'package:provider/provider.dart';
import 'package:zorigami/container.dart';
import 'package:zorigami/core/domain/entities/data_set.dart';
import 'package:zorigami/features/browse/preso/bloc/snapshot_browser_bloc.dart';
import 'package:zorigami/features/browse/preso/bloc/tree_browser_bloc.dart'
    as tbb;
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
          BlocProvider<tbb.TreeBrowserBloc>(
            create: (_) => getIt<tbb.TreeBrowserBloc>(),
          ),
        ],
        child: BlocBuilder<SnapshotBrowserBloc, SnapshotBrowserState>(
          builder: (context, state) {
            if (state is Empty) {
              // kick off the initial remote request
              BlocProvider.of<SnapshotBrowserBloc>(context).add(
                LoadSnapshot(digest: dataset.snapshot.unwrap().checksum),
              );
            }
            if (state is Error) {
              return Text('Error getting snapshot: ' + state.message);
            }
            if (state is Loaded) {
              return Provider<DataSet>.value(
                value: dataset,
                child: SnapshotViewer(state: state),
              );
            }
            return CircularProgressIndicator();
          },
        ),
      ),
    );
  }
}
