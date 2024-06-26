//
// Copyright (c) 2024 Nathan Fiedler
//
import 'package:flutter/material.dart';
import 'package:flutter_bloc/flutter_bloc.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:zorigami/core/domain/entities/data_set.dart';
import 'package:zorigami/features/browse/preso/bloc/providers.dart';
import 'package:zorigami/features/browse/preso/bloc/snapshot_browser_bloc.dart';
import 'package:zorigami/features/browse/preso/bloc/tree_browser_bloc.dart'
    as tbb;
import 'package:zorigami/features/browse/preso/widgets/snapshot_viewer.dart';

class SnapshotScreen extends ConsumerWidget {
  // the data set under inspection
  final DataSet dataset;

  const SnapshotScreen({super.key, required this.dataset});

  @override
  Widget build(BuildContext context, WidgetRef ref) {
    return Scaffold(
      appBar: AppBar(
        title: const Text('SNAPSHOTS'),
      ),
      body: MultiBlocProvider(
        providers: [
          BlocProvider<SnapshotBrowserBloc>(
            create: (_) => ref.read(snapshotBrowserBlocProvider),
          ),
          BlocProvider<tbb.TreeBrowserBloc>(
            create: (_) => ref.read(treeBrowserBlocProvider),
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
              return Text('Error getting snapshot: ${state.message}');
            }
            if (state is Loaded) {
              return SnapshotViewer(state: state, dataset: dataset);
            }
            return const Center(child: CircularProgressIndicator());
          },
        ),
      ),
    );
  }
}
