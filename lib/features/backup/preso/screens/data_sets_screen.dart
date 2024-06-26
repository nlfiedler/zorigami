//
// Copyright (c) 2024 Nathan Fiedler
//
import 'package:flutter/material.dart';
import 'package:flutter_bloc/flutter_bloc.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:zorigami/features/backup/preso/screens/new_data_set_screen.dart';
import 'package:zorigami/features/backup/preso/widgets/data_sets_list.dart';
import 'package:zorigami/features/browse/preso/bloc/data_sets_bloc.dart';
import 'package:zorigami/features/browse/preso/bloc/providers.dart';
import 'package:zorigami/navigation_drawer.dart';

class DataSetsScreen extends ConsumerWidget {
  const DataSetsScreen({super.key});

  @override
  Widget build(BuildContext context, WidgetRef ref) {
    return BlocProvider<DataSetsBloc>(
      create: (_) => ref.read(datasetsBlocProvider),
      child: BlocBuilder<DataSetsBloc, DataSetsState>(
        builder: (context, state) {
          return Scaffold(
            appBar: AppBar(
              title: const Text('DATA SETS'),
              actions: <Widget>[
                IconButton(
                  icon: const Icon(Icons.refresh),
                  tooltip: 'Refresh',
                  onPressed: () {
                    BlocProvider.of<DataSetsBloc>(context)
                        .add(ReloadDataSets());
                  },
                ),
              ],
            ),
            body: buildBody(context, state),
            floatingActionButton: FloatingActionButton(
              onPressed: () async {
                final result = await Navigator.push(
                  context,
                  MaterialPageRoute(
                    builder: (_) => const NewDataSetScreen(),
                  ),
                );
                if (result != null && context.mounted) {
                  BlocProvider.of<DataSetsBloc>(context).add(ReloadDataSets());
                }
              },
              child: const Icon(Icons.add),
            ),
            drawer: MyNavigationDrawer(),
          );
        },
      ),
    );
  }

  Widget buildBody(BuildContext context, DataSetsState state) {
    if (state is Empty) {
      // kick off the initial remote request
      BlocProvider.of<DataSetsBloc>(context).add(LoadAllDataSets());
    }
    if (state is Error) {
      return Card(
        child: ListTile(
          title: const Text('Error loading data sets'),
          subtitle: Text(state.message),
        ),
      );
    }
    if (state is Loaded) {
      return DataSetsList(sets: state.sets);
    }
    return const CircularProgressIndicator();
  }
}
