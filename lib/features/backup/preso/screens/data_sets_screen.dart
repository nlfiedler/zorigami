//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:flutter/material.dart';
import 'package:flutter_bloc/flutter_bloc.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:zorigami/features/backup/preso/screens/new_data_set_screen.dart';
import 'package:zorigami/features/backup/preso/widgets/data_sets_list.dart';
import 'package:zorigami/features/browse/preso/bloc/data_sets_bloc.dart';
import 'package:zorigami/features/browse/preso/bloc/providers.dart';
import 'package:zorigami/navigation_drawer.dart';

class DataSetsScreen extends StatelessWidget {
  @override
  Widget build(BuildContext context) {
    return BlocProvider<DataSetsBloc>(
      create: (_) => BuildContextX(context).read(datasetsBlocProvider),
      child: BlocBuilder<DataSetsBloc, DataSetsState>(
        builder: (context, state) {
          return Scaffold(
            appBar: AppBar(
              title: Text('DATA SETS'),
            ),
            body: buildBody(context, state),
            floatingActionButton: FloatingActionButton(
              onPressed: () async {
                final result = await Navigator.push(
                  context,
                  MaterialPageRoute(
                    builder: (_) => NewDataSetScreen(),
                  ),
                );
                if (result != null) {
                  BlocProvider.of<DataSetsBloc>(context).add(ReloadDataSets());
                }
              },
              child: Icon(Icons.add),
            ),
            drawer: NavigationDrawer(),
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
          title: Text('Error loading data sets'),
          subtitle: Text(state.message),
        ),
      );
    }
    if (state is Loaded) {
      return DataSetsList(sets: state.sets);
    }
    return CircularProgressIndicator();
  }
}
