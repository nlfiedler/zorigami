//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:flutter/material.dart';
import 'package:flutter_bloc/flutter_bloc.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:intl/intl.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/data_set.dart';
import 'package:zorigami/features/browse/preso/bloc/data_sets_bloc.dart';
import 'package:zorigami/features/browse/preso/bloc/providers.dart';
import 'package:zorigami/features/browse/preso/screens/snapshot_screen.dart';

class DataSetsList extends StatelessWidget {
  @override
  Widget build(BuildContext context) {
    return BlocProvider<DataSetsBloc>(
      create: (_) => BuildContextX(context).read(datasetsBlocProvider),
      child: BlocBuilder<DataSetsBloc, DataSetsState>(
        builder: (context, state) {
          if (state is Empty) {
            // kick off the initial remote request
            BlocProvider.of<DataSetsBloc>(context).add(LoadAllDataSets());
          }
          if (state is Error) {
            return Text('Error: ' + state.message);
          }
          if (state is Loaded) {
            return state.sets.isEmpty
                ? buildHelp(context)
                : buildDatasetList(context, state.sets);
          }
          return CircularProgressIndicator();
        },
      ),
    );
  }
}

Widget buildDatasetList(BuildContext context, List<DataSet> sets) {
  final elements = List<Widget>.from(
    sets.map((e) {
      return Card(
        child: ListTile(
          leading: Icon(Icons.dns),
          title: Text(e.basepath + ', runs ' + getSchedule(e)),
          subtitle: Text('Status: ' + getStatus(e)),
          trailing: Icon(Icons.chevron_right),
          onTap: () {
            if (e.snapshot is Some) {
              Navigator.push(
                context,
                MaterialPageRoute(
                  builder: (_) => SnapshotScreen(dataset: e),
                ),
              );
            }
          },
        ),
      );
    }),
  );
  return ListView(children: elements);
}

String getSchedule(DataSet dataset) {
  if (dataset.schedules.isEmpty) {
    return 'manually';
  }
  if (dataset.schedules.length > 1) {
    return 'on multiple schedules';
  }
  return dataset.schedules[0].toPrettyString();
}

String getStatus(DataSet dataset) {
  if (dataset.errorMsg is Some) {
    return dataset.errorMsg.unwrap();
  }
  return dataset.snapshot.mapOrElse(
    (s) => s.endTime.mapOrElse(
      (e) => 'finished at ' + DateFormat.yMd().add_jm().format(e.toLocal()),
      () => 'still running',
    ),
    () => 'not yet run',
  );
}

Widget buildHelp(BuildContext context) {
  return Card(
    child: ListTile(
      leading: Icon(Icons.dns),
      title: Text('No data sets found'),
      subtitle: Text(
        'First configure one or more pack stores, then create a data set using those stores.',
      ),
      trailing: Icon(Icons.chevron_right),
      onTap: () => Navigator.pushNamedAndRemoveUntil(
          context, '/stores', ModalRoute.withName('/')),
    ),
  );
}
