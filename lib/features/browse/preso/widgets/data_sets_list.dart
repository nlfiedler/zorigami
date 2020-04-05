//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:flutter/material.dart';
import 'package:flutter_bloc/flutter_bloc.dart';
import 'package:intl/intl.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/container.dart';
import 'package:zorigami/core/domain/entities/data_set.dart';
import 'package:zorigami/features/browse/preso/bloc/datasets_bloc.dart';
import 'package:zorigami/features/browse/preso/screens/snapshot_screen.dart';

class DataSetsList extends StatelessWidget {
  @override
  Widget build(BuildContext context) {
    return BlocProvider<DatasetsBloc>(
      create: (_) => getIt<DatasetsBloc>(),
      child: BlocBuilder<DatasetsBloc, DatasetsState>(
        builder: (context, state) {
          if (state is Empty) {
            // kick off the initial remote request
            BlocProvider.of<DatasetsBloc>(context).add(LoadAllDataSets());
            return Text('Starting...');
          }
          if (state is Error) {
            return Text('Error: ' + state.message);
          }
          if (state is Loaded) {
            final elements = List<Widget>.from(
              state.sets.map((e) {
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
          return Text('Loading...');
        },
      ),
    );
  }
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
  return dataset.snapshot.mapOrElse(
    (s) => s.endTime.mapOrElse(
      (e) => 'finished at ' + DateFormat.yMd().add_jm().format(e.toLocal()),
      () => 'still running',
    ),
    () => 'not yet run',
  );
}
