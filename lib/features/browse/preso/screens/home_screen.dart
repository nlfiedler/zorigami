//
// Copyright (c) 2024 Nathan Fiedler
//
import 'package:flutter/material.dart';
import 'package:flutter_bloc/flutter_bloc.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/data_set.dart';
import 'package:zorigami/features/browse/preso/bloc/data_sets_bloc.dart';
import 'package:zorigami/features/browse/preso/bloc/providers.dart';
import 'package:zorigami/features/browse/preso/screens/snapshot_screen.dart';
import 'package:zorigami/navigation_drawer.dart';

// ignore: use_key_in_widget_constructors
class HomeScreen extends ConsumerWidget {
  @override
  Widget build(BuildContext context, WidgetRef ref) {
    return BlocProvider<DataSetsBloc>(
      create: (_) => ref.read(datasetsBlocProvider),
      child: BlocBuilder<DataSetsBloc, DataSetsState>(
        builder: (context, state) {
          return Scaffold(
            appBar: AppBar(
              title: const Text('ZORIGAMI'),
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
            drawer: MyNavigationDrawer(),
          );
        },
      ),
    );
  }
}

Widget buildBody(BuildContext context, DataSetsState state) {
  if (state is Empty) {
    // kick off the initial remote request
    BlocProvider.of<DataSetsBloc>(context).add(LoadAllDataSets());
  }
  if (state is Error) {
    return Text('Error: ${state.message}');
  }
  if (state is Loaded) {
    return state.sets.isEmpty
        ? buildHelp(context)
        : buildDatasetList(context, state.sets);
  }
  return const CircularProgressIndicator();
}

Widget buildDatasetList(BuildContext context, List<DataSet> sets) {
  final elements = List<Widget>.from(
    sets.map((e) {
      return Card(
        child: ListTile(
          leading: backupButton(context, e),
          title: Text('${e.basepath}, runs ${getSchedule(e)}'),
          subtitle: Text('Status: ${e.describeStatus()}'),
          trailing: e.snapshot is Some ? const Icon(Icons.chevron_right) : null,
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

Widget backupButton(BuildContext context, DataSet dataset) {
  if (dataset.status == Status.running) {
    return ElevatedButton.icon(
      icon: const Icon(Icons.cancel),
      label: const Text('Stop backup'),
      onPressed: () {
        BlocProvider.of<DataSetsBloc>(context)
            .add(StopBackup(dataset: dataset));
      },
    );
  } else {
    return ElevatedButton.icon(
      icon: const Icon(Icons.backup),
      label: const Text('Start backup'),
      onPressed: () {
        BlocProvider.of<DataSetsBloc>(context)
            .add(StartBackup(dataset: dataset));
      },
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

Widget buildHelp(BuildContext context) {
  return ListView(
    children: [
      Card(
        child: ListTile(
          leading: const Icon(Icons.dns),
          title: const Text('Create Pack Store'),
          subtitle: const Text(
            'Click here to create a pack store',
          ),
          trailing: const Icon(Icons.chevron_right),
          onTap: () => Navigator.pushNamedAndRemoveUntil(
              context, '/stores', ModalRoute.withName('/')),
        ),
      ),
      Card(
        child: ListTile(
          leading: const Icon(Icons.dns),
          title: const Text('Restore Database'),
          subtitle: const Text(
            'Click here to restore a database from a pack store',
          ),
          trailing: const Icon(Icons.chevron_right),
          onTap: () => Navigator.pushNamedAndRemoveUntil(
              context, '/restore', ModalRoute.withName('/')),
        ),
      ),
    ],
  );
}
