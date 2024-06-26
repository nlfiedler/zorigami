//
// Copyright (c) 2024 Nathan Fiedler
//
import 'package:flutter/material.dart';
import 'package:flutter_bloc/flutter_bloc.dart';
import 'package:flutter_form_builder/flutter_form_builder.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:zorigami/core/domain/entities/data_set.dart';
import 'package:zorigami/core/domain/entities/pack_store.dart';
import 'package:zorigami/features/browse/preso/bloc/data_sets_bloc.dart';
import 'package:zorigami/features/backup/preso/bloc/edit_data_sets_bloc.dart'
    as edsb;
import 'package:zorigami/features/backup/preso/bloc/pack_stores_bloc.dart'
    as psb;
import 'package:zorigami/features/backup/preso/bloc/providers.dart';
import 'package:zorigami/features/backup/preso/widgets/data_set_form.dart';

class DataSetsList extends ConsumerWidget {
  final List<DataSet> sets;

  const DataSetsList({super.key, required this.sets});

  @override
  Widget build(BuildContext context, WidgetRef ref) {
    return BlocProvider<psb.PackStoresBloc>(
      create: (_) => ref.read(packStoresBlocProvider),
      child: BlocBuilder<psb.PackStoresBloc, psb.PackStoresState>(
        builder: (context, state) {
          if (state is psb.Empty) {
            // kick off the initial remote request
            BlocProvider.of<psb.PackStoresBloc>(context)
                .add(psb.LoadAllPackStores());
          }
          if (state is psb.Error) {
            return Card(
              child: ListTile(
                title: const Text('Error loading pack stores'),
                subtitle: Text(state.message),
              ),
            );
          }
          if (state is psb.Loaded) {
            return state.stores.isEmpty
                ? buildStoresHelp(context)
                : sets.isEmpty
                    ? buildSetsHelp(context)
                    : DataSetsListInner(
                        sets: sets,
                        stores: state.stores,
                      );
          }
          return const CircularProgressIndicator();
        },
      ),
    );
  }
}

Widget buildStoresHelp(BuildContext context) {
  return Card(
    child: ListTile(
      leading: const Icon(Icons.dns),
      title: const Text('No pack stores found'),
      subtitle: const Text(
        'First configure one or more pack stores, then create a data set using those stores.',
      ),
      trailing: const Icon(Icons.chevron_right),
      onTap: () => Navigator.pushNamedAndRemoveUntil(
          context, '/stores', ModalRoute.withName('/')),
    ),
  );
}

Widget buildSetsHelp(BuildContext context) {
  return const Card(
    child: ListTile(
      leading: Icon(Icons.dns),
      title: Text('No data sets found'),
      subtitle: Text('Use the + button below to add a data set.'),
    ),
  );
}

class DataSetsListInner extends ConsumerStatefulWidget {
  final List<DataSet> sets;
  final List<PackStore> stores;

  const DataSetsListInner({
    super.key,
    required this.sets,
    required this.stores,
  });

  @override
  ConsumerState<DataSetsListInner> createState() => _DataSetsListState();
}

class _DataSetsListState extends ConsumerState<DataSetsListInner> {
  late List<ExpansionItem> items;

  @override
  void initState() {
    super.initState();
    items = List<ExpansionItem>.from(
      widget.sets.map((e) {
        final headerValue = ListTile(
          leading: const Icon(Icons.dns),
          title: Text('${e.basepath}, runs ${getSchedule(e)}'),
          subtitle: Text('Status: ${e.describeStatus()}'),
        );
        final expandedValue = Card(
          child: Padding(
            padding: const EdgeInsets.symmetric(
              vertical: 8.0,
              horizontal: 32.0,
            ),
            child: DataSetListDetails(dataset: e, stores: widget.stores),
          ),
        );
        return ExpansionItem(
          headerValue: headerValue,
          expandedValue: expandedValue,
        );
      }),
    );
  }

  @override
  Widget build(BuildContext context) {
    return SingleChildScrollView(
      child: BlocProvider<edsb.EditDataSetsBloc>(
        create: (_) => ref.read(editDataSetsBlocProvider),
        child: ExpansionPanelList(
          expansionCallback: (int index, bool isExpanded) {
            setState(() {
              items[index].isExpanded = !items[index].isExpanded;
            });
          },
          children: items.map<ExpansionPanel>((ExpansionItem item) {
            return ExpansionPanel(
              canTapOnHeader: true,
              headerBuilder: (BuildContext context, bool isExpanded) {
                return item.headerValue;
              },
              body: item.expandedValue,
              isExpanded: item.isExpanded,
            );
          }).toList(),
        ),
      ),
    );
  }
}

class DataSetListDetails extends StatelessWidget {
  DataSetListDetails({
    super.key,
    required this.dataset,
    required this.stores,
  });

  final formKey = GlobalKey<FormBuilderState>();
  final DataSet dataset;
  final List<PackStore> stores;

  void saveDataSet(BuildContext context, DataSetForm setForm) {
    if (formKey.currentState!.saveAndValidate()) {
      final dataset = DataSetForm.datasetFromState(
        formKey.currentState!,
        stores,
      );
      BlocProvider.of<edsb.EditDataSetsBloc>(context).add(
        edsb.UpdateDataSet(dataset: dataset),
      );
    }
  }

  @override
  Widget build(BuildContext context) {
    final datasetForm = DataSetForm(
      dataset: dataset,
      stores: stores,
      formKey: formKey,
    );
    return BlocConsumer<edsb.EditDataSetsBloc, edsb.EditDataSetsState>(
      listener: (context, state) {
        if (state is edsb.Submitted) {
          // this will force everything to rebuild
          BlocProvider.of<DataSetsBloc>(context).add(ReloadDataSets());
        } else if (state is edsb.Error) {
          ScaffoldMessenger.of(context).showSnackBar(
            SnackBar(content: Text('Error: ${state.message}')),
          );
        }
      },
      builder: (context, state) {
        return Column(
          children: <Widget>[
            FormBuilder(
              key: formKey,
              initialValue: DataSetForm.initialValuesFrom(dataset, stores),
              autovalidateMode: AutovalidateMode.always,
              // not convinced this enabled is effective
              enabled: state is! edsb.Submitting,
              child: datasetForm,
            ),
            ButtonBar(
              children: <Widget>[
                ElevatedButton.icon(
                  icon: const Icon(Icons.save),
                  label: const Text('SAVE'),
                  onPressed: (state is edsb.Submitting)
                      ? null
                      : () => saveDataSet(context, datasetForm),
                ),
                TextButton.icon(
                  icon: const Icon(Icons.delete),
                  label: const Text('DELETE'),
                  onPressed: (state is edsb.Submitting)
                      ? null
                      : () {
                          BlocProvider.of<edsb.EditDataSetsBloc>(context).add(
                            edsb.DeleteDataSet(dataset: dataset),
                          );
                        },
                ),
              ],
            )
          ],
        );
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

class ExpansionItem {
  ExpansionItem({
    required this.expandedValue,
    required this.headerValue,
    this.isExpanded = false,
  });
  Widget expandedValue;
  Widget headerValue;
  bool isExpanded;
}
