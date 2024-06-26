//
// Copyright (c) 2024 Nathan Fiedler
//
import 'package:flutter/material.dart';
import 'package:flutter_bloc/flutter_bloc.dart';
import 'package:flutter_form_builder/flutter_form_builder.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/data_set.dart';
import 'package:zorigami/core/domain/entities/pack_store.dart';
import 'package:zorigami/features/backup/preso/bloc/providers.dart';
import 'package:zorigami/features/backup/preso/bloc/create_data_sets_bloc.dart';
import 'package:zorigami/features/backup/preso/bloc/pack_stores_bloc.dart'
    as psb;
import 'package:zorigami/features/backup/preso/widgets/data_set_form.dart';

class NewDataSetScreen extends ConsumerWidget {
  const NewDataSetScreen({super.key});

  @override
  Widget build(BuildContext context, WidgetRef ref) {
    return Scaffold(
      appBar: AppBar(
        title: const Text('ADD DATASET'),
      ),
      body: MultiBlocProvider(
        providers: [
          BlocProvider<CreateDataSetsBloc>(
            create: (_) => ref.read(createDatasetsBlocProvider),
          ),
          BlocProvider<psb.PackStoresBloc>(
            create: (_) => ref.read(packStoresBlocProvider),
          ),
        ],
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
              final dataset = defaultDataSet();
              return Padding(
                padding: const EdgeInsets.all(16.0),
                child: NewDataSetWidget(
                  dataset: dataset,
                  stores: state.stores,
                ),
              );
            }
            return const CircularProgressIndicator();
          },
        ),
      ),
    );
  }
}

class NewDataSetWidget extends StatelessWidget {
  NewDataSetWidget({
    super.key,
    required this.dataset,
    required this.stores,
  });

  final formKey = GlobalKey<FormBuilderState>();
  final DataSet dataset;
  final List<PackStore> stores;

  void addDataSet(BuildContext context, DataSetForm setForm) {
    if (formKey.currentState!.saveAndValidate()) {
      final dataset = DataSetForm.datasetFromState(
        formKey.currentState!,
        stores,
      );
      BlocProvider.of<CreateDataSetsBloc>(context).add(
        DefineDataSet(dataset: dataset),
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
    return BlocConsumer<CreateDataSetsBloc, CreateDataSetsState>(
      listener: (context, state) {
        if (state is Submitted) {
          // this will force everything to rebuild
          Navigator.pop(context, true);
        } else if (state is Error) {
          ScaffoldMessenger.of(context).showSnackBar(
            SnackBar(content: Text('Error: ${state.message}')),
          );
        }
      },
      builder: (context, state) {
        return SingleChildScrollView(
          child: Column(
            children: <Widget>[
              FormBuilder(
                key: formKey,
                initialValue: DataSetForm.initialValuesFrom(dataset, stores),
                autovalidateMode: AutovalidateMode.always,
                // not convinced this enabled is effective
                enabled: state is! Submitting,
                child: datasetForm,
              ),
              ButtonBar(
                children: <Widget>[
                  ElevatedButton.icon(
                    icon: const Icon(Icons.save),
                    label: const Text('ADD'),
                    onPressed: (state is Submitting)
                        ? null
                        : () => addDataSet(context, datasetForm),
                  ),
                ],
              )
            ],
          ),
        );
      },
    );
  }
}

DataSet defaultDataSet() {
  return const DataSet(
    key: 'auto-generated',
    computerId: 'auto-generated',
    packSize: 67108864,
    snapshot: None(),
    basepath: '/',
    schedules: [],
    stores: [],
    excludes: [],
    errorMsg: None(),
    status: Status.none,
    backupState: None(),
  );
}
