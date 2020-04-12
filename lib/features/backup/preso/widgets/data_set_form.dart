//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:flutter/material.dart';
import 'package:flutter_form_builder/flutter_form_builder.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/data_set.dart';
import 'package:zorigami/core/domain/entities/pack_store.dart';

final frequencyManual = FrequencyOption(
  label: 'Manual',
  frequency: null,
);
final frequencyHourly = FrequencyOption(
  label: 'Hourly',
  frequency: Frequency.hourly,
);
final frequencyDaily = FrequencyOption(
  label: 'Daily',
  frequency: Frequency.daily,
);
final frequencyWeekly = FrequencyOption(
  label: 'Weekly',
  frequency: Frequency.weekly,
);
final frequencyMonthly = FrequencyOption(
  label: 'Monthly',
  frequency: Frequency.monthly,
);

final List<FrequencyOption> frequencies = [
  frequencyManual,
  frequencyHourly,
  frequencyDaily,
  frequencyWeekly,
  frequencyMonthly,
];

class DataSetForm extends StatelessWidget {
  final DataSet dataset;
  final List<PackStore> stores;

  DataSetForm({
    Key key,
    @required this.dataset,
    @required this.stores,
  }) : super(key: key);

  Map<String, dynamic> initialValuesFrom(DataSet dataset) {
    // convert pack size int of bytes to string of megabytes
    final packSize = (dataset.packSize / 1048576).round().toString();
    final frequency = frequencyFromDataSet(dataset);
    final initialStores = buildInitialStores(dataset, stores);
    return {
      'key': dataset.key,
      'computerId': dataset.computerId,
      'basepath': dataset.basepath,
      'packSize': packSize,
      'frequency': frequency,
      'stores': initialStores,
    };
  }

  DataSet datasetFromState(FormBuilderState state) {
    // convert pack size string of megabytes to int of bytes
    final packSize = int.parse(state.value['packSize']) * 1048576;
    final schedules = schedulesFromState(state);
    final List<String> selectedStores = List.from(
      state.value['stores'].map((e) => e.key),
    );
    return DataSet(
      key: state.value['key'],
      computerId: state.value['computerId'],
      basepath: state.value['basepath'],
      packSize: packSize,
      snapshot: None(),
      schedules: schedules,
      stores: selectedStores,
    );
  }

  @override
  Widget build(BuildContext context) {
    final frequency = frequencyFromDataSet(dataset);
    final initialStores = buildInitialStores(dataset, stores);
    final packStoreOptions = buildStoreOptions(stores);
    return Column(
      children: <Widget>[
        FormBuilderTextField(
          attribute: 'key',
          decoration: InputDecoration(
            icon: Icon(Icons.vpn_key),
            labelText: 'Dataset Key',
          ),
          readOnly: true,
        ),
        FormBuilderTextField(
          attribute: 'computerId',
          decoration: const InputDecoration(
            icon: Icon(Icons.computer),
            labelText: 'Computer ID',
          ),
          readOnly: true,
        ),
        FormBuilderTextField(
          attribute: 'basepath',
          decoration: const InputDecoration(
            icon: Icon(Icons.folder_open),
            labelText: 'Base Path',
          ),
          validators: [FormBuilderValidators.required()],
        ),
        FormBuilderTextField(
          attribute: 'packSize',
          decoration: const InputDecoration(
            icon: Icon(Icons.folder_open),
            labelText: 'Pack Size (MB)',
          ),
          validators: [
            FormBuilderValidators.numeric(),
            FormBuilderValidators.min(16),
            FormBuilderValidators.max(256),
          ],
        ),
        FormBuilderCheckboxList(
          attribute: 'stores',
          initialValue: initialStores,
          leadingInput: true,
          options: packStoreOptions,
          decoration: InputDecoration(
            icon: Icon(Icons.archive),
            labelText: 'Pack Store(s)',
          ),
          // require at least one pack store is selected
          validators: [FormBuilderValidators.required()],
        ),
        FormBuilderRadio(
          attribute: 'frequency',
          initialValue: frequency,
          leadingInput: true,
          options: frequencies.map((item) {
            return FormBuilderFieldOption(
              value: item,
              child: Text(item.label),
            );
          }).toList(growable: false),
          decoration: const InputDecoration(
            icon: Icon(Icons.calendar_today),
            labelText: 'Schedule',
          ),
        )
      ],
    );
  }
}

class FrequencyOption {
  FrequencyOption({@required this.label, @required this.frequency});
  final String label;
  final Frequency frequency;
}

List<FormBuilderFieldOption> buildStoreOptions(List<PackStore> stores) {
  final List<FormBuilderFieldOption> options = List.from(
    stores.map((e) {
      return FormBuilderFieldOption(
        child: Text(e.key),
        value: e,
      );
    }),
  );
  return options;
}

List<PackStore> buildInitialStores(DataSet dataset, List<PackStore> stores) {
  return stores.where((e) => dataset.stores.contains(e.key)).toList();
}

FrequencyOption frequencyFromDataSet(DataSet dataset) {
  if (dataset.schedules.isEmpty) {
    return frequencyManual;
  }
  switch (dataset.schedules[0].frequency) {
    case Frequency.hourly:
      return frequencyHourly;
    case Frequency.daily:
      return frequencyDaily;
    case Frequency.weekly:
      return frequencyWeekly;
    case Frequency.monthly:
      return frequencyMonthly;
    default:
      throw ArgumentError('frequency is not recognized');
  }
}

List<Schedule> schedulesFromState(FormBuilderState state) {
  final FrequencyOption option = state.value['frequency'];
  if (option.frequency == null) {
    return [];
  }
  return [
    Schedule(
      frequency: option.frequency,
      timeRange: None(),
      weekOfMonth: None(),
      dayOfWeek: None(),
      dayOfMonth: None(),
    )
  ];
}
