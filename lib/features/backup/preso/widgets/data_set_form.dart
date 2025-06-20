//
// Copyright (c) 2024 Nathan Fiedler
//
import 'package:flutter/material.dart';
import 'package:form_builder_validators/form_builder_validators.dart';
import 'package:flutter_form_builder/flutter_form_builder.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/data_set.dart';
import 'package:zorigami/core/domain/entities/pack_store.dart';
import 'package:zorigami/features/backup/preso/widgets/time_range_picker.dart'
    as tpicker;

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

// Only allow hourly and daily frequencies for the time being.
final List<FrequencyOption> frequencies = [
  frequencyManual,
  frequencyHourly,
  frequencyDaily,
  // frequencyWeekly,
  // frequencyMonthly,
];

class DataSetForm extends StatefulWidget {
  final DataSet dataset;
  final List<PackStore> stores;
  final GlobalKey<FormBuilderState> formKey;

  const DataSetForm({
    super.key,
    required this.dataset,
    required this.stores,
    required this.formKey,
  });

  static Map<String, dynamic> initialValuesFrom(
    DataSet dataset,
    List<PackStore> stores,
  ) {
    // convert pack size int of bytes to string of megabytes
    final packSize = (dataset.packSize / 1048576).round();
    final frequency = frequencyFromDataSet(dataset);
    final timeRange = timeRangeFromDataSet(dataset);
    final initialStores = buildInitialStores(dataset, stores);
    final initialExcludes = dataset.excludes.join(', ');
    return {
      'key': dataset.key,
      'computerId': dataset.computerId,
      'basepath': dataset.basepath,
      'packSize': packSize,
      'frequency': frequency,
      'timeRange': timeRange,
      'stores': initialStores,
      'excludes': initialExcludes,
    };
  }

  static DataSet datasetFromState(
    FormBuilderState state,
    List<PackStore> stores,
  ) {
    // convert pack size string of megabytes to int of bytes
    final packSize = state.value['packSize'] * 1048576;
    final schedules = schedulesFromState(state);
    final excludes = excludesFromState(state);
    return DataSet(
      key: state.value['key'],
      computerId: state.value['computerId'],
      basepath: state.value['basepath'],
      packSize: packSize,
      snapshot: const None(),
      schedules: schedules,
      stores: state.value['stores'],
      excludes: excludes,
      errorMsg: const None(),
      status: Status.none,
      backupState: const None(),
    );
  }

  @override
  // ignore: no_logic_in_create_state
  State<DataSetForm> createState() {
    // moving this into _DataSetFormState() results in a null error
    final frequency = frequencyFromDataSet(dataset);
    final enableTimePickers = allowTimeRange(frequency);
    return _DataSetFormState(timePickersEnabled: enableTimePickers);
  }
}

class _DataSetFormState extends State<DataSetForm> {
  bool timePickersEnabled;

  _DataSetFormState({required this.timePickersEnabled});

  @override
  Widget build(BuildContext context) {
    final packStoreOptions = buildStoreOptions(widget.dataset, widget.stores);
    FormBuilderState formState = FormBuilder.of(context)!;
    return Column(
      children: <Widget>[
        FormBuilderTextField(
          name: 'key',
          decoration: const InputDecoration(
            icon: Icon(Icons.vpn_key),
            labelText: 'Dataset Key',
          ),
          readOnly: true,
        ),
        FormBuilderTextField(
          name: 'computerId',
          decoration: const InputDecoration(
            icon: Icon(Icons.computer),
            labelText: 'Computer ID',
          ),
          readOnly: true,
        ),
        FormBuilderTextField(
          name: 'basepath',
          decoration: const InputDecoration(
            icon: Icon(Icons.folder_open),
            labelText: 'Base Path',
          ),
          validator: FormBuilderValidators.required(),
        ),
        FormBuilderTextField(
          name: 'excludes',
          decoration: const InputDecoration(
            icon: Icon(Icons.filter_list),
            labelText: 'Excludes',
          ),
        ),
        FormBuilderSlider(
          name: 'packSize',
          decoration: const InputDecoration(
            icon: Icon(Icons.folder_open),
            labelText: 'Pack Size (MB)',
          ),
          initialValue: 64.0,
          min: 16.0,
          max: 256.0,
          divisions: (256 - 16) ~/ 16,
        ),
        FormBuilderCheckboxGroup<String>(
          name: 'stores',
          options: packStoreOptions,
          // bug https://github.com/danvick/flutter_form_builder/issues/657
          initialValue: formState.initialValue['stores'],
          decoration: const InputDecoration(
            icon: Icon(Icons.archive),
            labelText: 'Pack Store(s)',
          ),
          // require at least one pack store is selected
          validator: FormBuilderValidators.required(),
        ),
        FormBuilderRadioGroup(
          name: 'frequency',
          options: frequencies.map((item) {
            return FormBuilderFieldOption(
              value: item,
              child: Text(item.label),
            );
          }).toList(growable: false),
          decoration: const InputDecoration(
            icon: Icon(Icons.calendar_today),
            labelText: 'Frequency',
          ),
          onChanged: (val) {
            setState(() =>
                timePickersEnabled = allowTimeRange(val as FrequencyOption));
          },
        ),
        FormBuilderField<TimeRange>(
          name: "timeRange",
          enabled: timePickersEnabled,
          builder: (FormFieldState<dynamic> field) {
            return InputDecorator(
              decoration: const InputDecoration(
                icon: Icon(Icons.schedule),
                labelText: 'Start/Stop Time',
              ),
              child: TextButton(
                onPressed: timePickersEnabled
                    ? () async {
                        TimeOfDay? start = startTimeFromValue(field.value);
                        TimeOfDay? stop = stopTimeFromValue(field.value);
                        var result = await tpicker.showTimeRangePicker(
                          context: context,
                          start: start,
                          end: stop,
                        );
                        final converted = timeRangeFromPicker(result);
                        if (converted != null) {
                          field.didChange(converted);
                        }
                      }
                    : null,
                child: Text(
                  field.value != null
                      ? field.value.toPrettyString()
                      : "tap to set",
                ),
              ),
            );
          },
          validator:
              timePickersEnabled ? FormBuilderValidators.required() : null,
        ),
      ],
    );
  }
}

class FrequencyOption {
  FrequencyOption({required this.label, required this.frequency});
  final String label;
  final Frequency? frequency;
}

// The key of the option is used to determine if it is checked.
List<FormBuilderFieldOption<String>> buildStoreOptions(
  DataSet dataset,
  List<PackStore> stores,
) {
  final List<FormBuilderFieldOption<String>> options = List.from(
    stores.map((e) {
      return FormBuilderFieldOption<String>(
        value: e.key,
        child: Text(e.label),
      );
    }),
  );
  return options;
}

// Return the keys of the stores that are to be selected initially.
List<String> buildInitialStores(DataSet dataset, List<PackStore> stores) {
  return stores
      .where((e) => dataset.stores.contains(e.key))
      .map((e) => e.key)
      .toList();
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
      // for now, only allow certain frequencies
      // return frequencyWeekly;
      return frequencyDaily;
    case Frequency.monthly:
      // for now, only allow certain frequencies
      // return frequencyMonthly;
      return frequencyDaily;
  }
}

TimeOfDay? startTimeFromValue(TimeRange? value) {
  if (value != null) {
    return timeOfDateFromInt(value.start);
  }
  return null;
}

TimeOfDay? stopTimeFromValue(TimeRange? value) {
  if (value != null) {
    return timeOfDateFromInt(value.stop);
  }
  return null;
}

// convert from TimeRange.start/stop to the material TimeOfDay
TimeOfDay timeOfDateFromInt(int time) {
  final int asMinutes = (time / 60) as int;
  final hour = asMinutes / 60;
  final minutes = asMinutes - (hour.toInt() * 60);
  return TimeOfDay(hour: hour.toInt(), minute: minutes.toInt());
}

TimeRange? timeRangeFromPicker(tpicker.TimeRange? value) {
  if (value != null) {
    final start = (value.startTime.hour * 60 + value.startTime.minute) * 60;
    final stop = (value.endTime.hour * 60 + value.endTime.minute) * 60;
    return TimeRange(start: start, stop: stop);
  }
  return null;
}

TimeRange? timeRangeFromDataSet(DataSet dataset) {
  if (dataset.schedules.isEmpty) {
    return null;
  }
  switch (dataset.schedules[0].frequency) {
    case Frequency.hourly:
      return null;
    case Frequency.daily:
    case Frequency.weekly:
    case Frequency.monthly:
      return dataset.schedules[0].timeRange.toNullable();
  }
}

List<Schedule> schedulesFromState(FormBuilderState state) {
  final FrequencyOption option = state.value['frequency'];
  if (option.frequency == null) {
    // manual (no) scheduling
    return [];
  }
  // dart needs help knowing exactly what type of option is returned
  final Option<TimeRange> timeRange = allowTimeRange(option)
      ? Option.from(state.value['timeRange'])
      : const None();
  return [
    Schedule(
      frequency: option.frequency!,
      timeRange: timeRange,
      weekOfMonth: const None(),
      dayOfWeek: const None(),
      dayOfMonth: const None(),
    )
  ];
}

List<String> excludesFromState(FormBuilderState state) {
  final String value = state.value['excludes'];
  return List.from(value.split(',').map((e) => e.trim()));
}

bool allowTimeRange(FrequencyOption frequency) {
  //
  // For now, only daily can have a time range. Eventually, once advanced
  // scheduling is supported, then combinations of frequency and day-of-week,
  // day-of-month, or week-of-month will make sense when combined with a time
  // range.
  //
  switch (frequency.frequency) {
    case Frequency.daily:
      return true;
    default:
      return false;
  }
}
