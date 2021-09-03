//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:flutter/material.dart';
import 'package:flutter_form_builder/flutter_form_builder.dart';
import 'package:intl/intl.dart';
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

// Only allow hourly and daily frequencies for the time being.
final List<FrequencyOption> frequencies = [
  // frequencyManual,
  frequencyHourly,
  frequencyDaily,
  // frequencyWeekly,
  // frequencyMonthly,
];

class DataSetForm extends StatefulWidget {
  final DataSet dataset;
  final List<PackStore> stores;
  final GlobalKey<FormBuilderState> formKey;

  DataSetForm({
    Key? key,
    required this.dataset,
    required this.stores,
    required this.formKey,
  }) : super(key: key);

  static Map<String, dynamic> initialValuesFrom(
    DataSet dataset,
    List<PackStore> stores,
  ) {
    // convert pack size int of bytes to string of megabytes
    final packSize = (dataset.packSize / 1048576).round().toString();
    final frequency = frequencyFromDataSet(dataset);
    final start = startTimeFromDataSet(dataset);
    final stop = stopTimeFromDataSet(dataset);
    final initialStores = buildInitialStores(dataset, stores);
    final initialExcludes = dataset.excludes.join(', ');
    return {
      'key': dataset.key,
      'computerId': dataset.computerId,
      'basepath': dataset.basepath,
      'packSize': packSize,
      'frequency': frequency,
      'start': start,
      'stop': stop,
      'stores': initialStores,
      'excludes': initialExcludes,
    };
  }

  static DataSet datasetFromState(
    FormBuilderState state,
    List<PackStore> stores,
  ) {
    // convert pack size string of megabytes to int of bytes
    final packSize = int.parse(state.value['packSize']) * 1048576;
    final schedules = schedulesFromState(state);
    final excludes = excludesFromState(state);
    return DataSet(
      key: state.value['key'],
      computerId: state.value['computerId'],
      basepath: state.value['basepath'],
      packSize: packSize,
      snapshot: None(),
      schedules: schedules,
      stores: state.value['stores'],
      excludes: excludes,
      errorMsg: None(),
      status: Status.none,
    );
  }

  @override
  _DataSetFormState createState() {
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
          decoration: InputDecoration(
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
          validator: FormBuilderValidators.required(context),
        ),
        FormBuilderTextField(
          name: 'excludes',
          decoration: const InputDecoration(
            icon: Icon(Icons.filter_list),
            labelText: 'Excludes',
          ),
        ),
        FormBuilderTextField(
          name: 'packSize',
          decoration: const InputDecoration(
            icon: Icon(Icons.folder_open),
            labelText: 'Pack Size (MB)',
          ),
          validator: FormBuilderValidators.compose([
            FormBuilderValidators.numeric(context),
            FormBuilderValidators.min(context, 16),
            FormBuilderValidators.max(context, 256),
          ]),
        ),
        FormBuilderCheckboxGroup<String>(
          name: 'stores',
          options: packStoreOptions,
          // bug https://github.com/danvick/flutter_form_builder/issues/657
          initialValue: formState.initialValue['stores'],
          decoration: InputDecoration(
            icon: Icon(Icons.archive),
            labelText: 'Pack Store(s)',
          ),
          // require at least one pack store is selected
          validator: FormBuilderValidators.required(context),
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
        FormBuilderDateTimePicker(
          name: 'start',
          enabled: timePickersEnabled,
          inputType: InputType.time,
          decoration: const InputDecoration(
            icon: Icon(Icons.schedule),
            labelText: 'Start Time',
          ),
          format: DateFormat.jm(),
          validator: (val) {
            final stop = widget.formKey.currentState!.fields['stop'];
            if (stop?.value == null && val != null) {
              return 'Please set stop time';
            }
            return null;
          },
        ),
        FormBuilderDateTimePicker(
          name: 'stop',
          enabled: timePickersEnabled,
          inputType: InputType.time,
          decoration: const InputDecoration(
            icon: Icon(Icons.schedule),
            labelText: 'Stop Time',
          ),
          format: DateFormat.jm(),
          validator: (val) {
            final start = widget.formKey.currentState!.fields['start'];
            if (start?.value == null && val != null) {
              return 'Please set start time';
            }
            return null;
          },
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
    // for now, only allow hourly and daily frequencies
    // return frequencyManual;
    return frequencyHourly;
  }
  switch (dataset.schedules[0].frequency) {
    case Frequency.hourly:
      return frequencyHourly;
    case Frequency.daily:
      return frequencyDaily;
    case Frequency.weekly:
      // for now, only allow hourly and daily frequencies
      // return frequencyWeekly;
      return frequencyDaily;
    case Frequency.monthly:
      // for now, only allow hourly and daily frequencies
      // return frequencyMonthly;
      return frequencyDaily;
    default:
      throw ArgumentError('frequency is not recognized');
  }
}

DateTime? startTimeFromDataSet(DataSet dataset) {
  final option = getTimeFromDataSet(dataset, (Schedule schedule) {
    return schedule.timeRange.map((v) {
      return timeFromInt(v.start);
    });
  });
  return option.toNullable();
}

DateTime? stopTimeFromDataSet(DataSet dataset) {
  final option = getTimeFromDataSet(dataset, (Schedule schedule) {
    return schedule.timeRange.map((v) {
      return timeFromInt(v.stop);
    });
  });
  return option.toNullable();
}

Option<DateTime> getTimeFromDataSet(
  DataSet dataset,
  Option<DateTime> Function(Schedule) op,
) {
  if (dataset.schedules.isEmpty) {
    return None();
  }
  switch (dataset.schedules[0].frequency) {
    case Frequency.hourly:
      return None();
    case Frequency.daily:
    case Frequency.weekly:
    case Frequency.monthly:
      return op(dataset.schedules[0]);
    default:
      throw ArgumentError('frequency is not recognized');
  }
}

DateTime timeFromInt(int time) {
  final int as_minutes = (time / 60) as int;
  //
  // This code works fine in dartdevc:
  //
  // final int hour = as_minutes ~/ 60;
  // final int minutes = as_minutes % 60;
  // return DateTime(1, 1, 1, hour, minutes);
  //
  // However, it seems like the dart2js compiler produces something erroneous
  // and it causes this code to fail miserably, which in turn causes the
  // component to paint an enormous grey rectangle. As such, perform all the
  // arithmetic using not-integer operators and then convert to ints at the end.
  //
  final hour = as_minutes / 60;
  final minutes = as_minutes - (hour.toInt() * 60);
  return DateTime(1, 1, 1, hour.toInt(), minutes.toInt());
}

List<Schedule> schedulesFromState(FormBuilderState state) {
  final FrequencyOption option = state.value['frequency'];
  if (option.frequency == null) {
    // manual (no) scheduling
    return [];
  }
  // dart needs help knowing exactly what type of option is returned
  final Option<TimeRange> timeRange =
      allowTimeRange(option) ? timeRangeFromState(state) : None();
  return [
    Schedule(
      frequency: option.frequency!,
      timeRange: timeRange,
      weekOfMonth: None(),
      dayOfWeek: None(),
      dayOfMonth: None(),
    )
  ];
}

List<String> excludesFromState(FormBuilderState state) {
  final String value = state.value['excludes'];
  return List.from(value.split(',').map((e) => e.trim()));
}

Option<TimeRange> timeRangeFromState(FormBuilderState state) {
  final startDateTime = state.value['start'];
  final stopDateTime = state.value['stop'];
  if (startDateTime != null && stopDateTime != null) {
    final start = (startDateTime.hour * 60 + startDateTime.minute) * 60;
    final stop = (stopDateTime.hour * 60 + stopDateTime.minute) * 60;
    return Some(TimeRange(start: start, stop: stop));
  }
  return None();
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
