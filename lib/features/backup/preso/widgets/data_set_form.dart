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
  final GlobalKey<FormBuilderState> formKey;

  DataSetForm({
    Key key,
    @required this.dataset,
    @required this.stores,
    @required this.formKey,
  }) : super(key: key);

  Map<String, dynamic> initialValuesFrom(DataSet dataset) {
    // convert pack size int of bytes to string of megabytes
    final packSize = (dataset.packSize / 1048576).round().toString();
    final frequency = frequencyFromDataSet(dataset);
    final start = startTimeFromDataSet(dataset);
    final stop = stopTimeFromDataSet(dataset);
    final initialStores = buildInitialStores(dataset, stores);
    return {
      'key': dataset.key,
      'computerId': dataset.computerId,
      'basepath': dataset.basepath,
      'packSize': packSize,
      'frequency': frequency,
      'start': start,
      'stop': stop,
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
    final start = startTimeFromDataSet(dataset);
    final stop = stopTimeFromDataSet(dataset);
    //
    // For now, always enable the time pickers, until changing one form field
    // can fire a rebuild of the form to cause other fields to be disabled.
    //
    // final timePickersEnabled = allowTimeRange(frequency);
    final timePickersEnabled = true;
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
            labelText: 'Frequency',
          ),
        ),
        FormBuilderDateTimePicker(
          attribute: 'start',
          initialValue: start,
          enabled: timePickersEnabled,
          inputType: InputType.time,
          decoration: const InputDecoration(
            icon: Icon(Icons.schedule),
            labelText: 'Start Time',
          ),
          validators: [
            (val) {
              final stop = formKey.currentState.fields['stop'];
              if (stop.currentState.value == null && val != null) {
                return 'Please set stop time';
              }
              return null;
            },
          ],
        ),
        FormBuilderDateTimePicker(
          attribute: 'stop',
          initialValue: stop,
          enabled: timePickersEnabled,
          inputType: InputType.time,
          decoration: const InputDecoration(
            icon: Icon(Icons.schedule),
            labelText: 'Stop Time',
          ),
          validators: [
            (val) {
              final start = formKey.currentState.fields['start'];
              if (start.currentState.value == null && val != null) {
                return 'Please set start time';
              }
              return null;
            },
          ],
        ),
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
        child: Text(e.label),
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

DateTime startTimeFromDataSet(DataSet dataset) {
  final option = getTimeFromDataSet(dataset, (Schedule schedule) {
    return schedule.timeRange.map((v) {
      return timeFromInt(v.start);
    });
  });
  return option.unwrapOr(null);
}

DateTime stopTimeFromDataSet(DataSet dataset) {
  final option = getTimeFromDataSet(dataset, (Schedule schedule) {
    return schedule.timeRange.map((v) {
      return timeFromInt(v.stop);
    });
  });
  return option.unwrapOr(null);
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
      return op(dataset.schedules[0]);
    case Frequency.weekly:
      return op(dataset.schedules[0]);
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
      frequency: option.frequency,
      timeRange: timeRange,
      weekOfMonth: None(),
      dayOfWeek: None(),
      dayOfMonth: None(),
    )
  ];
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
