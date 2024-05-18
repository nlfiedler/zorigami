//
// Copyright (c) 2024 Nathan Fiedler
//
import 'package:flutter/material.dart';
import 'package:intl/intl.dart' as intl;
import 'package:progressive_time_picker/progressive_time_picker.dart';

class TimeRange {
  TimeOfDay startTime;
  TimeOfDay endTime;

  TimeRange({required this.startTime, required this.endTime});

  factory TimeRange.fromPickedTime(PickedTime start, PickedTime end) {
    return TimeRange(
      startTime: TimeOfDay(hour: start.h, minute: start.m),
      endTime: TimeOfDay(hour: end.h, minute: end.m),
    );
  }

  @override
  String toString() {
    return "Start: ${startTime.toString()} to ${endTime.toString()}";
  }
}

final ftd = intl.NumberFormat('00');

PickedTime? picked(TimeOfDay? value) {
  if (value != null) {
    return PickedTime(h: value.hour, m: value.minute);
  }
  return null;
}

showTimeRangePicker({
  required BuildContext context,
  TimeOfDay? start,
  TimeOfDay? end,
}) async {
  final pstart = picked(start);
  final pend = picked(end);
  Widget dialog = Dialog(
    elevation: 12,
    child: TimeRangePicker(
      start: pstart,
      end: pend,
    ),
  );
  return await showDialog<TimeRange>(
      context: context,
      builder: (BuildContext context) {
        return dialog;
      });
}

class TimeRangePicker extends StatefulWidget {
  final PickedTime? start;
  final PickedTime? end;

  const TimeRangePicker({
    Key? key,
    required this.start,
    required this.end,
  }) : super(key: key);

  @override
  State<TimeRangePicker> createState() => _TimeRangePickerState();
}

class _TimeRangePickerState extends State<TimeRangePicker> {
  final ClockTimeFormat _clockTimeFormat = ClockTimeFormat.twentyFourHours;
  PickedTime _startTime = PickedTime(h: 0, m: 0);
  PickedTime _endTime = PickedTime(h: 8, m: 0);
  bool? validRange = true;

  @override
  void initState() {
    super.initState();
    if (widget.start != null) {
      _startTime = widget.start!;
    }
    if (widget.end != null) {
      _endTime = widget.end!;
    }
  }

  @override
  Widget build(BuildContext context) {
    final pickerColor = Theme.of(context).colorScheme.inversePrimary;
    final baseColor = Theme.of(context).colorScheme.inverseSurface;
    return Scaffold(
      body: Column(
        mainAxisAlignment: MainAxisAlignment.spaceEvenly,
        children: [
          const Text(
            'Choose the backup time range',
            style: TextStyle(
              fontSize: 24,
              fontWeight: FontWeight.bold,
            ),
          ),
          TimePicker(
            initTime: _startTime,
            endTime: _endTime,
            height: 320.0,
            width: 320.0,
            onSelectionChange: (init, end, isDisableRange) {
              setState(() {
                _startTime = init;
                _endTime = end;
              });
            },
            onSelectionEnd: (start, end, isDisableRange) => {},
            primarySectors: _clockTimeFormat.value,
            secondarySectors: _clockTimeFormat.value * 2,
            decoration: TimePickerDecoration(
              baseColor: baseColor,
              pickerBaseCirclePadding: 15.0,
              sweepDecoration: TimePickerSweepDecoration(
                pickerStrokeWidth: 30.0,
                pickerColor: pickerColor,
                showConnector: true,
              ),
              initHandlerDecoration: TimePickerHandlerDecoration(
                shape: BoxShape.circle,
                radius: 12.0,
              ),
              endHandlerDecoration: TimePickerHandlerDecoration(
                shape: BoxShape.circle,
                radius: 12.0,
              ),
              primarySectorsDecoration: TimePickerSectorDecoration(
                width: 1.0,
                size: 4.0,
                radiusPadding: 25.0,
              ),
              secondarySectorsDecoration: TimePickerSectorDecoration(
                width: 1.0,
                size: 2.0,
                radiusPadding: 25.0,
              ),
            ),
            child: Padding(
              padding: const EdgeInsets.all(32.0),
              child: Column(
                mainAxisAlignment: MainAxisAlignment.center,
                children: [
                  Text(
                    _timeRange(),
                    style: const TextStyle(
                      fontSize: 18.0,
                      fontWeight: FontWeight.bold,
                    ),
                  ),
                ],
              ),
            ),
          ),
          ButtonBar(
            children: <Widget>[
              TextButton(
                child: const Text('Cancel'),
                onPressed: () => Navigator.of(context).pop(),
              ),
              ElevatedButton(
                child: const Text('OK'),
                onPressed: () => Navigator.of(context)
                    .pop(TimeRange.fromPickedTime(_startTime, _endTime)),
              ),
            ],
          )
        ],
      ),
    );
  }

  String _timeRange() {
    final start = '${ftd.format(_startTime.h)}:${ftd.format(_startTime.m)}';
    final stop = '${ftd.format(_endTime.h)}:${ftd.format(_endTime.m)}';
    return '$start - $stop';
  }
}
