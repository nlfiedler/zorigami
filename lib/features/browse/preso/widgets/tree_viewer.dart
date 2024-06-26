//
// Copyright (c) 2024 Nathan Fiedler
//
import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:flutter_bloc/flutter_bloc.dart';
import 'package:intl/intl.dart';
import 'package:zorigami/core/domain/entities/data_set.dart';
import 'package:zorigami/core/domain/entities/tree.dart';
import 'package:zorigami/features/browse/preso/bloc/tree_browser_bloc.dart';

class TreeViewer extends StatelessWidget {
  final String rootTree;
  final DataSet dataset;

  const TreeViewer({
    super.key,
    required this.dataset,
    required this.rootTree,
  });

  @override
  Widget build(BuildContext context) {
    return BlocConsumer<TreeBrowserBloc, TreeBrowserState>(
      listener: (context, state) {
        if (state is Loaded) {
          if (state.restoresEnqueued) {
            const content = Text('File restores enqueued');
            // must show snackbar outside of builder
            ScaffoldMessenger.of(context).showSnackBar(
              const SnackBar(content: content),
            );
          }
        }
      },
      builder: (context, state) {
        if (state is Empty) {
          // kick off the initial remote request
          BlocProvider.of<TreeBrowserBloc>(context).add(
            LoadTree(digest: rootTree),
          );
        }
        if (state is Error) {
          return Text('Error: ${state.message}');
        }
        if (state is Loaded) {
          return Column(
            children: <Widget>[
              TreePath(dataset: dataset, state: state),
              Expanded(child: TreeTable(state: state)),
            ],
          );
        }
        return const Center(child: CircularProgressIndicator());
      },
    );
  }
}

class TreePath extends StatelessWidget {
  final DataSet dataset;
  final Loaded state;

  const TreePath({
    super.key,
    required this.dataset,
    required this.state,
  });

  @override
  Widget build(BuildContext context) {
    return Padding(
      padding: const EdgeInsets.all(8.0),
      child: Row(
        children: <Widget>[
          ElevatedButton.icon(
            icon: const Icon(Icons.arrow_upward),
            label: const Text('Up'),
            onPressed: state.path.isNotEmpty
                ? () => BlocProvider.of<TreeBrowserBloc>(context).add(
                      NavigateUpward(),
                    )
                : null,
          ),
          const SizedBox(width: 16.0),
          ElevatedButton.icon(
            icon: const Icon(Icons.restore),
            label: const Text('Put Back'),
            onPressed: state.selections.isNotEmpty
                ? () {
                    BlocProvider.of<TreeBrowserBloc>(context).add(
                      RestoreSelections(datasetKey: dataset.key),
                    );
                  }
                : null,
          ),
          const SizedBox(width: 56.0),
          Text('${state.tree.entries.length} entries'),
          const SizedBox(width: 16.0),
          Text(
            ' / ${state.path.join(' / ')}',
            style: const TextStyle(fontFamily: 'RobotoMono'),
          ),
        ],
      ),
    );
  }
}

class TreeTable extends StatefulWidget {
  final Loaded state;

  const TreeTable({super.key, required this.state});

  @override
  State<TreeTable> createState() => _TreeTableState();
}

class _TreeTableState extends State<TreeTable> {
  bool _sortNameAsc = true;
  bool _sortDateAsc = true;
  bool _sortRefAsc = true;
  bool _sortAscending = true;
  int? _sortColumnIndex;

  @override
  Widget build(BuildContext context) {
    const mono = TextStyle(fontFamily: 'RobotoMono');
    final List<DataRow> rows = List.of(widget.state.tree.entries.map((e) {
      final name = DataCell(
        Tooltip(
            message: e.reference.type == EntryType.tree
                ? 'Navigate to folder'
                : 'Copy digest to clipboard',
            child: Row(
              children: [
                Icon(e.reference.type == EntryType.tree
                    ? Icons.folder_open
                    : Icons.insert_drive_file),
                const SizedBox(width: 8.0),
                Text(e.name, style: mono),
              ],
            )),
        onTap: e.reference.type == EntryType.tree
            ? () {
                if (e.reference.type == EntryType.tree) {
                  BlocProvider.of<TreeBrowserBloc>(context).add(
                    LoadEntry(entry: e),
                  );
                }
              }
            : () async {
                await Clipboard.setData(ClipboardData(text: e.reference.value));
                // would like to show a Snackbar but we are in an async fn
              },
      );
      final date = DataCell(Text(
        DateFormat.yMd().add_jm().format(e.modTime.toLocal()),
      ));
      final ref = DataCell(Text(e.reference.value, style: mono));
      onSelectChanged(selected) =>
          BlocProvider.of<TreeBrowserBloc>(context).add(
            SetSelection(entry: e, selected: selected),
          );
      final selected = widget.state.selections.contains(e);
      return DataRow(
        cells: [name, date, ref],
        selected: selected,
        onSelectChanged: onSelectChanged,
      );
    }));

    // the sort is modifying the tree nested within the bloc state
    final List<DataColumn> columns = [
      DataColumn(
        label: const Text('Name'),
        onSort: (columnIndex, sortAscending) {
          setState(() {
            if (columnIndex == _sortColumnIndex) {
              _sortAscending = _sortNameAsc = sortAscending;
            } else {
              _sortColumnIndex = columnIndex;
              _sortAscending = _sortNameAsc;
            }
            widget.state.tree.entries.sort(_sortAscending
                ? (a, b) => a.name.compareTo(b.name)
                : (a, b) => b.name.compareTo(a.name));
          });
        },
      ),
      DataColumn(
        label: const Text('Date'),
        onSort: (columnIndex, sortAscending) {
          setState(() {
            if (columnIndex == _sortColumnIndex) {
              _sortAscending = _sortDateAsc = sortAscending;
            } else {
              _sortColumnIndex = columnIndex;
              _sortAscending = _sortDateAsc;
            }
            widget.state.tree.entries.sort(_sortAscending
                ? (a, b) => a.modTime.compareTo(b.modTime)
                : (a, b) => b.modTime.compareTo(a.modTime));
          });
        },
      ),
      DataColumn(
        label: const Text('Reference'),
        onSort: (columnIndex, sortAscending) {
          setState(() {
            if (columnIndex == _sortColumnIndex) {
              _sortAscending = _sortRefAsc = sortAscending;
            } else {
              _sortColumnIndex = columnIndex;
              _sortAscending = _sortRefAsc;
            }
            widget.state.tree.entries.sort(_sortAscending
                ? (a, b) => a.reference.value.compareTo(b.reference.value)
                : (a, b) => b.reference.value.compareTo(a.reference.value));
          });
        },
      ),
    ];

    return SingleChildScrollView(
      scrollDirection: Axis.vertical,
      child: Row(
        children: <Widget>[
          Expanded(
            child: DataTable(
              columns: columns,
              sortColumnIndex: _sortColumnIndex,
              sortAscending: _sortAscending,
              rows: rows,
            ),
          ),
        ],
      ),
    );
  }
}
