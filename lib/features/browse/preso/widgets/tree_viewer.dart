//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:flutter/material.dart';
import 'package:flutter_bloc/flutter_bloc.dart';
import 'package:intl/intl.dart';
import 'package:zorigami/core/domain/entities/tree.dart';
import 'package:zorigami/features/browse/preso/bloc/tree_browser_bloc.dart';

// tree browser bloc logic:
// --> onTap for "file" entry
// 1) use onSelectChanged param in DataRow constructor
// 2) fire ToggleSelection
// 3) set `selected` parameter of the DataRow based on bloc state
//
// --> appbar action restore-selection(s)
// 1) already has the Loaded state, uses that to get selections

class TreeViewer extends StatelessWidget {
  final String rootTree;

  TreeViewer({Key key, @required this.rootTree}) : super(key: key);

  @override
  Widget build(BuildContext context) {
    return Container(
      child: BlocBuilder<TreeBrowserBloc, TreeBrowserState>(
        builder: (context, state) {
          if (state is Empty) {
            // kick off the initial remote request
            BlocProvider.of<TreeBrowserBloc>(context).add(
              LoadTree(digest: rootTree),
            );
            return Text('Starting...');
          }
          if (state is Error) {
            return Text('Error: ' + state.message);
          }
          if (state is Loaded) {
            return Column(
              children: <Widget>[
                TreePath(path: state.path),
                Expanded(child: TreeTable(state: state)),
              ],
            );
          }
          return Text('Loading...');
        },
      ),
    );
  }
}

class TreePath extends StatelessWidget {
  final List<String> path;

  TreePath({@required this.path});

  @override
  Widget build(BuildContext context) {
    return Padding(
      padding: const EdgeInsets.all(8.0),
      child: Row(
        children: <Widget>[
          RaisedButton(
            child: Text('Up'),
            onPressed: path.isNotEmpty
                ? () => BlocProvider.of<TreeBrowserBloc>(context).add(
                      NavigateUpward(),
                    )
                : null,
          ),
          SizedBox(width: 56.0),
          Text(
            'Path:',
            style: TextStyle(fontWeight: FontWeight.bold),
          ),
          Text(
            ' / ${path.join(' / ')}',
            style: TextStyle(fontFamily: 'RobotoMono'),
          )
        ],
      ),
    );
  }
}

class TreeTable extends StatefulWidget {
  final Loaded state;

  TreeTable({Key key, @required this.state}) : super(key: key);

  @override
  _TreeTableState createState() => _TreeTableState();
}

class _TreeTableState extends State<TreeTable> {
  bool _sortNameAsc = true;
  bool _sortDateAsc = true;
  bool _sortRefAsc = true;
  bool _sortAscending = true;
  int _sortColumnIndex;

  @override
  Widget build(BuildContext context) {
    final mono = TextStyle(fontFamily: 'RobotoMono');
    final List<DataRow> rows = List.of(widget.state.tree.entries.map((e) {
      final icon = DataCell(
        Icon(e.reference.type == EntryType.tree
            ? Icons.folder_open
            : Icons.insert_drive_file),
      );
      final name = DataCell(
        Text(e.name, style: mono),
        onTap: e.reference.type == EntryType.tree
            ? () => BlocProvider.of<TreeBrowserBloc>(context).add(
                  LoadEntry(entry: e),
                )
            : null,
      );
      final date = DataCell(Text(
        DateFormat.yMd().add_jm().format(e.modTime.toLocal()),
      ));
      final ref = DataCell(Text(e.reference.value, style: mono));
      return DataRow(cells: [icon, name, date, ref]);
    }));

    // the sort is modifying the tree nested within the bloc state
    final List<DataColumn> columns = [
      DataColumn(label: Text('Type')),
      DataColumn(
        label: Text('Name'),
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
        label: Text('Date'),
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
        label: Text('Reference'),
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
