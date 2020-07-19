//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:flutter/material.dart';
import 'package:flutter_bloc/flutter_bloc.dart';
import 'package:zorigami/container.dart';
import 'package:zorigami/core/domain/entities/pack_store.dart';
import 'package:zorigami/features/backup/preso/bloc/create_pack_stores_bloc.dart';
import 'package:zorigami/features/backup/preso/bloc/pack_stores_bloc.dart'
    as psb;

final List<NewStoreItem> storeItems = [
  // The null kind signals the action button to be disabled, so by default
  // nothing is created until the user selects something.
  NewStoreItem(title: 'Select Type', kind: null),
  NewStoreItem(title: 'Local', kind: 'local'),
  NewStoreItem(title: 'Google', kind: 'google'),
  NewStoreItem(title: 'Minio', kind: 'minio'),
  NewStoreItem(title: 'SFTP', kind: 'sftp'),
];

class PackStoreHeader extends StatelessWidget {
  @override
  Widget build(BuildContext context) {
    return BlocProvider<CreatePackStoresBloc>(
      create: (_) => getIt<CreatePackStoresBloc>(),
      child: BlocListener<CreatePackStoresBloc, CreatePackStoresState>(
        listener: (context, state) {
          if (state is Submitted) {
            Scaffold.of(context).showSnackBar(
              SnackBar(
                content: Text('Created new pack store'),
              ),
            );
            BlocProvider.of<psb.PackStoresBloc>(context).add(
              psb.ReloadPackStores(),
            );
          } else if (state is Error) {
            Scaffold.of(context).showSnackBar(
              SnackBar(
                content: ListTile(
                  title: Text('Error creating pack store'),
                  subtitle: Text(state.message),
                ),
              ),
            );
          }
        },
        child: PackStoreCreator(),
      ),
    );
  }
}

class PackStoreCreator extends StatefulWidget {
  @override
  _PackStoreCreatorState createState() => _PackStoreCreatorState();
}

class _PackStoreCreatorState extends State<PackStoreCreator> {
  NewStoreItem selectedItem = storeItems[0];

  @override
  Widget build(BuildContext context) {
    return Row(
      mainAxisAlignment: MainAxisAlignment.end,
      children: <Widget>[
        Text('Create new store:'),
        SizedBox(width: 16),
        DropdownButton<NewStoreItem>(
          value: selectedItem,
          onChanged: (item) {
            setState(() => selectedItem = item);
          },
          items: storeItems
              .map<DropdownMenuItem<NewStoreItem>>((NewStoreItem value) {
            return DropdownMenuItem<NewStoreItem>(
              value: value,
              child: Text(value.title),
            );
          }).toList(),
        ),
        FlatButton(
          child: Text('CREATE'),
          onPressed: selectedItem.kind == null
              ? null
              : () {
                  final packStore = getIt<PackStore>(param1: selectedItem.kind);
                  BlocProvider.of<CreatePackStoresBloc>(context).add(
                    DefinePackStore(
                      store: packStore,
                    ),
                  );
                },
        ),
      ],
    );
  }
}

class NewStoreItem {
  NewStoreItem({
    @required this.title,
    @required this.kind,
  });
  final String title;
  final String kind;
}
