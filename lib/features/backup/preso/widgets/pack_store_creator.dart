//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:flutter/material.dart';
import 'package:flutter_bloc/flutter_bloc.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:zorigami/core/domain/entities/pack_store.dart';
import 'package:zorigami/features/backup/preso/bloc/create_pack_stores_bloc.dart';
import 'package:zorigami/features/backup/preso/bloc/pack_stores_bloc.dart'
    as psb;
import 'package:zorigami/features/backup/preso/bloc/providers.dart';

final List<NewStoreItem> storeItems = [
  // The null kind signals the action button to be disabled, so by default
  // nothing is created until the user selects something.
  NewStoreItem(title: 'Select Type', kind: null),
  NewStoreItem(title: 'Local', kind: StoreKind.local),
  NewStoreItem(title: 'Google', kind: StoreKind.google),
  NewStoreItem(title: 'Minio', kind: StoreKind.minio),
  NewStoreItem(title: 'SFTP', kind: StoreKind.sftp),
];

class PackStoreHeader extends StatelessWidget {
  @override
  Widget build(BuildContext context) {
    return BlocProvider<CreatePackStoresBloc>(
      create: (_) => BuildContextX(context).read(createPackStoresBlocProvider),
      child: BlocListener<CreatePackStoresBloc, CreatePackStoresState>(
        listener: (context, state) {
          if (state is Submitted) {
            ScaffoldMessenger.of(context).showSnackBar(
              SnackBar(content: Text('Created new pack store')),
            );
            BlocProvider.of<psb.PackStoresBloc>(context).add(
              psb.ReloadPackStores(),
            );
          } else if (state is Error) {
            ScaffoldMessenger.of(context).showSnackBar(
              SnackBar(
                content: Text('Error creating pack store: ${state.message}'),
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
            setState(() => selectedItem = item as NewStoreItem);
          },
          items: storeItems
              .map<DropdownMenuItem<NewStoreItem>>((NewStoreItem value) {
            return DropdownMenuItem<NewStoreItem>(
              value: value,
              child: Text(value.title),
            );
          }).toList(),
        ),
        TextButton(
          onPressed: selectedItem.kind == null
              ? null
              : () {
                  final packStore = defaultPackStore(selectedItem.kind!);
                  BlocProvider.of<CreatePackStoresBloc>(context).add(
                    DefinePackStore(
                      store: packStore,
                    ),
                  );
                },
          child: Text('CREATE'),
        ),
      ],
    );
  }
}

class NewStoreItem {
  NewStoreItem({
    required this.title,
    required this.kind,
  });
  final String title;
  final StoreKind? kind;
}

/// Factory to create a generic pack store for the given kind.
PackStore defaultPackStore(StoreKind kind) {
  switch (kind) {
    case StoreKind.local:
      return PackStore(
        kind: StoreKind.local,
        key: 'auto-generated',
        label: 'local',
        options: <String, dynamic>{
          'basepath': '.',
        },
      );
    case StoreKind.google:
      return PackStore(
        kind: StoreKind.google,
        key: 'auto-generated',
        label: 'google',
        options: <String, dynamic>{
          'credentials': '/Users/charlie/credentials.json',
          'project': 'white-sunspot-12345',
          'region': 'us-west1',
          'storage': 'NEARLINE',
        },
      );
    case StoreKind.minio:
      return PackStore(
        kind: StoreKind.minio,
        key: 'auto-generated',
        label: 'minio',
        options: <String, dynamic>{
          'region': 'us-west-1',
          'endpoint': 'http://localhost:9000',
          'access_key': 'AKIAIOSFODNN7EXAMPLE',
          'secret_key': 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
        },
      );
    case StoreKind.sftp:
      return PackStore(
        kind: StoreKind.sftp,
        key: 'auto-generated',
        label: 'sftp',
        options: <String, dynamic>{
          'remote_addr': '127.0.0.1:22',
          'username': 'charlie',
          'password': 'secret123',
          'basepath': '.',
        },
      );
    default:
      throw ArgumentError('kind is not recognized');
  }
}
