//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:flutter/material.dart';
import 'package:flutter_bloc/flutter_bloc.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:zorigami/features/backup/preso/bloc/pack_stores_bloc.dart';
import 'package:zorigami/features/backup/preso/bloc/providers.dart';
import 'package:zorigami/features/backup/preso/widgets/pack_store_creator.dart';
import 'package:zorigami/features/backup/preso/widgets/pack_stores_list.dart';
import 'package:zorigami/navigation_drawer.dart';

class PackStoresScreen extends ConsumerWidget {
  @override
  Widget build(BuildContext context, WidgetRef ref) {
    return Scaffold(
      appBar: AppBar(
        title: Text('PACK STORES'),
      ),
      body: BlocProvider<PackStoresBloc>(
        create: (_) => ref.read(packStoresBlocProvider),
        child: BlocBuilder<PackStoresBloc, PackStoresState>(
          builder: (context, state) {
            if (state is Empty) {
              // kick off the initial remote request
              BlocProvider.of<PackStoresBloc>(context).add(LoadAllPackStores());
            }
            if (state is Error) {
              return Card(
                child: ListTile(
                  title: Text('Error loading pack stores'),
                  subtitle: Text(state.message),
                ),
              );
            }
            if (state is Loaded) {
              final body = state.stores.isEmpty
                  ? buildHelp(context)
                  : Expanded(child: PackStoresList(stores: state.stores));
              return Column(
                children: <Widget>[
                  BlocProvider.value(
                    value: BlocProvider.of<PackStoresBloc>(context),
                    child: PackStoreHeader(),
                  ),
                  body,
                ],
              );
            }
            return CircularProgressIndicator();
          },
        ),
      ),
      drawer: NavigationDrawer(),
    );
  }
}

Widget buildHelp(BuildContext context) {
  return Card(
    child: ListTile(
      leading: Icon(Icons.dns),
      title: Text('No pack stores found'),
      subtitle: Text('Use the form above to create a store.'),
    ),
  );
}
