//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:flutter/material.dart';
import 'package:flutter_bloc/flutter_bloc.dart';
import 'package:zorigami/container.dart';
import 'package:zorigami/features/backup/preso/bloc/pack_stores_bloc.dart';
import 'package:zorigami/features/backup/preso/widgets/pack_store_creator.dart';
import 'package:zorigami/features/backup/preso/widgets/pack_stores_list.dart';
import 'package:zorigami/navigation_drawer.dart';

class PackStoresScreen extends StatelessWidget {
  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: Text('PACK STORES'),
      ),
      body: BlocProvider<PackStoresBloc>(
        create: (_) => getIt<PackStoresBloc>(),
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
              return Column(
                children: <Widget>[
                  BlocProvider.value(
                    value: BlocProvider.of<PackStoresBloc>(context),
                    child: PackStoreHeader(),
                  ),
                  Expanded(child: PackStoresList(stores: state.stores)),
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
