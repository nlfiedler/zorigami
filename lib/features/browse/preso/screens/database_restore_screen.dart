//
// Copyright (c) 2024 Nathan Fiedler
//
import 'package:flutter/material.dart';
import 'package:flutter_bloc/flutter_bloc.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:zorigami/core/domain/entities/pack_store.dart';
import 'package:zorigami/features/backup/preso/bloc/pack_stores_bloc.dart';
import 'package:zorigami/features/backup/preso/bloc/providers.dart';
import 'package:zorigami/features/browse/preso/bloc/database_restore_bloc.dart'
    as rdb;
import 'package:zorigami/features/browse/preso/bloc/providers.dart';
import 'package:zorigami/navigation_drawer.dart';

class DatabaseRestoreScreen extends ConsumerWidget {
  const DatabaseRestoreScreen({super.key});

  @override
  Widget build(BuildContext context, WidgetRef ref) {
    return Scaffold(
      appBar: AppBar(
        title: const Text('RESTORE'),
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
                  title: const Text('Error loading pack stores'),
                  subtitle: Text(state.message),
                ),
              );
            }
            if (state is Loaded) {
              if (state.stores.isEmpty) {
                return buildNoStoresHelp(context);
              }
              const helpTile = Card(
                child: ListTile(
                  leading: Icon(Icons.dns),
                  title: Text('Select a pack store'),
                  subtitle: Text(
                    'Choose a pack store from which to restore the database',
                  ),
                ),
              );
              final stores = List<Widget>.from(
                state.stores.map((e) => PackStoreListEntry(store: e)),
              );
              return ListView(
                children: [helpTile, ...stores],
              );
            }
            return const CircularProgressIndicator();
          },
        ),
      ),
      drawer: MyNavigationDrawer(),
    );
  }
}

Widget buildNoStoresHelp(BuildContext context) {
  return Card(
    child: ListTile(
      leading: const Icon(Icons.dns),
      title: const Text('No pack stores found'),
      subtitle: const Text(
        'Click here to create a pack store',
      ),
      trailing: const Icon(Icons.chevron_right),
      onTap: () => Navigator.pushNamed(context, '/stores'),
    ),
  );
}

class PackStoreListEntry extends ConsumerWidget {
  final PackStore store;

  const PackStoreListEntry({super.key, required this.store});

  @override
  Widget build(BuildContext context, WidgetRef ref) {
    final title = packStoreTitle(store);
    final subtitle = packStoreSubtitle(store);
    return BlocProvider<rdb.DatabaseRestoreBloc>(
      create: (_) => ref.read(databaseRestoreBlocProvider),
      child: BlocConsumer<rdb.DatabaseRestoreBloc, rdb.DatabaseRestoreState>(
        listener: (context, state) {
          final resultStatus = _loadedResult(state);
          if (resultStatus != null) {
            ScaffoldMessenger.of(context).showSnackBar(
              SnackBar(content: Text(resultStatus)),
            );
          }
        },
        builder: (context, state) {
          final trailing = state is rdb.Loading
              ? const CircularProgressIndicator()
              : const Icon(Icons.restore);
          final onTap =
              state is rdb.Loading ? null : () => _showRestoreDialog(context);
          return Card(
            child: ListTile(
              leading: const Icon(Icons.archive),
              title: Text(title),
              subtitle: Text(subtitle),
              trailing: trailing,
              onTap: onTap,
            ),
          );
        },
      ),
    );
  }

  void _showRestoreDialog(BuildContext contextO) {
    showDialog(
      context: contextO,
      barrierDismissible: true,
      builder: (BuildContext context) {
        return AlertDialog(
          title: const Text('Restore Database?'),
          content: const Text('This will overwrite the current database.'),
          actions: [
            TextButton(
              onPressed: () => Navigator.of(context).pop(),
              child: const Text('Cancel'),
            ),
            ElevatedButton(
              onPressed: () {
                BlocProvider.of<rdb.DatabaseRestoreBloc>(contextO).add(
                  rdb.RestoreDatabase(storeId: store.key),
                );
                Navigator.of(context).pop();
              },
              child: const Text('Restore'),
            ),
          ],
        );
      },
    );
  }

  String? _loadedResult(rdb.DatabaseRestoreState state) {
    if (state is rdb.Loaded) {
      if (state.result == 'ok') {
        return 'Database restore successful.';
      }
      return 'Database restore failed: ${state.result}';
    }
    if (state is rdb.Error) {
      return 'Database restore error: ${state.message}';
    }
    return null;
  }
}
