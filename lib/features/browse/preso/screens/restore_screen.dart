//
// Copyright (c) 2022 Nathan Fiedler
//
import 'package:flutter/material.dart';
import 'package:flutter_bloc/flutter_bloc.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:intl/intl.dart';
import 'package:oxidized/oxidized.dart';
import 'package:zorigami/core/domain/entities/request.dart';
import 'package:zorigami/features/browse/preso/bloc/restores_bloc.dart';
import 'package:zorigami/features/browse/preso/bloc/providers.dart';
import 'package:zorigami/navigation_drawer.dart';

class RestoreRequestsScreen extends ConsumerWidget {
  @override
  Widget build(BuildContext context, WidgetRef ref) {
    return BlocProvider<RestoresBloc>(
      create: (_) => ref.read(restoresBlocProvider),
      child: BlocConsumer<RestoresBloc, RestoresState>(
        listener: (context, state) {
          if (state is Loaded && state.requestCancelled) {
            ScaffoldMessenger.of(context).showSnackBar(
              const SnackBar(content: Text('Request cancelled')),
            );
          }
        },
        builder: (context, state) {
          return Scaffold(
            appBar: AppBar(
              title: const Text('RESTORE'),
              actions: <Widget>[
                IconButton(
                  icon: const Icon(Icons.refresh),
                  tooltip: 'Refresh',
                  onPressed: () {
                    BlocProvider.of<RestoresBloc>(context).add(LoadRequests());
                  },
                ),
              ],
            ),
            body: buildBody(context, state),
            drawer: MyNavigationDrawer(),
          );
        },
      ),
    );
  }
}

Widget buildBody(BuildContext context, RestoresState state) {
  if (state is Empty) {
    // kick off the initial remote request
    BlocProvider.of<RestoresBloc>(context).add(LoadRequests());
  }
  if (state is Error) {
    return Card(
      child: ListTile(
        title: const Text('Error loading restore requests'),
        subtitle: Text(state.message),
      ),
    );
  }
  if (state is Loaded) {
    if (state.requests.isEmpty) {
      return const Card(
        child: ListTile(
          leading: Icon(Icons.dns),
          title: Text('No restore requests found'),
        ),
      );
    }
    return ListView(
      children: List<Widget>.from(
        state.requests.map((e) => RestoreListEntry(request: e)),
      ),
    );
  }
  return const CircularProgressIndicator();
}

class RestoreListEntry extends StatelessWidget {
  final Request request;

  RestoreListEntry({Key? key, required this.request}) : super(key: key);

  @override
  Widget build(BuildContext context) {
    final subtitle = requestSubtitle(request);
    final inProgress = request.finished is None && request.errorMessage is None;
    final trailing = request.errorMessage is Some
        ? const Icon(Icons.error)
        : request.finished is None
            ? const CircularProgressIndicator()
            : const Icon(Icons.done);
    final onTap = inProgress ? () => _showCancelDialog(context) : null;
    final card = Card(
      child: ListTile(
        leading: const Icon(Icons.archive),
        title: Text(request.filepath),
        subtitle: Text(subtitle),
        trailing: trailing,
        onTap: onTap,
      ),
    );
    if (inProgress) {
      return Tooltip(
        message: 'Click to cancel the pending request.',
        child: card,
      );
    }
    return card;
  }

  void _showCancelDialog(BuildContext contextO) {
    showDialog(
      context: contextO,
      barrierDismissible: true,
      builder: (BuildContext context) {
        return AlertDialog(
          title: const Text('Cancel request?'),
          content: const Text('Do you wish to cancel the restore request?'),
          actions: [
            TextButton(
              onPressed: () {
                BlocProvider.of<RestoresBloc>(contextO).add(
                  CancelRequest(
                    tree: request.tree,
                    entry: request.entry,
                    filepath: request.filepath,
                    dataset: request.dataset,
                  ),
                );
                Navigator.of(context).pop();
              },
              child: const Text('Yes'),
            ),
            ElevatedButton(
              onPressed: () => Navigator.of(context).pop(),
              child: const Text('No'),
            ),
          ],
        );
      },
    );
  }
}

String requestSubtitle(Request request) {
  return request.errorMessage.mapOrElse(
    (err) => 'Restore error: $err',
    () => request.finished.mapOrElse(
      (e) {
        var fin = DateFormat.yMd().add_jm().format(e.toLocal());
        return 'finished at $fin';
      },
      () => '${request.filesRestored} files restored so far...',
    ),
  );
}
