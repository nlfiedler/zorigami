//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:flutter/material.dart';
import 'package:flutter_bloc/flutter_bloc.dart';
import 'package:flutter_form_builder/flutter_form_builder.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:zorigami/core/domain/entities/pack_store.dart';
import 'package:zorigami/features/backup/preso/bloc/edit_pack_stores_bloc.dart'
    as epsb;
import 'package:zorigami/features/backup/preso/bloc/pack_stores_bloc.dart';
import 'package:zorigami/features/backup/preso/bloc/providers.dart';
import 'package:zorigami/features/backup/preso/widgets/google_store_form.dart';
import 'package:zorigami/features/backup/preso/widgets/local_store_form.dart';
import 'package:zorigami/features/backup/preso/widgets/minio_store_form.dart';
import 'package:zorigami/features/backup/preso/widgets/pack_store_form.dart';
import 'package:zorigami/features/backup/preso/widgets/sftp_store_form.dart';

class PackStoresList extends StatefulWidget {
  final List<PackStore> stores;

  PackStoresList({Key key, @required this.stores}) : super(key: key);

  @override
  _PackStoresListState createState() => _PackStoresListState();
}

class _PackStoresListState extends State<PackStoresList> {
  List<ExpansionItem> items;

  @override
  void initState() {
    super.initState();
    items = List<ExpansionItem>.from(
      widget.stores.map((e) {
        final title = packStoreTitle(e);
        final subtitle = packStoreSubtitle(e);
        final headerValue = ListTile(
          leading: Icon(Icons.archive),
          title: Text(title),
          subtitle: Text(subtitle),
        );
        final expandedValue = Card(
          child: Padding(
            padding: const EdgeInsets.symmetric(
              vertical: 8.0,
              horizontal: 32.0,
            ),
            child: PackStoreListDetails(store: e),
          ),
        );
        return ExpansionItem(
          headerValue: headerValue,
          expandedValue: expandedValue,
        );
      }),
    );
  }

  @override
  Widget build(BuildContext context) {
    return SingleChildScrollView(
      child: Container(
        child: BlocProvider<epsb.EditPackStoresBloc>(
          create: (_) =>
              BuildContextX(context).read(editPackStoresBlocProvider),
          child: ExpansionPanelList(
            expansionCallback: (int index, bool isExpanded) {
              setState(() {
                items[index].isExpanded = !isExpanded;
              });
            },
            children: items.map<ExpansionPanel>((ExpansionItem item) {
              return ExpansionPanel(
                canTapOnHeader: true,
                headerBuilder: (BuildContext context, bool isExpanded) {
                  return item.headerValue;
                },
                body: item.expandedValue,
                isExpanded: item.isExpanded,
              );
            }).toList(),
          ),
        ),
      ),
    );
  }
}

class PackStoreListDetails extends StatelessWidget {
  PackStoreListDetails({
    Key key,
    @required this.store,
  }) : super(key: key);

  final formKey = GlobalKey<FormBuilderState>();
  final PackStore store;

  void savePack(BuildContext context, PackStoreForm storeForm) {
    if (formKey.currentState.saveAndValidate()) {
      final store = storeForm.storeFromState(
        formKey.currentState,
      );
      BlocProvider.of<epsb.EditPackStoresBloc>(context).add(
        epsb.UpdatePackStore(store: store),
      );
    }
  }

  @override
  Widget build(BuildContext context) {
    final storeForm = buildStoreForm(store);
    return BlocConsumer<epsb.EditPackStoresBloc, epsb.EditPackStoresState>(
      listener: (context, state) {
        if (state is epsb.Submitted) {
          // this will force everything to rebuild
          BlocProvider.of<PackStoresBloc>(context).add(ReloadPackStores());
        } else if (state is epsb.Error) {
          ScaffoldMessenger.of(context).showSnackBar(
            SnackBar(
              content: Text('Error updating pack store: ${state.message}'),
            ),
          );
        }
      },
      builder: (context, state) {
        return Column(
          children: <Widget>[
            FormBuilder(
              key: formKey,
              initialValue: storeForm.initialValuesFrom(store),
              autovalidate: true,
              child: storeForm,
              // not convinced this readOnly is effective
              readOnly: (state is epsb.Submitting),
            ),
            ButtonBar(
              children: <Widget>[
                RaisedButton.icon(
                  icon: Icon(Icons.save),
                  label: const Text('SAVE'),
                  onPressed: (state is epsb.Submitting)
                      ? null
                      : () => savePack(context, storeForm),
                ),
                FlatButton.icon(
                  icon: Icon(Icons.delete),
                  label: const Text('DELETE'),
                  onPressed: (state is epsb.Submitting)
                      ? null
                      : () {
                          BlocProvider.of<epsb.EditPackStoresBloc>(context).add(
                            epsb.DeletePackStore(store: store),
                          );
                        },
                ),
              ],
            )
          ],
        );
      },
    );
  }
}

String packStoreTitle(PackStore store) {
  return store.label + ' :: ' + prettyKind(store.kind);
}

String packStoreSubtitle(PackStore store) {
  switch (store.kind) {
    case StoreKind.google:
      return store.options['project'];
    case StoreKind.local:
      return store.options['basepath'];
    case StoreKind.minio:
      return store.options['endpoint'];
    case StoreKind.sftp:
      return store.options['remote_addr'];
    default:
      throw ArgumentError('kind is not recognized');
  }
}

String prettyKind(StoreKind kind) {
  switch (kind) {
    case StoreKind.local:
      return 'local disk';
    case StoreKind.google:
      return 'remote google';
    case StoreKind.minio:
      return 'remote minio';
    case StoreKind.sftp:
      return 'remote SFTP';
    default:
      throw ArgumentError('kind is not recognized');
  }
}

class ExpansionItem {
  ExpansionItem({
    this.expandedValue,
    this.headerValue,
    this.isExpanded = false,
  });
  Widget expandedValue;
  Widget headerValue;
  bool isExpanded;
}

/// Factory to build form widgets for pack store details.
PackStoreForm buildStoreForm(PackStore store) {
  if (store.kind == StoreKind.local) {
    return LocalStoreForm(store: store);
  }
  if (store.kind == StoreKind.google) {
    return GoogleStoreForm(store: store);
  }
  if (store.kind == StoreKind.minio) {
    return MinioStoreForm(store: store);
  }
  if (store.kind == StoreKind.sftp) {
    return SftpStoreForm(store: store);
  }
  return null;
}
