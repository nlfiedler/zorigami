//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:flutter/material.dart';
import 'package:flutter_bloc/flutter_bloc.dart';
import 'package:flutter_form_builder/flutter_form_builder.dart';
import 'package:zorigami/container.dart';
import 'package:zorigami/core/domain/entities/pack_store.dart';
import 'package:zorigami/features/backup/preso/bloc/edit_pack_stores_bloc.dart';
import 'package:zorigami/features/backup/preso/bloc/pack_stores_bloc.dart';
import 'package:zorigami/features/backup/preso/widgets/pack_store_form.dart';

class PackStoresList extends StatefulWidget {
  final List<PackStore> stores;

  PackStoresList({Key key, @required this.stores}) : super(key: key);

  @override
  _PackStoresListState createState() => _PackStoresListState();
}

class _PackStoresListState extends State<PackStoresList> {
  final formKey = GlobalKey<FormBuilderState>();
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
            child: PackStoreListDetails(formKey: formKey, store: e),
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
    );
  }
}

class PackStoreListDetails extends StatelessWidget {
  const PackStoreListDetails({
    Key key,
    @required this.formKey,
    @required this.store,
  }) : super(key: key);

  final GlobalKey<FormBuilderState> formKey;
  final PackStore store;

  void savePack(BuildContext context, PackStoreForm storeForm) {
    if (formKey.currentState.saveAndValidate()) {
      final options = storeForm.optionsFromState(
        formKey.currentState,
      );
      BlocProvider.of<EditPackStoresBloc>(context).add(
        UpdatePackStore(
          key: formKey.currentState.value['key'],
          options: options,
        ),
      );
    }
  }

  @override
  Widget build(BuildContext context) {
    final storeForm = getIt<PackStoreForm>(param1: store);
    return BlocProvider<EditPackStoresBloc>(
      create: (_) => getIt<EditPackStoresBloc>(),
      child: BlocBuilder<EditPackStoresBloc, EditPackStoresState>(
        builder: (context, state) {
          if (state is Submitted) {
            // this will force everything to rebuild
            BlocProvider.of<PackStoresBloc>(context).add(ReloadPackStores());
          }
          return Column(
            children: <Widget>[
              FormBuilder(
                key: formKey,
                initialValue: storeForm.initialValuesFrom(store),
                autovalidate: true,
                child: storeForm,
                // not convinced this readOnly is effective
                readOnly: (state is Submitting),
              ),
              ButtonBar(
                children: <Widget>[
                  FlatButton.icon(
                    icon: Icon(Icons.save),
                    label: const Text('SAVE'),
                    onPressed: (state is Submitting)
                        ? null
                        : () => savePack(context, storeForm),
                  ),
                  FlatButton.icon(
                    icon: Icon(Icons.delete),
                    label: const Text('DELETE'),
                    onPressed: (state is Submitting) ? null : () {/* TODO */},
                  ),
                ],
              )
            ],
          );
        },
      ),
    );
  }
}

String packStoreTitle(PackStore store) {
  return store.label + ' :: ' + prettyKind(store.kind);
}

String packStoreSubtitle(PackStore store) {
  switch (store.kind) {
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
