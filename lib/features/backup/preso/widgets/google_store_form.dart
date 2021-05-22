//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:flutter/material.dart';
import 'package:flutter_form_builder/flutter_form_builder.dart';
import 'package:zorigami/core/domain/entities/pack_store.dart';
import 'package:zorigami/features/backup/preso/widgets/pack_store_form.dart';

class GoogleStoreForm extends PackStoreForm {
  final PackStore store;

  GoogleStoreForm({Key? key, required this.store}) : super(key: key);

  @override
  Map<String, dynamic> initialValuesFrom(PackStore store) {
    final region = store.options['region'] ?? '';
    final storage = store.options['storage'] ?? '';
    return {
      'key': store.key,
      'label': store.label,
      'credentials': store.options['credentials'],
      'project': store.options['project'],
      'region': region,
      'storage': storage,
    };
  }

  @override
  PackStore storeFromState(FormBuilderState state) {
    final String region = state.value['region'];
    final String storage = state.value['storage'];
    return PackStore(
      key: state.value['key'],
      label: state.value['label'],
      kind: StoreKind.google,
      options: {
        'credentials': state.value['credentials'],
        'project': state.value['project'],
        'region': region.isEmpty ? null : region,
        'storage': storage.isEmpty ? null : storage,
      },
    );
  }

  @override
  Widget build(BuildContext context) {
    return Column(
      children: <Widget>[
        FormBuilderTextField(
          name: 'key',
          decoration: InputDecoration(
            icon: Icon(Icons.vpn_key),
            labelText: 'Store Key',
          ),
          readOnly: true,
        ),
        FormBuilderTextField(
          name: 'label',
          decoration: const InputDecoration(
            icon: Icon(Icons.label),
            labelText: 'Label',
          ),
          validator: FormBuilderValidators.required(context),
        ),
        FormBuilderTextField(
          name: 'credentials',
          decoration: const InputDecoration(
            icon: Icon(Icons.attachment),
            labelText: 'Credentials File',
          ),
          validator: FormBuilderValidators.required(context),
        ),
        FormBuilderTextField(
          name: 'project',
          decoration: const InputDecoration(
            icon: Icon(Icons.folder),
            labelText: 'Project ID',
          ),
          validator: FormBuilderValidators.required(context),
        ),
        FormBuilderTextField(
          name: 'region',
          decoration: const InputDecoration(
            icon: Icon(Icons.location_on),
            labelText: 'Region',
          ),
        ),
        FormBuilderDropdown(
          name: 'storage',
          decoration: const InputDecoration(
            icon: Icon(Icons.storage),
            labelText: 'Storage Class',
          ),
          hint: Text('Select storage class'),
          items: ['STANDARD', 'NEARLINE', 'COLDLINE', 'ARCHIVE']
              .map(
                (sclass) => DropdownMenuItem(
                  value: sclass,
                  child: Text('$sclass'),
                ),
              )
              .toList(),
        ),
      ],
    );
  }
}
