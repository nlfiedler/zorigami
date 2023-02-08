//
// Copyright (c) 2023 Nathan Fiedler
//
import 'package:flutter/material.dart';
import 'package:form_builder_validators/form_builder_validators.dart';
import 'package:flutter_form_builder/flutter_form_builder.dart';
import 'package:zorigami/core/domain/entities/pack_store.dart';
import 'package:zorigami/features/backup/preso/widgets/pack_store_form.dart';

class AmazonStoreForm extends PackStoreForm {
  final PackStore store;

  AmazonStoreForm({Key? key, required this.store}) : super(key: key);

  @override
  Map<String, dynamic> initialValuesFrom(PackStore store) {
    final region = store.options['region'] ?? '';
    final storage = store.options['storage'] ?? '';
    return {
      'key': store.key,
      'label': store.label,
      'region': region,
      'storage': storage,
      'access_key': store.options['access_key'],
      'secret_key': store.options['secret_key'],
    };
  }

  @override
  PackStore storeFromState(FormBuilderState state) {
    final String storage = state.value['storage'];
    return PackStore(
      key: state.value['key'],
      label: state.value['label'],
      kind: StoreKind.amazon,
      options: {
        'region': state.value['region'],
        'storage': storage.isEmpty ? null : storage,
        'access_key': state.value['access_key'],
        'secret_key': state.value['secret_key'],
      },
    );
  }

  @override
  Widget build(BuildContext context) {
    return Column(
      children: <Widget>[
        FormBuilderTextField(
          name: 'key',
          decoration: const InputDecoration(
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
          validator: FormBuilderValidators.required(),
        ),
        FormBuilderTextField(
          name: 'region',
          decoration: const InputDecoration(
            icon: Icon(Icons.folder_open),
            labelText: 'Region',
          ),
          validator: FormBuilderValidators.required(),
        ),
        FormBuilderDropdown(
          name: 'storage',
          decoration: const InputDecoration(
            icon: Icon(Icons.storage),
            labelText: 'Storage Class',
            hintText: 'Select storage class',
          ),
          items: ['STANDARD', 'STANDARD_IA', 'GLACIER_IR']
              .map(
                (sclass) => DropdownMenuItem(
                  value: sclass,
                  child: Text(sclass),
                ),
              )
              .toList(),
        ),
        FormBuilderTextField(
          name: 'access_key',
          decoration: const InputDecoration(
            icon: Icon(Icons.folder_open),
            labelText: 'Access Key',
          ),
          validator: FormBuilderValidators.required(),
        ),
        FormBuilderTextField(
          name: 'secret_key',
          obscureText: true,
          maxLines: 1,
          decoration: const InputDecoration(
            icon: Icon(Icons.folder_open),
            labelText: 'Secret Key',
          ),
          validator: FormBuilderValidators.required(),
        ),
      ],
    );
  }
}
