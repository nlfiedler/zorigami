//
// Copyright (c) 2023 Nathan Fiedler
//
import 'package:flutter/material.dart';
import 'package:form_builder_validators/form_builder_validators.dart';
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
    return PackStore(
      key: state.value['key'],
      label: state.value['label'],
      kind: StoreKind.google,
      options: {
        'credentials': state.value['credentials'],
        'project': state.value['project'],
        'region': state.value['region'],
        'storage': state.value['storage'],
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
          name: 'credentials',
          decoration: const InputDecoration(
            icon: Icon(Icons.attachment),
            labelText: 'Credentials File',
          ),
          validator: FormBuilderValidators.required(),
        ),
        FormBuilderTextField(
          name: 'project',
          decoration: const InputDecoration(
            icon: Icon(Icons.folder),
            labelText: 'Project ID',
          ),
          validator: FormBuilderValidators.required(),
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
            hintText: 'Select storage class',
          ),
          items: ['STANDARD', 'NEARLINE', 'COLDLINE', 'ARCHIVE']
              .map(
                (sclass) => DropdownMenuItem(
                  value: sclass,
                  child: Text(sclass),
                ),
              )
              .toList(),
        ),
      ],
    );
  }
}
