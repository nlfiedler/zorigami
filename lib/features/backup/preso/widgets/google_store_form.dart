//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:flutter/material.dart';
import 'package:flutter_form_builder/flutter_form_builder.dart';
import 'package:zorigami/core/domain/entities/pack_store.dart';
import 'package:zorigami/features/backup/preso/widgets/pack_store_form.dart';

class GoogleStoreForm extends PackStoreForm {
  final PackStore store;

  GoogleStoreForm({Key key, @required this.store}) : super(key: key);

  @override
  Map<String, dynamic> initialValuesFrom(PackStore store) {
    return {
      'key': store.key,
      'label': store.label,
      'credentials': store.options['credentials'],
      'project': store.options['project'],
      'storage': store.options['storage'],
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
        'storage': state.value['storage'],
      },
    );
  }

  @override
  Widget build(BuildContext context) {
    return Column(
      children: <Widget>[
        FormBuilderTextField(
          attribute: 'key',
          decoration: InputDecoration(
            icon: Icon(Icons.vpn_key),
            labelText: 'Store Key',
          ),
          readOnly: true,
        ),
        FormBuilderTextField(
          attribute: 'label',
          decoration: const InputDecoration(
            icon: Icon(Icons.label),
            labelText: 'Label',
          ),
          validators: [FormBuilderValidators.required()],
        ),
        FormBuilderTextField(
          attribute: 'credentials',
          decoration: const InputDecoration(
            icon: Icon(Icons.attachment),
            labelText: 'Credentials File',
          ),
          validators: [FormBuilderValidators.required()],
        ),
        FormBuilderTextField(
          attribute: 'project',
          decoration: const InputDecoration(
            icon: Icon(Icons.folder),
            labelText: 'Project ID',
          ),
          validators: [FormBuilderValidators.required()],
        ),
        FormBuilderDropdown(
          attribute: 'storage',
          decoration: const InputDecoration(
            icon: Icon(Icons.storage),
            labelText: 'Storage Class',
          ),
          hint: Text('Select storage class'),
          validators: [FormBuilderValidators.required()],
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
