//
// Copyright (c) 2024 Nathan Fiedler
//
import 'package:flutter/material.dart';
import 'package:form_builder_validators/form_builder_validators.dart';
import 'package:flutter_form_builder/flutter_form_builder.dart';
import 'package:zorigami/core/domain/entities/pack_store.dart';
import 'package:zorigami/features/backup/preso/widgets/pack_store_form.dart';

class AzureStoreForm extends PackStoreForm {
  final PackStore store;

  AzureStoreForm({Key? key, required this.store}) : super(key: key);

  @override
  Map<String, dynamic> initialValuesFrom(PackStore store) {
    final customUri = store.options['custom_uri'] ?? '';
    return {
      'key': store.key,
      'label': store.label,
      'account': store.options['account'],
      'access_key': store.options['access_key'],
      'custom_uri': customUri,
      'access_tier': store.options['access_tier'],
    };
  }

  @override
  PackStore storeFromState(FormBuilderState state) {
    final Map<String, dynamic> options = {
      'account': state.value['account'],
      'access_key': state.value['access_key'],
      'access_tier': state.value['access_tier'],
    };
    final String customUri = state.value['custom_uri'];
    if (customUri.isNotEmpty) {
      options['custom_uri'] = customUri;
    }
    return PackStore(
      key: state.value['key'],
      label: state.value['label'],
      kind: StoreKind.azure,
      options: options,
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
          name: 'account',
          decoration: const InputDecoration(
            icon: Icon(Icons.account_box),
            labelText: 'Account',
          ),
          validator: FormBuilderValidators.required(),
        ),
        FormBuilderTextField(
          name: 'access_key',
          decoration: const InputDecoration(
            icon: Icon(Icons.folder_open),
            labelText: 'Access Key',
          ),
          validator: FormBuilderValidators.required(),
        ),
        FormBuilderDropdown(
          name: 'access_tier',
          decoration: const InputDecoration(
            icon: Icon(Icons.storage),
            labelText: 'Access Tier',
            hintText: 'Select access tier',
          ),
          items: ['Hot', 'Cool']
              .map(
                (sclass) => DropdownMenuItem(
                  value: sclass,
                  child: Text(sclass),
                ),
              )
              .toList(),
        ),
        FormBuilderTextField(
          name: 'custom_uri',
          decoration: const InputDecoration(
            icon: Icon(Icons.link),
            labelText: 'Custom URI',
          ),
        ),
      ],
    );
  }
}
