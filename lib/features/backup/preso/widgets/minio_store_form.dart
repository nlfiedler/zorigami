//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:flutter/material.dart';
import 'package:flutter_form_builder/flutter_form_builder.dart';
import 'package:zorigami/core/domain/entities/pack_store.dart';
import 'package:zorigami/features/backup/preso/widgets/pack_store_form.dart';

class MinioStoreForm extends PackStoreForm {
  final PackStore store;

  MinioStoreForm({Key key, @required this.store}) : super(key: key);

  @override
  Map<String, dynamic> initialValuesFrom(PackStore store) {
    return {
      'key': store.key,
      'label': store.label,
      'region': store.options['region'],
      'endpoint': store.options['endpoint'],
      'access_key': store.options['access_key'],
      'secret_key': store.options['secret_key'],
    };
  }

  @override
  PackStore storeFromState(FormBuilderState state) {
    return PackStore(
      key: state.value['key'],
      label: state.value['label'],
      kind: StoreKind.minio,
      options: {
        'region': state.value['region'],
        'endpoint': state.value['endpoint'],
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
          attribute: 'region',
          decoration: const InputDecoration(
            icon: Icon(Icons.folder_open),
            labelText: 'Region',
          ),
          validators: [FormBuilderValidators.required()],
        ),
        FormBuilderTextField(
          attribute: 'endpoint',
          decoration: const InputDecoration(
            icon: Icon(Icons.cloud),
            labelText: 'Endpoint',
          ),
          validators: [FormBuilderValidators.url()],
        ),
        FormBuilderTextField(
          attribute: 'access_key',
          decoration: const InputDecoration(
            icon: Icon(Icons.folder_open),
            labelText: 'Access Key',
          ),
          validators: [FormBuilderValidators.required()],
        ),
        FormBuilderTextField(
          attribute: 'secret_key',
          obscureText: true,
          maxLines: 1,
          decoration: const InputDecoration(
            icon: Icon(Icons.folder_open),
            labelText: 'Secret Key',
          ),
          validators: [FormBuilderValidators.required()],
        ),
      ],
    );
  }
}
