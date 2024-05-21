//
// Copyright (c) 2024 Nathan Fiedler
//
import 'package:flutter/material.dart';
import 'package:form_builder_validators/form_builder_validators.dart';
import 'package:flutter_form_builder/flutter_form_builder.dart';
import 'package:zorigami/core/domain/entities/pack_store.dart';
import 'package:zorigami/features/backup/preso/widgets/pack_store_form.dart';

class LocalStoreForm extends PackStoreForm {
  final PackStore store;

  const LocalStoreForm({super.key, required this.store});

  @override
  Map<String, dynamic> initialValuesFrom(PackStore store) {
    return {
      'key': store.key,
      'label': store.label,
      'basepath': store.options['basepath']
    };
  }

  @override
  PackStore storeFromState(FormBuilderState state) {
    return PackStore(
      key: state.value['key'],
      label: state.value['label'],
      kind: StoreKind.local,
      options: {'basepath': state.value['basepath']},
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
          name: 'basepath',
          decoration: const InputDecoration(
            icon: Icon(Icons.folder_open),
            labelText: 'Base Path',
          ),
          validator: FormBuilderValidators.required(),
        ),
      ],
    );
  }
}
