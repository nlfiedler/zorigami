//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:flutter/material.dart';
import 'package:flutter_form_builder/flutter_form_builder.dart';
import 'package:zorigami/core/domain/entities/pack_store.dart';
import 'package:zorigami/features/backup/preso/widgets/pack_store_form.dart';

class LocalStoreForm extends PackStoreForm {
  final PackStore store;

  LocalStoreForm({Key key, @required this.store}) : super(key: key);

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
          attribute: 'basepath',
          decoration: const InputDecoration(
            icon: Icon(Icons.folder_open),
            labelText: 'Base Path',
          ),
          validators: [FormBuilderValidators.required()],
        ),
      ],
    );
  }
}
