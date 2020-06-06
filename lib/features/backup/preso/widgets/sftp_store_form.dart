//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:flutter/material.dart';
import 'package:flutter_form_builder/flutter_form_builder.dart';
import 'package:zorigami/core/domain/entities/pack_store.dart';
import 'package:zorigami/features/backup/preso/widgets/pack_store_form.dart';

class SftpStoreForm extends PackStoreForm {
  final PackStore store;

  SftpStoreForm({Key key, @required this.store}) : super(key: key);

  @override
  Map<String, dynamic> initialValuesFrom(PackStore store) {
    final password = store.options['password'] ?? '';
    final basepath = store.options['basepath'] ?? '';
    return {
      'key': store.key,
      'label': store.label,
      'remote_addr': store.options['remote_addr'],
      'username': store.options['username'],
      'password': password,
      'basepath': basepath,
    };
  }

  @override
  PackStore storeFromState(FormBuilderState state) {
    final String password = state.value['password'];
    final String basepath = state.value['basepath'];
    return PackStore(
      key: state.value['key'],
      label: state.value['label'],
      kind: StoreKind.minio,
      options: {
        'remote_addr': state.value['remote_addr'],
        'username': state.value['username'],
        'password': password.isEmpty ? null : password,
        'basepath': basepath.isEmpty ? null : basepath,
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
          attribute: 'remote_addr',
          decoration: const InputDecoration(
            icon: Icon(Icons.cloud),
            labelText: 'Address',
          ),
          validators: [FormBuilderValidators.required()],
        ),
        FormBuilderTextField(
          attribute: 'username',
          decoration: const InputDecoration(
            icon: Icon(Icons.folder_open),
            labelText: 'Username',
          ),
          validators: [FormBuilderValidators.required()],
        ),
        FormBuilderTextField(
          attribute: 'password',
          obscureText: true,
          maxLines: 1,
          decoration: const InputDecoration(
            icon: Icon(Icons.folder_open),
            labelText: 'Password',
          ),
        ),
        FormBuilderTextField(
          attribute: 'basepath',
          decoration: const InputDecoration(
            icon: Icon(Icons.folder_open),
            labelText: 'Base Path',
          ),
        ),
      ],
    );
  }
}