//
// Copyright (c) 2024 Nathan Fiedler
//
import 'package:flutter/material.dart';
import 'package:form_builder_validators/form_builder_validators.dart';
import 'package:flutter_form_builder/flutter_form_builder.dart';
import 'package:zorigami/core/domain/entities/pack_store.dart';
import 'package:zorigami/features/backup/preso/widgets/pack_store_form.dart';

class SftpStoreForm extends PackStoreForm {
  final PackStore store;

  const SftpStoreForm({Key? key, required this.store}) : super(key: key);

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
    final Map<String, dynamic> options = {
      'remote_addr': state.value['remote_addr'],
      'username': state.value['username'],
    };
    final String password = state.value['password'];
    if (password.isNotEmpty) {
      options['password'] = password;
    }
    final String basepath = state.value['basepath'];
    if (basepath.isNotEmpty) {
      options['basepath'] = basepath;
    }
    return PackStore(
      key: state.value['key'],
      label: state.value['label'],
      kind: StoreKind.sftp,
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
          name: 'remote_addr',
          decoration: const InputDecoration(
            icon: Icon(Icons.cloud),
            labelText: 'Address',
          ),
          validator: FormBuilderValidators.required(),
        ),
        FormBuilderTextField(
          name: 'username',
          decoration: const InputDecoration(
            icon: Icon(Icons.folder_open),
            labelText: 'Username',
          ),
          validator: FormBuilderValidators.required(),
        ),
        FormBuilderTextField(
          name: 'password',
          obscureText: true,
          maxLines: 1,
          decoration: const InputDecoration(
            icon: Icon(Icons.folder_open),
            labelText: 'Password',
          ),
        ),
        FormBuilderTextField(
          name: 'basepath',
          decoration: const InputDecoration(
            icon: Icon(Icons.folder_open),
            labelText: 'Base Path',
          ),
        ),
      ],
    );
  }
}
