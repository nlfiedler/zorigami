//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:flutter/material.dart';
import 'package:flutter_form_builder/flutter_form_builder.dart';
import 'package:zorigami/core/domain/entities/pack_store.dart';

abstract class PackStoreForm extends StatelessWidget {
  PackStoreForm({Key key}) : super(key: key);

  /// Return default values for form inputs.
  Map<String, dynamic> initialValues();

  /// Prepare the initial form values using the given store.
  Map<String, dynamic> initialValuesFrom(PackStore store);

  /// Convert the form state into pack store options.
  Map<String, dynamic> optionsFromState(FormBuilderState state);
}
