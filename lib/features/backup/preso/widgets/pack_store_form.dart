//
// Copyright (c) 2024 Nathan Fiedler
//
import 'package:flutter/material.dart';
import 'package:flutter_form_builder/flutter_form_builder.dart';
import 'package:zorigami/core/domain/entities/pack_store.dart';

abstract class PackStoreForm extends StatelessWidget {
  const PackStoreForm({super.key});

  /// Prepare the initial form values using the given store.
  Map<String, dynamic> initialValuesFrom(PackStore store);

  /// Convert the form state into a pack store.
  PackStore storeFromState(FormBuilderState state);
}
