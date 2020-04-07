//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:zorigami/core/domain/entities/pack_store.dart';
import 'package:zorigami/features/backup/preso/widgets/local_store_form.dart';
import 'package:zorigami/features/backup/preso/widgets/pack_store_form.dart';

// Factory to build form widgets for pack store details.
PackStoreForm buildStoreForm(PackStore store, void param2) {
  if (store.kind == StoreKind.local) {
    return LocalStoreForm(store: store);
  }
  return null;
}
