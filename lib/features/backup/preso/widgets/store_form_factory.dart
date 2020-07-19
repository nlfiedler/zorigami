//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:zorigami/core/domain/entities/pack_store.dart';
import 'package:zorigami/features/backup/preso/widgets/google_store_form.dart';
import 'package:zorigami/features/backup/preso/widgets/local_store_form.dart';
import 'package:zorigami/features/backup/preso/widgets/minio_store_form.dart';
import 'package:zorigami/features/backup/preso/widgets/pack_store_form.dart';
import 'package:zorigami/features/backup/preso/widgets/sftp_store_form.dart';

/// Factory to build form widgets for pack store details.
PackStoreForm buildStoreForm(PackStore store, void param2) {
  if (store.kind == StoreKind.local) {
    return LocalStoreForm(store: store);
  }
  if (store.kind == StoreKind.google) {
    return GoogleStoreForm(store: store);
  }
  if (store.kind == StoreKind.minio) {
    return MinioStoreForm(store: store);
  }
  if (store.kind == StoreKind.sftp) {
    return SftpStoreForm(store: store);
  }
  return null;
}

/// Factory to create a generic pack store for the given kind.
PackStore defaultPackStore(String kind, void param2) {
  switch (kind) {
    case 'local':
      return PackStore(
        kind: StoreKind.local,
        key: 'auto-generated',
        label: 'local',
        options: <String, dynamic>{
          'basepath': '.',
        },
      );
    case 'google':
      return PackStore(
        kind: StoreKind.google,
        key: 'auto-generated',
        label: 'google',
        options: <String, dynamic>{
          'credentials': '/Users/charlie/credentials.json',
          'project': 'white-sunspot-12345',
          'region': 'us-west1',
          'storage': 'NEARLINE',
        },
      );
    case 'minio':
      return PackStore(
        kind: StoreKind.minio,
        key: 'auto-generated',
        label: 'minio',
        options: <String, dynamic>{
          'region': 'us-west-1',
          'endpoint': 'http://localhost:9000',
          'access_key': 'AKIAIOSFODNN7EXAMPLE',
          'secret_key': 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
        },
      );
    case 'sftp':
      return PackStore(
        kind: StoreKind.sftp,
        key: 'auto-generated',
        label: 'sftp',
        options: <String, dynamic>{
          'remote_addr': '127.0.0.1:22',
          'username': 'charlie',
          'password': null,
          'basepath': null,
        },
      );
    default:
      throw ArgumentError('kind is not recognized');
  }
}
