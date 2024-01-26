//
// Copyright (c) 2024 Nathan Fiedler
//
import 'package:flutter/material.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:form_builder_validators/form_builder_validators.dart';
import 'package:zorigami/features/backup/preso/screens/data_sets_screen.dart';
import 'package:zorigami/features/backup/preso/screens/pack_stores_screen.dart';
import 'package:zorigami/features/browse/preso/screens/home_screen.dart';
import 'package:zorigami/features/browse/preso/screens/database_restore_screen.dart';
import 'package:zorigami/features/browse/preso/screens/restore_screen.dart';

void main() {
  runApp(const ProviderScope(child: MyApp()));
}

class MyApp extends StatelessWidget {
  const MyApp({Key? key}) : super(key: key);

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'Zorigami',
      initialRoute: '/',
      routes: {
        '/': (context) => HomeScreen(),
        '/requests': (context) => const RestoreRequestsScreen(),
        '/restore': (context) => const DatabaseRestoreScreen(),
        '/sets': (context) => const DataSetsScreen(),
        '/stores': (context) => const PackStoresScreen(),
      },
      theme: ThemeData(
        bottomSheetTheme: BottomSheetThemeData(
          backgroundColor: Colors.black.withOpacity(0),
        ),
      ),
      localizationsDelegates: const [
        FormBuilderLocalizations.delegate,
      ],
    );
  }
}
