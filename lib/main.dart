//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:flutter/material.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:zorigami/features/backup/preso/screens/data_sets_screen.dart';
import 'package:zorigami/features/backup/preso/screens/pack_stores_screen.dart';
import 'package:zorigami/features/browse/preso/screens/home_screen.dart';
import 'package:zorigami/features/browse/preso/screens/database_restore_screen.dart';
import 'package:zorigami/features/browse/preso/screens/restore_screen.dart';

void main() {
  runApp(ProviderScope(child: MyApp()));
}

class MyApp extends StatelessWidget {
  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'Zorigami',
      initialRoute: '/',
      routes: {
        '/': (context) => HomeScreen(),
        '/requests': (context) => RestoreRequestsScreen(),
        '/restore': (context) => DatabaseRestoreScreen(),
        '/sets': (context) => DataSetsScreen(),
        '/stores': (context) => PackStoresScreen(),
      },
      theme: ThemeData(
        bottomSheetTheme: BottomSheetThemeData(
          backgroundColor: Colors.black.withOpacity(0),
        ),
      ),
    );
  }
}
