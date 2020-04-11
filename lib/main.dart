//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:flutter/material.dart';
import 'package:zorigami/features/backup/preso/screens/data_sets_screen.dart';
import 'package:zorigami/features/backup/preso/screens/pack_stores_screen.dart';
import 'package:zorigami/features/browse/preso/screens/home_screen.dart';
import 'container.dart' as ioc;

void main() {
  ioc.init();
  runApp(MyApp());
}

class MyApp extends StatelessWidget {
  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'Zorigami',
      initialRoute: '/',
      routes: {
        '/': (context) => HomeScreen(),
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
