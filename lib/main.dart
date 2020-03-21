//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:flutter/material.dart';
import 'package:zorigami/features/browse/preso/widgets/configuration.dart';
import 'container.dart' as di;

void main() {
  di.init();
  runApp(MyApp());
}

class MyApp extends StatelessWidget {
  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'Zorigami',
      home: Scaffold(
        appBar: AppBar(
          title: Text('ZORIGAMI'),
        ),
        body: Configuration(),
      ),
    );
  }
}
