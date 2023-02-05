//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:flutter/material.dart';
import 'package:zorigami/features/browse/preso/widgets/data_sets_list.dart';
import 'package:zorigami/navigation_drawer.dart';

class HomeScreen extends StatelessWidget {
  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: Text('ZORIGAMI'),
      ),
      body: DataSetsList(),
      drawer: MyNavigationDrawer(),
    );
  }
}
