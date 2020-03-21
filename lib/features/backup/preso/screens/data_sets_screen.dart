//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:flutter/material.dart';
import 'package:zorigami/navigation_drawer.dart';

class DataSetsScreen extends StatelessWidget {
  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: Text('DATA SETS'),
      ),
      body: Center(
        child: Text('Data Sets'),
      ),
      drawer: NavigationDrawer(),
    );
  }
}
