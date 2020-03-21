//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:flutter/material.dart';
import 'package:zorigami/navigation_drawer.dart';

class HomeScreen extends StatelessWidget {
  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: Text('ZORIGAMI'),
      ),
      body: Center(
        child: Text('Snapshots'),
      ),
      drawer: NavigationDrawer(),
    );
  }
}
