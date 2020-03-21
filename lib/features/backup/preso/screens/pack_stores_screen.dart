//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:flutter/material.dart';
import 'package:zorigami/navigation_drawer.dart';

class PackStoresScreen extends StatelessWidget {
  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: Text('PACK STORES'),
      ),
      body: Center(
        child: Text('Pack Stores'),
      ),
      drawer: NavigationDrawer(),
    );
  }
}
