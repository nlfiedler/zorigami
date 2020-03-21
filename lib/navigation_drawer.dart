//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:flutter/material.dart';
import 'package:zorigami/features/browse/preso/widgets/configuration.dart';

class NavigationDrawer extends StatelessWidget {
  @override
  Widget build(BuildContext context) {
    return Drawer(
      child: ListView(
        padding: EdgeInsets.zero,
        children: <Widget>[
          Configuration(),
          ListTile(
            leading: Icon(Icons.settings),
            title: Text('Snapshots'),
            onTap: () => Navigator.pushNamedAndRemoveUntil(
                context, '/', ModalRoute.withName('/')),
          ),
          ListTile(
            leading: Icon(Icons.message),
            title: Text('Data Sets'),
            onTap: () => Navigator.pushNamedAndRemoveUntil(
                context, '/sets', ModalRoute.withName('/')),
          ),
          ListTile(
            leading: Icon(Icons.account_circle),
            title: Text('Pack Stores'),
            onTap: () => Navigator.pushNamedAndRemoveUntil(
                context, '/stores', ModalRoute.withName('/')),
          ),
        ],
      ),
    );
  }
}
