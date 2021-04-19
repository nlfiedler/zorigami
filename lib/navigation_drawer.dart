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
            leading: Icon(Icons.timeline),
            title: Text('Snapshots'),
            onTap: () => Navigator.pushNamedAndRemoveUntil(
                context, '/', ModalRoute.withName('/')),
          ),
          ListTile(
            leading: Icon(Icons.dns),
            title: Text('Data Sets'),
            onTap: () => Navigator.pushNamedAndRemoveUntil(
                context, '/sets', ModalRoute.withName('/')),
          ),
          ListTile(
            leading: Icon(Icons.archive),
            title: Text('Pack Stores'),
            onTap: () => Navigator.pushNamedAndRemoveUntil(
                context, '/stores', ModalRoute.withName('/')),
          ),
          ListTile(
            leading: Icon(Icons.dns),
            title: Text('Restore Requests'),
            onTap: () => Navigator.pushNamedAndRemoveUntil(
                context, '/requests', ModalRoute.withName('/')),
          ),
          ListTile(
            leading: Icon(Icons.restore),
            title: Text('Database Restore'),
            onTap: () => Navigator.pushNamedAndRemoveUntil(
                context, '/restore', ModalRoute.withName('/')),
          ),
        ],
      ),
    );
  }
}
