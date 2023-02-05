//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:flutter/material.dart';
import 'package:zorigami/features/browse/preso/widgets/configuration.dart';

// ignore: use_key_in_widget_constructors
class MyNavigationDrawer extends StatelessWidget {
  @override
  Widget build(BuildContext context) {
    return Drawer(
      child: ListView(
        padding: EdgeInsets.zero,
        children: <Widget>[
          Configuration(),
          ListTile(
            leading: const Icon(Icons.timeline),
            title: const Text('Snapshots'),
            onTap: () => Navigator.pushNamedAndRemoveUntil(
                context, '/', ModalRoute.withName('/')),
          ),
          ListTile(
            leading: const Icon(Icons.dns),
            title: const Text('Data Sets'),
            onTap: () => Navigator.pushNamedAndRemoveUntil(
                context, '/sets', ModalRoute.withName('/')),
          ),
          ListTile(
            leading: const Icon(Icons.archive),
            title: const Text('Pack Stores'),
            onTap: () => Navigator.pushNamedAndRemoveUntil(
                context, '/stores', ModalRoute.withName('/')),
          ),
          ListTile(
            leading: const Icon(Icons.dns),
            title: const Text('Restore Requests'),
            onTap: () => Navigator.pushNamedAndRemoveUntil(
                context, '/requests', ModalRoute.withName('/')),
          ),
          ListTile(
            leading: const Icon(Icons.restore),
            title: const Text('Database Restore'),
            onTap: () => Navigator.pushNamedAndRemoveUntil(
                context, '/restore', ModalRoute.withName('/')),
          ),
        ],
      ),
    );
  }
}
