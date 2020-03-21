//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:flutter/material.dart';
import 'package:flutter_bloc/flutter_bloc.dart';
import 'package:zorigami/container.dart';
import 'package:zorigami/features/browse/preso/bloc/configuration_bloc.dart';

class Configuration extends StatelessWidget {
  @override
  Widget build(BuildContext context) {
    return BlocProvider<ConfigurationBloc>(
      create: (_) => getIt<ConfigurationBloc>(),
      child: BlocBuilder<ConfigurationBloc, ConfigurationState>(
        builder: (context, state) {
          if (state is Empty) {
            // kick off the initial remote request
            BlocProvider.of<ConfigurationBloc>(context)
                .add(LoadConfiguration());
            return Text('Starting...');
          }
          if (state is Error) {
            return Text('Error: ' + state.message);
          }
          if (state is Loaded) {
            final config = state.config;
            final title = config.username + '@' + config.hostname;
            return UserAccountsDrawerHeader(
              currentAccountPicture: Icon(
                Icons.computer,
                color: Colors.white,
                size: 64.0,
              ),
              accountEmail: Text(title),
              accountName: Text(config.computerId),
            );
          }
          return Text('Loading...');
        },
      ),
    );
  }
}
