//
// Copyright (c) 2024 Nathan Fiedler
//
import 'package:flutter/material.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:flutter_bloc/flutter_bloc.dart';
import 'package:zorigami/features/browse/preso/bloc/configuration_bloc.dart';
import 'package:zorigami/features/browse/preso/bloc/providers.dart';

class Configuration extends ConsumerWidget {
  const Configuration({super.key});

  @override
  Widget build(BuildContext context, WidgetRef ref) {
    return BlocProvider<ConfigurationBloc>(
      create: (_) => ref.read(configurationBlocProvider),
      child: BlocBuilder<ConfigurationBloc, ConfigurationState>(
        builder: (context, state) {
          if (state is Empty) {
            // kick off the initial remote request
            BlocProvider.of<ConfigurationBloc>(context)
                .add(LoadConfiguration());
          }
          if (state is Error) {
            return Text('Error: ${state.message}');
          }
          if (state is Loaded) {
            final config = state.config;
            final title = '${config.username}@${config.hostname}';
            return UserAccountsDrawerHeader(
              currentAccountPicture: const Icon(
                Icons.computer,
                color: Colors.white,
                size: 64.0,
              ),
              accountEmail: Text(title),
              accountName: Text(config.computerId),
            );
          }
          return const CircularProgressIndicator();
        },
      ),
    );
  }
}
