//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:meta/meta.dart';
import 'package:zorigami/core/domain/entities/configuration.dart';

class ConfigurationModel extends Configuration {
  ConfigurationModel({
    @required String hostname,
    @required String username,
    @required String computerId,
  }) : super(
          hostname: hostname,
          username: username,
          computerId: computerId,
        );

  factory ConfigurationModel.fromJson(Map<String, dynamic> json) {
    return ConfigurationModel(
      hostname: json['hostname'],
      username: json['username'],
      computerId: json['computerId'],
    );
  }

  Map<String, dynamic> toJson() {
    return {
      'hostname': hostname,
      'username': username,
      'computerId': computerId,
    };
  }
}
