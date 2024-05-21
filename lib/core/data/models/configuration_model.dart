//
// Copyright (c) 2024 Nathan Fiedler
//
import 'package:zorigami/core/domain/entities/configuration.dart';

class ConfigurationModel extends Configuration {
  const ConfigurationModel({
    required super.hostname,
    required super.username,
    required super.computerId,
  });

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
