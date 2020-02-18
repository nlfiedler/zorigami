//
// Copyright (c) 2020 Nathan Fiedler
//
import 'package:flutter/material.dart';
import 'package:graphql_flutter/graphql_flutter.dart';

const String queryConfiguration = """
  query {
    configuration {
      hostname
      username
      computerId
    }
  }
""";

/// Display the configuration of the application.
class Configuration extends StatelessWidget {
  @override
  Widget build(BuildContext context) {
    return Query(
      options: QueryOptions(
        documentNode: gql(queryConfiguration),
      ),
      builder: (QueryResult result,
          {VoidCallback refetch, FetchMore fetchMore}) {
        if (result.hasException) {
          return Text(result.exception.toString());
        }
        if (result.loading) {
          return Text('Loading');
        }
        final title = result.data['configuration']['username'] +
            '@' +
            result.data['configuration']['hostname'];
        return Card(
          child: ListTile(
            leading: Icon(Icons.computer),
            title: Text(title),
            subtitle: Text(result.data['configuration']['computerId']),
          ),
        );
      },
    );
  }
}
