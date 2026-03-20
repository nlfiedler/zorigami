//
// Copyright (c) 2026 Nathan Fiedler
//
import { createResource, Suspense } from 'solid-js';
import { type TypedDocumentNode, gql } from '@apollo/client';
import { useApolloClient } from '../apollo-provider';
import { type Query } from 'zorigami/generated/graphql.ts';

const CONFIGURATION: TypedDocumentNode<Query, Record<string, never>> = gql`
  query {
    configuration {
      hostname
      username
      computerId
      computerBucket
    }
  }
`;

function Settings() {
  const client = useApolloClient();
  const [confQuery] = createResource(async () => {
    const { data } = await client.query({ query: CONFIGURATION });
    return data;
  });
  return (
    <Suspense fallback={'...'}>
      <ul>
        <li>
          <strong>Hostname:</strong> {confQuery()?.configuration.hostname}
        </li>
        <li>
          <strong>Username:</strong> {confQuery()?.configuration.username}
        </li>
        <li>
          <strong>Computer ID:</strong> {confQuery()?.configuration.computerId}
        </li>
        <li>
          <strong>Database Bucket:</strong>{' '}
          {confQuery()?.configuration.computerBucket}
        </li>
      </ul>
    </Suspense>
  );
}

export default Settings;
