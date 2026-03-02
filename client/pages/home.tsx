//
// Copyright (c) 2026 Nathan Fiedler
//
import { createResource, For, Suspense, type JSX } from 'solid-js';
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

function Home() {
  const client = useApolloClient();
  const [confQuery] = createResource(async () => {
    const { data } = await client.query({ query: CONFIGURATION });
    return data;
  });
  return (
    <Suspense fallback={'...'}>
      <ul>
        <li>{confQuery()?.configuration.hostname}</li>
        <li>{confQuery()?.configuration.username}</li>
        <li>{confQuery()?.configuration.computerId}</li>
        <li>{confQuery()?.configuration.computerBucket}</li>
      </ul>
    </Suspense>
  );
}

export default Home;
