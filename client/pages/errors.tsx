//
// Copyright (c) 2026 Nathan Fiedler
//
import { createResource, For, Match, Show, Suspense, Switch } from 'solid-js';
import { A } from '@solidjs/router';
import { type TypedDocumentNode, gql } from '@apollo/client';
import { useApolloClient } from '../apollo-provider';
import {
  type Mutation,
  type MutationDeleteCapturedErrorArgs,
  type Query
} from 'zorigami/generated/graphql.ts';

const CAPTURED_ERRORS: TypedDocumentNode<Query, Record<string, never>> = gql`
  query CapturedErrors {
    capturedErrors {
      id
      timestamp
      operation
      datasetId
      message
    }
  }
`;

const DELETE_CAPTURED_ERROR: TypedDocumentNode<
  Mutation,
  MutationDeleteCapturedErrorArgs
> = gql`
  mutation DeleteCapturedError($id: BigInt!) {
    deleteCapturedError(id: $id)
  }
`;

const CLEAR_CAPTURED_ERRORS: TypedDocumentNode<
  Mutation,
  Record<string, never>
> = gql`
  mutation ClearCapturedErrors {
    clearCapturedErrors
  }
`;

export function Errors() {
  const client = useApolloClient();
  const [errorsQuery, { refetch }] = createResource(async () => {
    const { data } = await client.query({
      query: CAPTURED_ERRORS,
      fetchPolicy: 'network-only'
    });
    return data;
  });

  async function deleteOne(id: string) {
    await client.mutate({
      mutation: DELETE_CAPTURED_ERROR,
      variables: { id }
    });
    refetch();
  }

  async function clearAll() {
    if (!window.confirm('Delete all captured errors?')) {
      return;
    }
    await client.mutate({ mutation: CLEAR_CAPTURED_ERRORS });
    refetch();
  }

  return (
    <>
      <nav class="m-4 level">
        <div class="level-left">
          <div class="level-item">
            <A class="button" href="/">
              <span class="icon">
                <i class="fa-solid fa-arrow-left"></i>
              </span>
              <span>Back</span>
            </A>
          </div>
        </div>
        <div class="level-right">
          <div class="level-item">
            <button
              class="button is-danger is-light"
              disabled={!errorsQuery()?.capturedErrors.length}
              on:click={() => clearAll()}
            >
              <span class="icon">
                <i class="fa-solid fa-trash"></i>
              </span>
              <span>Clear All</span>
            </button>
          </div>
        </div>
      </nav>
      <div class="container mt-4">
        <Suspense fallback={'...'}>
          <Switch>
            <Match when={errorsQuery()?.capturedErrors.length === 0}>
              <article class="message">
                <div class="message-header">
                  <p>No Errors</p>
                </div>
                <div class="message-body">
                  No errors have been captured from background operations.
                </div>
              </article>
            </Match>
            <Match when={errorsQuery()?.capturedErrors.length}>
              <table class="table is-striped is-hoverable is-fullwidth has-text-left">
                <thead>
                  <tr>
                    <th>Time</th>
                    <th>Operation</th>
                    <th>Dataset</th>
                    <th>Message</th>
                    <th></th>
                  </tr>
                </thead>
                <tbody>
                  <For each={errorsQuery()?.capturedErrors}>
                    {(item) => (
                      <tr>
                        <td>{new Date(item.timestamp).toLocaleString()}</td>
                        <td>{item.operation}</td>
                        <td>
                          <Show when={item.datasetId} fallback="-">
                            <code>{item.datasetId}</code>
                          </Show>
                        </td>
                        <td style="white-space: pre-wrap;">{item.message}</td>
                        <td>
                          <button
                            class="button is-small is-danger is-light"
                            title="Delete"
                            on:click={() => deleteOne(item.id)}
                          >
                            <span class="icon">
                              <i class="fa-solid fa-xmark"></i>
                            </span>
                          </button>
                        </td>
                      </tr>
                    )}
                  </For>
                </tbody>
              </table>
            </Match>
          </Switch>
        </Suspense>
      </div>
    </>
  );
}
