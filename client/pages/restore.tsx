//
// Copyright (c) 2026 Nathan Fiedler
//
import {
  For,
  Match,
  Show,
  Suspense,
  Switch,
  createResource,
  onCleanup
} from 'solid-js';
import { action, type Submission, useAction } from '@solidjs/router';
import { type TypedDocumentNode, gql } from '@apollo/client';
import { useApolloClient } from '../apollo-provider';
import {
  type Mutation,
  type MutationCancelRestoreArgs,
  type Query,
  type RestoreRequest,
  RestoreStatus
} from 'zorigami/generated/graphql.ts';

const ALL_RESTORES: TypedDocumentNode<Query, Record<string, never>> = gql`
  query {
    restores {
      id
      status
      tree
      entry
      filepath
      dataset
      finished
      filesRestored
      errors
    }
  }
`;

const CANCEL_RESTORE: TypedDocumentNode<Mutation, MutationCancelRestoreArgs> =
  gql`
    mutation NoRestore($id: String!) {
      cancelRestore(id: $id)
    }
  `;

function Restore() {
  const client = useApolloClient();
  const [restoresQuery, { refetch }] = createResource(async () => {
    const { data } = await client.query({ query: ALL_RESTORES });
    return data;
  });
  const cancelAction = action(
    async (id: string): Promise<{ ok: boolean }> => {
      const result = await client.mutate({
        mutation: CANCEL_RESTORE,
        variables: {
          id
        }
      });
      if (!result.data?.cancelRestore) {
        return { ok: false };
      }
      return { ok: true };
    },
    {
      name: 'cancelRestore',
      onComplete: (s: Submission<any, any>) => {
        if (s.error) {
          console.error('file restore failed:', s.error);
        } else {
          refetch();
        }
      }
    }
  );
  const startCancel = useAction(cancelAction);
  const interval = setInterval(() => {
    refetch();
  }, 5000);
  onCleanup(() => {
    clearInterval(interval);
  });

  return (
    <div class="container">
      <h1 class="title">Restore</h1>
      <h2 class="subtitle">Requests</h2>
      <Suspense fallback={'...'}>
        <Switch>
          <Match when={restoresQuery()?.restores.length == 0}>
            <p>No restore requests</p>
          </Match>
          <Match when={restoresQuery()?.restores}>
            <div class="list has-hoverable-list-items has-overflow-ellipsis has-visible-pointer-controls">
              <For each={restoresQuery()?.restores}>
                {(item) => (
                  <RestoreRequest request={item} cancel={startCancel} />
                )}
              </For>
            </div>
          </Match>
        </Switch>
      </Suspense>
    </div>
  );
}

interface RestoreRequestProps {
  request: RestoreRequest;
  cancel: (id: string) => Promise<{ ok: boolean }>;
}

function RestoreRequest(props: RestoreRequestProps) {
  const req = props.request;
  return (
    <div class="list-item">
      <div class="list-item-image">
        <Switch>
          <Match when={req.status == RestoreStatus.Pending}>
            <span>
              <i class="fa-regular fa-clock"></i>
            </span>
          </Match>
          <Match when={req.status == RestoreStatus.Cancelled}>
            <span>
              <i class="fa-solid fa-xmark"></i>
            </span>
          </Match>
          <Match when={req.status == RestoreStatus.Running}>
            <span>
              <i class="fa-solid fa-spinner"></i>
            </span>
          </Match>
          <Match when={req.status == RestoreStatus.Completed}>
            <span>
              <i class="fa-solid fa-check"></i>
            </span>
          </Match>
        </Switch>
      </div>
      <div class="list-item-content">
        <div class="list-item-title">{req.filepath}</div>
        <div class="list-item-description">
          <Switch>
            <Match when={req.status == RestoreStatus.Pending}>
              <span>Pending...</span>
            </Match>
            <Match when={req.status == RestoreStatus.Cancelled}>
              <span>Cancelled</span>
            </Match>
            <Match when={req.status == RestoreStatus.Running}>
              <span>Processing... restored {req.filesRestored} files</span>
            </Match>
            <Match when={req.status == RestoreStatus.Completed}>
              <span>
                Completed: {new Date(req.finished).toLocaleString()}
                {' -- '}
                {req.filesRestored} files restored
              </span>
            </Match>
          </Switch>
          <Show when={req.errors.length > 0}>
            <br />
            <span>
              <strong>Error(s):</strong> {req.errors.slice(0, 3).join('; ')}
            </span>
          </Show>
        </div>
      </div>
      <div class="list-item-controls">
        <div class="buttons is-right">
          <button
            class="button"
            disabled={req.status != RestoreStatus.Pending}
            on:click={() => props.cancel(req.id)}
          >
            <span class="icon is-small">
              <i class="fa-regular fa-circle-xmark"></i>
            </span>
            <span>Cancel</span>
          </button>
        </div>
      </div>
    </div>
  );
}

export default Restore;
