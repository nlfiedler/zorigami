//
// Copyright (c) 2026 Nathan Fiedler
//
import {
  For,
  Match,
  type Setter,
  Show,
  Suspense,
  Switch,
  createResource,
  createSignal
} from 'solid-js';
import {
  A,
  action,
  type Submission,
  useAction,
  useNavigate,
  useSubmission
} from '@solidjs/router';
import { type TypedDocumentNode, gql } from '@apollo/client';
import { useApolloClient } from '../apollo-provider';
import { AutoRefreshCheckbox } from '../components/refresh-checkbox';
import {
  type Mutation,
  type MutationCancelRestoreArgs,
  type MutationRestoreDatabaseArgs,
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

export function Restore() {
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

  return (
    <>
      <nav class="m-4 level">
        <div class="level-left">
          <div class="level-item">
            <AutoRefreshCheckbox refetch={refetch} />
          </div>
        </div>
      </nav>
      <div class="section">
        <h1 class="title">File and Directory Restore</h1>
        <h2 class="subtitle">Pending, Active, and Completed Requests</h2>
        <Suspense fallback={'...'}>
          <Switch>
            <Match when={restoresQuery()?.restores.length == 0}>
              <article class="message">
                <div class="message-body">No restore requests found.</div>
              </article>
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

      <div class="section">
        <h1 class="title">Full Database Restore</h1>
        <h2 class="subtitle">Restore the database from a snapshot</h2>
        <DatabaseRestore />
      </div>
    </>
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

const ALL_STORES: TypedDocumentNode<Query, Record<string, never>> = gql`
  query {
    stores {
      id
      storeType
      label
    }
  }
`;

function DatabaseRestore() {
  const client = useApolloClient();
  const [storesQuery] = createResource(async () => {
    const { data } = await client.query({ query: ALL_STORES });
    return data;
  });
  const sortedStores = () => {
    // the stores returned from the server are in no particular order
    const sorted = [];
    for (const store of storesQuery()?.stores ?? []) {
      sorted.push(store);
    }
    sorted.sort((a, b) => a.id.localeCompare(b.id));
    return sorted;
  };
  const [modalOpen, setModalOpen] = createSignal(false);

  return (
    <Suspense fallback={'...'}>
      <Switch>
        <Match when={sortedStores()?.length == 0}>
          <article class="message">
            <div class="message-header">
              <p>No Pack Stores</p>
            </div>
            <div class="message-body">
              There are no pack stores from which to restore the database. To
              restore the database, first define a{' '}
              <A href="/stores">pack store</A> that specifies a location that
              has backups from which to retrieve a database archive.
            </div>
          </article>
        </Match>
        <Match when={sortedStores()}>
          <article class="message">
            <div class="message-body">
              To restore the database from the most recent snapshot, select one
              of the pack stores below. Note that the existing database will be
              overwritten by the most recently saved snapshot.
            </div>
          </article>
          <div class="list has-hoverable-list-items has-overflow-ellipsis has-visible-pointer-controls">
            <For each={sortedStores()}>
              {(store) => (
                <>
                  <div class="list-item">
                    <div class="list-item-image">
                      <span>
                        <i class="fa-solid fa-boxes-stacked"></i>
                      </span>
                    </div>
                    <div class="list-item-content">
                      <div class="list-item-title">{store.label}</div>
                      <div class="list-item-description">{store.storeType}</div>
                    </div>
                    <div class="list-item-controls">
                      <div class="buttons is-right">
                        <button
                          class="button"
                          on:click={() => setModalOpen(true)}
                        >
                          <span class="icon is-small">
                            <i class="fa-solid fa-download"></i>
                          </span>
                          <span>Restore</span>
                        </button>
                      </div>
                    </div>
                  </div>

                  <div
                    classList={{
                      modal: true,
                      'is-active': modalOpen()
                    }}
                  >
                    <div class="modal-background"></div>
                    <div class="modal-card">
                      <ConfirmDialog
                        setModalOpen={setModalOpen}
                        storeId={store.id}
                      />
                    </div>
                  </div>
                </>
              )}
            </For>
          </div>
        </Match>
      </Switch>
    </Suspense>
  );
}

const RESTORE_DATABASE: TypedDocumentNode<
  Mutation,
  MutationRestoreDatabaseArgs
> = gql`
  mutation RestoreDatabase($storeId: String!) {
    restoreDatabase(storeId: $storeId)
  }
`;

interface ConfirmDialogProps {
  storeId: string;
  setModalOpen: Setter<boolean>;
}

function ConfirmDialog(props: ConfirmDialogProps) {
  const navigate = useNavigate();
  const [errorMsg, setErrorMsg] = createSignal('');
  const client = useApolloClient();
  const restoreAction = action(
    async (): Promise<{ ok: boolean }> => {
      await client.mutate({
        mutation: RESTORE_DATABASE,
        variables: {
          storeId: props.storeId
        }
      });
      return { ok: true };
    },
    {
      name: 'restoreDatabase',
      onComplete: (s: Submission<any, any>) => {
        if (s.error) {
          console.error('database restore failed:', s.error);
          const msg =
            s.error?.graphQLErrors?.[0]?.message ??
            s.error?.message ??
            String(s.error);
          setErrorMsg(msg);
        } else {
          navigate('/');
        }
      }
    }
  );
  const startRestore = useAction(restoreAction);
  const restoreSubmission = useSubmission(restoreAction);

  return (
    <>
      <header class="modal-card-head">
        <p class="modal-card-title">Confirm changes to selected assets</p>
        <button
          class="delete"
          aria-label="close"
          on:click={(_) => {
            props.setModalOpen(false);
          }}
        ></button>
      </header>
      <section class="modal-card-body">
        <h2 class="subtitle">Are you sure you want to restore the database?</h2>
        <Show when={errorMsg().length > 0}>
          <div class="notification is-warning">
            <button class="delete" on:click={() => setErrorMsg('')}></button>
            {errorMsg()}
          </div>
        </Show>
      </section>
      <footer class="modal-card-foot">
        <div class="buttons">
          <button
            classList={{
              button: true,
              'is-success': true,
              'is-loading': restoreSubmission.pending
            }}
            disabled={restoreSubmission.pending}
            on:click={(_) => startRestore()}
          >
            Restore
          </button>
          <button class="button" on:click={(_) => props.setModalOpen(false)}>
            Cancel
          </button>
        </div>
      </footer>
    </>
  );
}
