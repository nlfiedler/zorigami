//
// Copyright (c) 2026 Nathan Fiedler
//
import {
  createEffect,
  createResource,
  createSignal,
  For,
  Match,
  Show,
  Suspense,
  Switch
} from 'solid-js';
import { createStore } from 'solid-js/store';
import {
  action,
  type Submission,
  useAction,
  useLocation,
  useNavigate,
  useParams,
  useSubmission
} from '@solidjs/router';
import { type TypedDocumentNode, gql } from '@apollo/client';
import { useApolloClient } from '../apollo-provider';
import useClickOutside from '../hooks/use-click-outside.ts';
import {
  type Mutation,
  type MutationRestoreFilesArgs,
  type Query,
  type QuerySnapshotArgs,
  type QuerySnapshotsArgs,
  type QueryTreeArgs,
  type Snapshot
} from 'zorigami/generated/graphql.ts';

const ALL_DATASETS: TypedDocumentNode<Query, Record<string, never>> = gql`
  query {
    datasets {
      id
      basepath
      status
    }
  }
`;

export function SnapshotsPage(props: any) {
  const navigate = useNavigate();
  const client = useApolloClient();
  const [datasetsQuery, { refetch }] = createResource(async () => {
    const { data } = await client.query({ query: ALL_DATASETS });
    return data;
  });
  const sortedDatasets = () => {
    // the datasets returned from the server are in no particular order
    const sorted = [];
    for (const dataset of datasetsQuery()?.datasets ?? []) {
      sorted.push(dataset);
    }
    sorted.sort((a, b) => a.id.localeCompare(b.id));
    return sorted;
  };
  // listen for path changes and cause the dataset to refresh
  const location = useLocation();
  // the pathname is not actually used, just listening for route changes
  createEffect(() => refetch(location.pathname));

  return (
    <div class="m-4 columns">
      <div class="column is-one-quarter">
        <div class="box">
          <div class="list has-hoverable-list-items">
            <Suspense fallback={'...'}>
              <Switch>
                <Match when={sortedDatasets().length === 0}>
                  <NoDatasetsHelp />
                </Match>
                <Match when={sortedDatasets().length}>
                  <For each={sortedDatasets()}>
                    {(dataset) => (
                      <div
                        class="list-item"
                        on:click={() => {
                          navigate(`/snapshots/${dataset.id}`);
                        }}
                      >
                        <div class="list-item-content">
                          <div class="list-item-title ellipsize-left">
                            {dataset.basepath}
                          </div>
                          <div class="list-item-description">
                            Status: {dataset.status}
                          </div>
                        </div>
                      </div>
                    )}
                  </For>
                </Match>
              </Switch>
            </Suspense>
          </div>
        </div>
      </div>
      <div class="column">{props.children}</div>
    </div>
  );
}

export function SnapshotHelp() {
  return (
    <div class="m-4">
      <p>Select a data set to view its snapshots.</p>
    </div>
  );
}

const ALL_STORES: TypedDocumentNode<Query, Record<string, never>> = gql`
  query {
    stores {
      id
    }
  }
`;

function NoDatasetsHelp() {
  const client = useApolloClient();
  const [storesQuery] = createResource(async () => {
    const { data } = await client.query({ query: ALL_STORES });
    return data;
  });

  return (
    <Suspense fallback={'...'}>
      <Switch>
        <Match when={storesQuery()?.stores.length === 0}>
          <div class="list-item">
            <div class="list-item-content">
              <div class="list-item-title">No Pack Stores</div>
              <div class="list-item-description">
                Visit the <a href="/stores">Stores</a> page to configure a pack
                store, then visit the <a href="/datasets">Datasets</a> page to
                configure a new data set, then return here to see snapshots once
                a backup has completed.
              </div>
            </div>
          </div>
        </Match>
        <Match when={storesQuery()?.stores.length}>
          <div class="list-item">
            <div class="list-item-content">
              <div class="list-item-title">No Data Sets</div>
              <div class="list-item-description">
                Visit the <a href="/datasets">Datasets</a> page to configure a
                new data set, then return here to see snapshots once a backup
                has completed.
              </div>
            </div>
          </div>
        </Match>
      </Switch>
    </Suspense>
  );
}

const ALL_SNAPSHOTS: TypedDocumentNode<Query, QuerySnapshotsArgs> = gql`
  query Snapshots($id: String!) {
    snapshots(id: $id) {
      checksum
      parent
      startTime
      endTime
      fileCount
      tree
    }
  }
`;

export function Snapshots() {
  const params = useParams();
  const navigate = useNavigate();
  const client = useApolloClient();
  // BUG: useParams() and createResource() fail to refresh when the id path
  // parameter changes, but createEffect() will show that a change occurs;
  // work-around with useLocation() and refetch() to force the data refresh
  // (https://github.com/solidjs/solid/discussions/1745)
  const [snapshotsQuery, { refetch }] = createResource(
    () => params.id,
    async (id: string) => {
      const { data } = await client.query({
        query: ALL_SNAPSHOTS,
        variables: { id }
      });
      return data;
    }
  );
  const location = useLocation();
  // the pathname is not actually used, just listening for route changes
  createEffect(() => refetch(location.pathname));

  return (
    <Suspense fallback={'...'}>
      <Switch>
        <Match when={snapshotsQuery()?.snapshots.length === 0}>
          <article class="message">
            <div class="message-header">
              <p>No Snapshots</p>
            </div>
            <div class="message-body">
              If the dataset has a schedule, then a backup will be performed at
              the appropriate time. Without a schedule, the backup must be run
              manually.
            </div>
          </article>
        </Match>
        <Match when={snapshotsQuery()}>
          <table class="table is-striped is-hoverable is-fullwidth has-text-left">
            <thead>
              <tr>
                <th>Start</th>
                <th>End</th>
                <th>Files</th>
                <th>Identifier</th>
              </tr>
            </thead>
            <tbody>
              <For each={snapshotsQuery()?.snapshots}>
                {(item) => (
                  <tr
                    style="cursor: pointer;"
                    on:click={() => {
                      navigate(
                        `/snapshots/${params.id}/browse/${item.checksum}`
                      );
                    }}
                  >
                    <td>{new Date(item.startTime).toLocaleString()}</td>
                    <td>{new Date(item.endTime).toLocaleString()}</td>
                    <td>{item.fileCount}</td>
                    <td>
                      <code>{item.checksum}</code>
                    </td>
                  </tr>
                )}
              </For>
            </tbody>
          </table>
        </Match>
      </Switch>
    </Suspense>
  );
}

const GET_SNAPSHOT: TypedDocumentNode<Query, QuerySnapshotArgs> = gql`
  query Snapshot($digest: Checksum!) {
    snapshot(digest: $digest) {
      checksum
      startTime
      endTime
      fileCount
      tree
    }
  }
`;

export function SnapshotBrowse() {
  const params = useParams();
  const navigate = useNavigate();
  const client = useApolloClient();
  // BUG: useParams() and createResource() fail to refresh when the id path
  // parameter changes, but createEffect() will show that a change occurs;
  // work-around with useLocation() and refetch() to force the data refresh
  // (https://github.com/solidjs/solid/discussions/1745)
  const [snapshotQuery, { refetch }] = createResource(
    () => params.sid,
    async (digest: string) => {
      const { data } = await client.query({
        query: GET_SNAPSHOT,
        variables: { digest }
      });
      return data;
    }
  );
  const location = useLocation();
  // the pathname is not actually used, just listening for route changes
  createEffect(() => refetch(location.pathname));

  return (
    <Show when={snapshotQuery()} fallback="..." keyed>
      <nav class="level">
        <div class="level-left">
          <div class="level-item">
            <SnapshotSelector
              dataset={params.id!}
              changed={(id: string) =>
                navigate(`/snapshots/${params.id}/browse/${id}`)
              }
            />
          </div>
        </div>
        <div class="level-right">
          <div class="level-item">
            <SnapshotSummary snapshot={snapshotQuery()?.snapshot!} />
          </div>
        </div>
      </nav>
      <TreeViewer
        dataset={params.id!}
        digest={snapshotQuery()?.snapshot?.tree!}
      />
    </Show>
  );
}

interface SnapshotSelectorProps {
  dataset: string;
  changed: (id: string) => void;
}

function SnapshotSelector(props: SnapshotSelectorProps) {
  const [dropdownOpen, setDropdownOpen] = createSignal(false);
  let dropdownRef: HTMLDivElement | undefined;
  useClickOutside(
    () => dropdownRef,
    () => setDropdownOpen(false)
  );
  const client = useApolloClient();
  const [snapshotsQuery] = createResource(
    () => props.dataset,
    async (id: string) => {
      const { data } = await client.query({
        query: ALL_SNAPSHOTS,
        variables: { id }
      });
      return data;
    }
  );

  // dropdown element must be unconditional so that the ref will get set early
  // enough for the useClickOutside to work effectively
  return (
    <div
      class="dropdown"
      ref={(el: HTMLDivElement) => (dropdownRef = el)}
      class:is-active={dropdownOpen()}
    >
      <div class="dropdown-trigger">
        <button
          class="button"
          on:click={() => setDropdownOpen((v) => !v)}
          aria-haspopup="true"
          aria-controls="dropdown-menu"
        >
          <span class="icon">
            <i class="fa-solid fa-box-archive" aria-hidden="true"></i>
          </span>
          <span>Choose Snapshot</span>
        </button>
      </div>
      <div class="dropdown-menu" id="dropdown-menu" role="menu">
        <div class="dropdown-content">
          <Show when={snapshotsQuery()} fallback="..." keyed>
            <For each={snapshotsQuery()?.snapshots}>
              {(item) => (
                <a
                  class="dropdown-item"
                  on:click={() => {
                    props.changed(item.checksum);
                    setDropdownOpen(false);
                  }}
                >
                  {new Date(item.startTime).toLocaleString()}
                </a>
              )}
            </For>
          </Show>
        </div>
      </div>
    </div>
  );
}

interface SnapshotSummaryProps {
  snapshot: Snapshot;
}

function SnapshotSummary(props: SnapshotSummaryProps) {
  return (
    <div class="block">
      <p class="title is-4">Snapshot: {props.snapshot.checksum}</p>
      <p class="subtitle is-6">
        <strong>Files:</strong> {props.snapshot.fileCount},{' '}
        <strong>Started:</strong>{' '}
        {new Date(props.snapshot.startTime).toLocaleString()},{' '}
        <strong>Finished:</strong>{' '}
        {new Date(props.snapshot.endTime).toLocaleString()}
      </p>
    </div>
  );
}

const GET_TREE: TypedDocumentNode<Query, QueryTreeArgs> = gql`
  query Tree($digest: Checksum!) {
    tree(digest: $digest) {
      entries {
        name
        modTime
        reference
      }
    }
  }
`;

const RESTORE_FILES: TypedDocumentNode<Mutation, MutationRestoreFilesArgs> =
  gql`
    mutation Restore(
      $tree: Checksum!
      $entry: String!
      $filepath: String!
      $dataset: String!
    ) {
      restoreFiles(
        tree: $tree
        entry: $entry
        filepath: $filepath
        dataset: $dataset
      )
    }
  `;

interface TreeViewerProps {
  dataset: string;
  digest: string;
}

function TreeViewer(props: TreeViewerProps) {
  const navigate = useNavigate();
  const [store, setStore] = createStore({
    paths: [['/', props.digest]],
    selections: [] as string[]
  });
  const client = useApolloClient();
  const [treeQuery] = createResource(
    () => store.paths.at(-1)![1],
    async (digest: string) => {
      const { data } = await client.query({
        query: GET_TREE,
        variables: { digest }
      });
      return data;
    }
  );
  const restoreAction = action(
    async (): Promise<{ ok: boolean }> => {
      const tree = store.paths.at(-1)![1];
      const basepath = store.paths
        .slice(1)
        .map((e) => e[0])
        .join('/');
      for (const entry of store.selections) {
        const filepath = basepath.length > 0 ? basepath + '/' + entry : entry;
        const result = await client.mutate({
          mutation: RESTORE_FILES,
          variables: {
            tree,
            entry,
            filepath,
            dataset: props.dataset
          }
        });
        if (!result.data?.restoreFiles) {
          return { ok: false };
        }
      }
      return { ok: true };
    },
    {
      name: 'restoreFiles',
      onComplete: (s: Submission<any, any>) => {
        if (s.error) {
          console.error('file restore failed:', s.error);
        } else {
          navigate('/restore');
        }
      }
    }
  );
  const startRestore = useAction(restoreAction);
  const restoreSubmission = useSubmission(restoreAction);

  return (
    <Show when={treeQuery()} fallback="..." keyed>
      <nav class="level">
        <div class="level-left">
          <div class="level-item">
            <button
              class="button"
              disabled={
                store.selections.length === 0 || restoreSubmission.pending
              }
              on:click={() => startRestore()}
            >
              <span class="icon">
                <i class="fa-solid fa-clock-rotate-left" aria-hidden="true"></i>
              </span>
              <span>Restore</span>
            </button>
          </div>
          <div class="level-item">
            <button
              class="button"
              disabled={store.paths.length == 1}
              on:click={(_) => {
                setStore('selections', []);
                setStore('paths', (paths) => {
                  return paths.slice(0, -1);
                });
              }}
            >
              <span class="icon">
                <i class="fa-solid fa-arrow-up" aria-hidden="true"></i>
              </span>
              <span>Up</span>
            </button>
          </div>
          <div class="level-item">
            <nav class="breadcrumb" aria-label="breadcrumbs">
              <ul>
                <For each={store.paths}>
                  {(item, index) => (
                    <li
                      classList={{
                        'is-active': index() == store.paths.length - 1
                      }}
                    >
                      <a
                        on:click={() => {
                          setStore('selections', []);
                          setStore('paths', (paths) =>
                            paths.slice(0, index() + 1)
                          );
                        }}
                      >
                        {item[0]}
                      </a>
                    </li>
                  )}
                </For>
              </ul>
            </nav>
          </div>
        </div>
        <div class="level-right">
          <div class="level-item">
            {treeQuery()?.tree?.entries.length} entries
          </div>
        </div>
      </nav>
      <table class="table is-striped is-hoverable is-fullwidth has-text-left">
        <thead>
          <tr>
            <th>
              <input
                type="checkbox"
                name="select-all"
                checked={
                  store.selections.length === treeQuery()?.tree?.entries!.length
                }
                on:change={(ev) => {
                  ev.preventDefault();
                  if (
                    store.selections.length ===
                    treeQuery()?.tree?.entries!.length
                  ) {
                    setStore('selections', []);
                  } else {
                    const selections = [];
                    for (const entry of treeQuery()?.tree?.entries!) {
                      selections.push(entry.name);
                    }
                    setStore('selections', selections);
                  }
                }}
              />
            </th>
            <th>Name</th>
            <th>Date</th>
            <th>Reference</th>
          </tr>
        </thead>
        <tbody>
          <For each={treeQuery()?.tree?.entries}>
            {(item, index) => (
              <tr
                style={
                  item.reference.startsWith('tree-') ? 'cursor: pointer;' : ''
                }
              >
                <td>
                  <input
                    type="checkbox"
                    name={`select-${index()}`}
                    checked={store.selections.includes(item.name)}
                    on:change={(ev) => {
                      ev.preventDefault();
                      setStore('selections', (selections) => {
                        if (selections.includes(item.name)) {
                          return selections.filter((e) => e !== item.name);
                        } else {
                          return [...selections, item.name];
                        }
                      });
                    }}
                  />
                </td>
                <td
                  on:click={(_) => {
                    if (item.reference.startsWith('tree-')) {
                      setStore('selections', []);
                      setStore('paths', store.paths.length, [
                        item.name,
                        item.reference.slice(5)
                      ]);
                    }
                  }}
                >
                  <span class="icon">
                    <i
                      class="fa-regular"
                      classList={{
                        'fa-file': item.reference.startsWith('file-'),
                        'fa-folder': item.reference.startsWith('tree-')
                      }}
                    ></i>
                  </span>
                  <code>{item.name}</code>
                </td>
                <td>{new Date(item.modTime).toLocaleString()}</td>
                <td>
                  <code>{item.reference.slice(5)}</code>
                </td>
              </tr>
            )}
          </For>
        </tbody>
      </table>
    </Show>
  );
}
