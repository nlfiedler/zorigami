//
// Copyright (c) 2026 Nathan Fiedler
//
import {
  createEffect,
  createResource,
  createSignal,
  For,
  Match,
  on,
  Show,
  Suspense,
  Switch
} from 'solid-js';
import { createStore } from 'solid-js/store';
import {
  action,
  type Submission,
  useAction,
  useNavigate,
  useParams,
  useSubmission
} from '@solidjs/router';
import { type TypedDocumentNode, gql } from '@apollo/client';
import { useApolloClient } from '../apollo-provider';
import { decodePath, encodePath } from '../paths.ts';
import {
  type Mutation,
  type MutationRestoreFilesArgs,
  type Query,
  type QuerySnapshotArgs,
  type QueryTreeArgs,
  type TreeEntry
} from 'zorigami/generated/graphql.ts';

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

export function SnapshotCompare() {
  const params = useParams();
  const navigate = useNavigate();
  const client = useApolloClient();
  const [snapshotA] = createResource(
    () => params.digestA,
    async (digest: string) => {
      const { data } = await client.query({
        query: GET_SNAPSHOT,
        variables: { digest }
      });
      return data;
    }
  );
  const [snapshotB] = createResource(
    () => params.digestB,
    async (digest: string) => {
      const { data } = await client.query({
        query: GET_SNAPSHOT,
        variables: { digest }
      });
      return data;
    }
  );

  return (
    <Show
      when={snapshotA()?.snapshot && snapshotB()?.snapshot}
      fallback="..."
      keyed
    >
      <nav class="level">
        <div class="level-left">
          <div class="level-item">
            <div class="block">
              <p class="title is-4 mb-1">Comparing Snapshots</p>
              <p class="subtitle is-6">
                <strong>A (older):</strong>{' '}
                {new Date(snapshotA()!.snapshot!.startTime).toLocaleString()}{' '}
                <code>{snapshotA()!.snapshot!.checksum}</code>
                <br />
                <strong>B (newer):</strong>{' '}
                {new Date(snapshotB()!.snapshot!.startTime).toLocaleString()}{' '}
                <code>{snapshotB()!.snapshot!.checksum}</code>
              </p>
            </div>
          </div>
        </div>
      </nav>
      <TreeDiffViewer
        dataset={params.id!}
        rootA={snapshotA()!.snapshot!.tree}
        rootB={snapshotB()!.snapshot!.tree}
        names={decodePath(params.path)}
        onNavigate={(names: string[], replace?: boolean) =>
          navigate(
            `/snapshots/${params.id}/compare/${params.digestA}/${params.digestB}${encodePath(names)}`,
            { replace }
          )
        }
      />
    </Show>
  );
}

export type Status = 'added' | 'removed' | 'changed';

interface DiffRow {
  name: string;
  status: Status;
  // entry on the A side, if any
  a: TreeEntry | null;
  // entry on the B side, if any
  b: TreeEntry | null;
}

interface PathSegment {
  name: string;
  digestA: string | null;
  digestB: string | null;
}

export const isTreeRef = (ref: string | undefined | null): boolean =>
  !!ref && ref.startsWith('tree-');

const isSelectable = (row: DiffRow): boolean =>
  row.status === 'changed' || row.status === 'removed';

// A row drills in when:
// - changed + both sides are trees (rule 6)
// - changed + types differ and either is a tree → no drill (rule 7)
// - added + B is a tree (rule 8)
// - removed + A is a tree (rule 9)
const canDrillIn = (row: DiffRow): boolean => {
  if (row.status === 'changed') {
    return isTreeRef(row.a?.reference) && isTreeRef(row.b?.reference);
  }
  if (row.status === 'added') return isTreeRef(row.b?.reference);
  if (row.status === 'removed') return isTreeRef(row.a?.reference);
  return false;
};

export const statusIconClass = (status: Status): string => {
  if (status === 'added') return 'fa-solid fa-plus';
  if (status === 'removed') return 'fa-solid fa-minus';
  return 'fa-solid fa-not-equal';
};

export const referenceIconClass = (ref: string | undefined | null): string => {
  if (!ref) return '';
  if (ref.startsWith('tree-')) return 'fa-regular fa-folder';
  if (ref.startsWith('file-')) return 'fa-regular fa-file';
  if (ref.startsWith('link-')) return 'fa-solid fa-link';
  if (ref.startsWith('small-')) return 'fa-solid fa-compress';
  return '';
};

interface TreeDiffViewerProps {
  dataset: string;
  rootA: string;
  rootB: string;
  // names of the directories leading to the one being compared
  names: string[];
  // invoked with the directory names when the user changes directory, or to
  // replace the location when the names do not exist in either snapshot
  onNavigate: (names: string[], replace?: boolean) => void;
}

function TreeDiffViewer(props: TreeDiffViewerProps) {
  const navigate = useNavigate();
  const client = useApolloClient();
  const [store, setStore] = createStore({
    paths: [
      { name: '/', digestA: props.rootA, digestB: props.rootB }
    ] as PathSegment[],
    selections: [] as string[]
  });
  // the trees are not shown until the directory names have been resolved
  const [walked, setWalked] = createSignal(false);

  const fetchTree = async (digest: string | null) => {
    if (digest === null) return null;
    const { data } = await client.query({
      query: GET_TREE,
      variables: { digest }
    });
    return data?.tree ?? null;
  };

  // resolve the directory names to pairs of tree digests whenever the roots or
  // the names change, reusing whatever prefix of the current path still applies
  let walkGen = 0;
  createEffect(
    on(
      () => [props.rootA, props.rootB, props.names.join('/')] as const,
      async ([rootA, rootB]) => {
        const gen = ++walkGen;
        const names = props.names;
        const resolved: PathSegment[] = [
          { name: '/', digestA: rootA, digestB: rootB }
        ];
        let reuse =
          store.paths[0]!.digestA === rootA &&
          store.paths[0]!.digestB === rootB;
        for (const name of names) {
          const known = store.paths[resolved.length];
          if (reuse && known && known.name === name) {
            resolved.push({ ...known });
            continue;
          }
          reuse = false;
          const current = resolved.at(-1)!;
          const [a, b] = await Promise.all([
            fetchTree(current.digestA),
            fetchTree(current.digestB)
          ]);
          if (gen !== walkGen) return;
          const refA = a?.entries.find((e) => e.name === name)?.reference;
          const refB = b?.entries.find((e) => e.name === name)?.reference;
          const digestA = isTreeRef(refA) ? refA!.slice(5) : null;
          const digestB = isTreeRef(refB) ? refB!.slice(5) : null;
          if (digestA === null && digestB === null) break;
          resolved.push({ name, digestA, digestB });
        }
        if (gen === walkGen) {
          setStore('paths', resolved);
          setStore('selections', []);
          setWalked(true);
          if (resolved.length - 1 < names.length) {
            // path does not exist in either snapshot, show the deepest match
            props.onNavigate(
              resolved.slice(1).map((p) => p.name),
              true
            );
          }
        }
      }
    )
  );

  const currentSegment = () => store.paths.at(-1)!;
  const currentNames = () => store.paths.slice(1).map((p) => p.name);
  const changeDirectory = (names: string[]) => {
    setStore('selections', []);
    props.onNavigate(names);
  };

  const [treeA] = createResource(
    () => walked() && currentSegment().digestA,
    fetchTree
  );
  const [treeB] = createResource(
    () => walked() && currentSegment().digestB,
    fetchTree
  );

  const ready = () =>
    walked() &&
    (currentSegment().digestA === null || treeA() !== undefined) &&
    (currentSegment().digestB === null || treeB() !== undefined);

  const rows = (): DiffRow[] => {
    if (!ready()) return [];
    // a resource keeps its previous value when its source becomes null, so
    // check the digests rather than trusting the resource values
    const aEntries =
      currentSegment().digestA === null ? [] : (treeA()?.entries ?? []);
    const bEntries =
      currentSegment().digestB === null ? [] : (treeB()?.entries ?? []);
    const aByName = new Map(aEntries.map((e) => [e.name, e]));
    const bByName = new Map(bEntries.map((e) => [e.name, e]));
    const result: DiffRow[] = [];
    for (const entry of aEntries) {
      const other = bByName.get(entry.name);
      if (!other) {
        result.push({
          name: entry.name,
          status: 'removed',
          a: entry,
          b: null
        });
      } else if (other.reference !== entry.reference) {
        result.push({
          name: entry.name,
          status: 'changed',
          a: entry,
          b: other
        });
      }
      // identical — elided
    }
    for (const entry of bEntries) {
      if (!aByName.has(entry.name)) {
        result.push({
          name: entry.name,
          status: 'added',
          a: null,
          b: entry
        });
      }
    }
    result.sort((x, y) => x.name.localeCompare(y.name));
    return result;
  };

  const drillInto = (row: DiffRow) => {
    if (!canDrillIn(row)) return;
    changeDirectory([...currentNames(), row.name]);
  };

  const selectableRows = () => rows().filter((r) => isSelectable(r));

  const restoreAction = action(
    async (): Promise<{ ok: boolean }> => {
      const aDigest = currentSegment().digestA;
      if (!aDigest) return { ok: false };
      const basepath = store.paths
        .slice(1)
        .map((p) => p.name)
        .join('/');
      for (const entry of store.selections) {
        const filepath = basepath.length > 0 ? basepath + '/' + entry : entry;
        const result = await client.mutate({
          mutation: RESTORE_FILES,
          variables: {
            tree: aDigest,
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
      name: 'restoreDiff',
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
    <Suspense fallback={'...'}>
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
                <i class="fa-solid fa-trash-arrow-up" aria-hidden="true"></i>
              </span>
              <span>Restore</span>
            </button>
          </div>
          <div class="level-item">
            <button
              class="button"
              disabled={store.paths.length == 1}
              on:click={() => changeDirectory(currentNames().slice(0, -1))}
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
                        on:click={() =>
                          changeDirectory(currentNames().slice(0, index()))
                        }
                      >
                        {item.name}
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
            <Show when={ready()}>{rows().length} differences</Show>
          </div>
        </div>
      </nav>
      <Show when={ready()} fallback="...">
        <Switch>
          <Match when={rows().length === 0}>
            <article class="message">
              <div class="message-body">No differences at this level.</div>
            </article>
          </Match>
          <Match when={rows().length > 0}>
            <table class="table is-striped is-hoverable is-fullwidth has-text-left">
              <thead>
                <tr>
                  <th>
                    <input
                      type="checkbox"
                      name="select-all"
                      checked={
                        selectableRows().length > 0 &&
                        store.selections.length === selectableRows().length
                      }
                      disabled={selectableRows().length === 0}
                      on:change={(ev) => {
                        ev.preventDefault();
                        const sel = selectableRows();
                        if (store.selections.length === sel.length) {
                          setStore('selections', []);
                        } else {
                          setStore(
                            'selections',
                            sel.map((r) => r.name)
                          );
                        }
                      }}
                    />
                  </th>
                  <th></th>
                  <th>Name</th>
                  <th>Date</th>
                </tr>
              </thead>
              <tbody>
                <For each={rows()}>
                  {(row, index) => {
                    // For display: prefer B-side entry (newer state) for
                    // changed/added rows; fall back to A-side for removed.
                    const display = () => row.b ?? row.a!;
                    return (
                      <tr style={canDrillIn(row) ? 'cursor: pointer;' : ''}>
                        <td on:click={(ev) => ev.stopPropagation()}>
                          <input
                            type="checkbox"
                            name={`select-${index()}`}
                            checked={store.selections.includes(row.name)}
                            disabled={!isSelectable(row)}
                            on:change={(ev) => {
                              ev.preventDefault();
                              if (!isSelectable(row)) return;
                              setStore('selections', (selections) => {
                                if (selections.includes(row.name)) {
                                  return selections.filter(
                                    (e) => e !== row.name
                                  );
                                }
                                return [...selections, row.name];
                              });
                            }}
                          />
                        </td>
                        <td title={row.status}>
                          <span class="icon">
                            <i
                              class={statusIconClass(row.status)}
                              aria-hidden="true"
                            ></i>
                          </span>
                        </td>
                        <td on:click={() => drillInto(row)}>
                          <span class="icon">
                            <i
                              class={referenceIconClass(display().reference)}
                            ></i>
                          </span>
                          <code>{row.name}</code>
                        </td>
                        <td on:click={() => drillInto(row)}>
                          {new Date(display().modTime).toLocaleString()}
                        </td>
                      </tr>
                    );
                  }}
                </For>
              </tbody>
            </table>
          </Match>
        </Switch>
      </Show>
    </Suspense>
  );
}
