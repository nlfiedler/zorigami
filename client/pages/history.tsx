//
// Copyright (c) 2026 Nathan Fiedler
//
import {
  createEffect,
  createResource,
  For,
  Match,
  on,
  Show,
  Suspense,
  Switch
} from 'solid-js';
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
  type PathVersion,
  type Query,
  type QueryPathHistoryArgs
} from 'zorigami/generated/graphql.ts';
import {
  isTreeRef,
  referenceIconClass,
  type Status,
  statusIconClass
} from './diffs.tsx';
import { itemName } from './snapshots.tsx';

const PATH_HISTORY: TypedDocumentNode<Query, QueryPathHistoryArgs> = gql`
  query PathHistory($dataset: String!, $path: String!) {
    pathHistory(dataset: $dataset, path: $path) {
      snapshot {
        checksum
        startTime
      }
      change
      entry {
        name
        modTime
        reference
      }
      parent
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

export function PathHistory() {
  const params = useParams();
  const navigate = useNavigate();
  const client = useApolloClient();
  const names = () => decodePath(params.path);
  // BUG: useParams() and createResource() fail to refresh when the path
  // parameters change, but createEffect() will show that a change occurs;
  // work-around with createEffect() and refetch() to force the data refresh
  // (https://github.com/solidjs/solid/discussions/1745)
  const [historyQuery, { refetch }] = createResource(
    () => params.path,
    async () => {
      const { data } = await client.query({
        query: PATH_HISTORY,
        variables: { dataset: params.id!, path: names().join('/') }
      });
      return data;
    }
  );
  createEffect(
    on(
      () => [params.id, params.path],
      () => refetch(),
      { defer: true }
    )
  );
  const versions = (): PathVersion[] => historyQuery()?.pathHistory ?? [];

  const restoreAction = action(
    async (version: PathVersion): Promise<{ ok: boolean }> => {
      if (!version.parent || !version.entry) return { ok: false };
      const result = await client.mutate({
        mutation: RESTORE_FILES,
        variables: {
          tree: version.parent,
          entry: version.entry.name,
          filepath: names().join('/'),
          dataset: params.id!
        }
      });
      return { ok: !!result.data?.restoreFiles };
    },
    {
      name: 'restoreVersion',
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

  const browse = (version: PathVersion) =>
    navigate(
      `/snapshots/${params.id}/browse/${version.snapshot.checksum}${encodePath(
        names().slice(0, -1)
      )}`
    );

  // versions are newest first, so the previous version is the next one
  const previous = (index: number): PathVersion | undefined =>
    versions()[index + 1];
  const canCompare = (version: PathVersion, index: number): boolean => {
    const older = previous(index);
    return (
      !!older &&
      (isTreeRef(version.entry?.reference) || isTreeRef(older.entry?.reference))
    );
  };
  const compare = (version: PathVersion, index: number) => {
    const older = previous(index)!;
    navigate(
      `/snapshots/${params.id}/compare/${older.snapshot.checksum}/${
        version.snapshot.checksum
      }${encodePath(names())}`
    );
  };

  return (
    <>
      <nav class="level">
        <div class="level-left">
          <div class="level-item">
            <div class="block">
              <p class="title is-4 mb-1">Path History</p>
              <p class="subtitle is-6">
                <code>/{names().join('/')}</code>
              </p>
            </div>
          </div>
        </div>
        <div class="level-right">
          <div class="level-item">
            <Show when={historyQuery()}>{versions().length} versions</Show>
          </div>
        </div>
      </nav>
      <Suspense fallback={'...'}>
        <Switch>
          <Match when={historyQuery() && versions().length === 0}>
            <article class="message">
              <div class="message-body">No history for this path.</div>
            </article>
          </Match>
          <Match when={versions().length > 0}>
            <table class="table is-striped is-hoverable is-fullwidth has-text-left">
              <thead>
                <tr>
                  <th></th>
                  <th>Snapshot</th>
                  <th>Name</th>
                  <th>Date</th>
                  <th>Reference</th>
                  <th></th>
                </tr>
              </thead>
              <tbody>
                <For each={versions()}>
                  {(version, index) => {
                    const status = () => version.change.toLowerCase() as Status;
                    return (
                      <tr>
                        <td title={status()}>
                          <span class="icon">
                            <i
                              class={statusIconClass(status())}
                              aria-hidden="true"
                            ></i>
                          </span>
                        </td>
                        <td>
                          {new Date(
                            version.snapshot.startTime
                          ).toLocaleString()}
                        </td>
                        <Show
                          when={version.entry}
                          fallback={
                            <td colSpan={3}>
                              <em>removed</em>
                            </td>
                          }
                        >
                          {(entry) => (
                            <>
                              <td>
                                <span class="icon">
                                  <i
                                    class={referenceIconClass(
                                      entry().reference
                                    )}
                                  ></i>
                                </span>
                                <code>{entry().name}</code>
                              </td>
                              <td>
                                {new Date(entry().modTime).toLocaleString()}
                              </td>
                              <td>
                                <code>{itemName(entry().reference)}</code>
                              </td>
                            </>
                          )}
                        </Show>
                        <td>
                          <div class="buttons are-small is-right">
                            <button
                              class="button"
                              title="Browse the snapshot at this path"
                              disabled={!version.entry}
                              on:click={() => browse(version)}
                            >
                              <span class="icon">
                                <i
                                  class="fa-solid fa-folder-open"
                                  aria-hidden="true"
                                ></i>
                              </span>
                            </button>
                            <button
                              class="button"
                              title="Restore this version"
                              disabled={
                                !version.entry || restoreSubmission.pending
                              }
                              on:click={() => startRestore(version)}
                            >
                              <span class="icon">
                                <i
                                  class="fa-solid fa-trash-arrow-up"
                                  aria-hidden="true"
                                ></i>
                              </span>
                            </button>
                            <button
                              class="button"
                              title="Compare with the previous version"
                              disabled={!canCompare(version, index())}
                              on:click={() => compare(version, index())}
                            >
                              <span class="icon">
                                <i
                                  class="fa-solid fa-code-compare"
                                  aria-hidden="true"
                                ></i>
                              </span>
                            </button>
                          </div>
                        </td>
                      </tr>
                    );
                  }}
                </For>
              </tbody>
            </table>
          </Match>
        </Switch>
      </Suspense>
    </>
  );
}
