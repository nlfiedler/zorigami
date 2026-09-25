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
import {
  action,
  type Submission,
  useAction,
  useNavigate,
  useParams,
  useSearchParams,
  useSubmission
} from '@solidjs/router';
import { type TypedDocumentNode, gql } from '@apollo/client';
import { useApolloClient } from '../apollo-provider';
import { encodePath } from '../paths.ts';
import {
  type Mutation,
  type MutationRestoreFilesArgs,
  type PathMatch,
  type PathSearch,
  type Query,
  type QuerySearchSnapshotsArgs
} from 'zorigami/generated/graphql.ts';
import { referenceIconClass } from './diffs.tsx';

const SEARCH_SNAPSHOTS: TypedDocumentNode<Query, QuerySearchSnapshotsArgs> =
  gql`
    query SearchSnapshots($dataset: String!, $pattern: String!) {
      searchSnapshots(dataset: $dataset, pattern: $pattern) {
        matches {
          path
          snapshot {
            checksum
            startTime
          }
          entry {
            name
            modTime
            reference
          }
          parent
          current
        }
        truncated
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

type SearchResult = { search?: PathSearch; error?: string };

export function SnapshotSearch() {
  const params = useParams();
  const navigate = useNavigate();
  const client = useApolloClient();
  const [searchParams, setSearchParams] = useSearchParams();
  const pattern = (): string => {
    const value = searchParams.pattern;
    return typeof value === 'string' ? value : '';
  };
  const [input, setInput] = createSignal(pattern());
  // keep the field in sync when the URL changes (e.g. Back button)
  createEffect(on(pattern, (p) => setInput(p), { defer: true }));

  // BUG: useParams() and createResource() fail to refresh when the id path
  // parameter changes, but createEffect() will show that a change occurs;
  // work-around with createEffect() and refetch() to force the data refresh
  // (https://github.com/solidjs/solid/discussions/1745)
  const [searchQuery, { refetch }] = createResource(
    () => pattern() || undefined,
    async (value: string): Promise<SearchResult> => {
      try {
        const { data } = await client.query({
          query: SEARCH_SNAPSHOTS,
          variables: { dataset: params.id!, pattern: value },
          fetchPolicy: 'network-only'
        });
        return { search: data?.searchSnapshots };
      } catch (error: any) {
        return { error: error?.message ?? String(error) };
      }
    }
  );
  createEffect(
    on(
      () => params.id,
      () => {
        if (pattern()) refetch();
      },
      { defer: true }
    )
  );
  const matches = (): PathMatch[] => searchQuery()?.search?.matches ?? [];

  const submit = (event: SubmitEvent) => {
    event.preventDefault();
    const value = input();
    if (value.trim().length === 0 || searchQuery.loading) return;
    if (value === pattern()) {
      // same pattern, the URL will not change, so search again explicitly
      refetch();
    } else {
      setSearchParams({ pattern: value });
    }
  };

  const restoreAction = action(
    async (match: PathMatch): Promise<{ ok: boolean }> => {
      const result = await client.mutate({
        mutation: RESTORE_FILES,
        variables: {
          tree: match.parent,
          entry: match.entry.name,
          filepath: match.path,
          dataset: params.id!
        }
      });
      return { ok: !!result.data?.restoreFiles };
    },
    {
      name: 'restoreMatch',
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

  const browse = (match: PathMatch) =>
    navigate(
      `/snapshots/${params.id}/browse/${match.snapshot.checksum}${encodePath(
        match.path.split('/').slice(0, -1)
      )}`
    );
  const history = (match: PathMatch) =>
    navigate(
      `/snapshots/${params.id}/history${encodePath(match.path.split('/'))}`
    );

  return (
    <>
      <form on:submit={submit}>
        <div class="field has-addons">
          <div class="control is-expanded">
            <input
              class="input"
              type="text"
              placeholder="*.avi"
              value={input()}
              on:input={(e) => setInput(e.currentTarget.value)}
            />
          </div>
          <div class="control">
            <button
              type="submit"
              class="button is-primary"
              disabled={input().trim().length === 0 || searchQuery.loading}
            >
              <span class="icon">
                <i class="fa-solid fa-magnifying-glass" aria-hidden="true"></i>
              </span>
              <span>Search</span>
            </button>
          </div>
        </div>
        <p class="help mb-4">
          Use <code>*</code> to match any characters. Without a <code>/</code>{' '}
          the pattern matches entry names at any depth; with a <code>/</code> it
          matches the path from the top of the dataset, where <code>**</code>{' '}
          matches any number of directories. Case is ignored.
        </p>
      </form>
      <Suspense
        fallback={<progress class="progress is-small is-primary"></progress>}
      >
        <Switch>
          <Match when={searchQuery()?.error}>
            {(error) => (
              <article class="message is-danger">
                <div class="message-body">{error()}</div>
              </article>
            )}
          </Match>
          <Match when={searchQuery()?.search && matches().length === 0}>
            <article class="message">
              <div class="message-body">No entries match this pattern.</div>
            </article>
          </Match>
          <Match when={matches().length > 0}>
            <Show when={searchQuery()?.search?.truncated}>
              <article class="message is-warning">
                <div class="message-body">
                  Only the first {matches().length} matches are shown, try a
                  narrower pattern.
                </div>
              </article>
            </Show>
            <table class="table is-striped is-hoverable is-fullwidth has-text-left">
              <thead>
                <tr>
                  <th></th>
                  <th>Path</th>
                  <th>Snapshot</th>
                  <th>Date</th>
                  <th></th>
                  <th></th>
                </tr>
              </thead>
              <tbody>
                <For each={matches()}>
                  {(match) => (
                    <tr>
                      <td>
                        <span class="icon">
                          <i
                            class={referenceIconClass(match.entry.reference)}
                            aria-hidden="true"
                          ></i>
                        </span>
                      </td>
                      <td>
                        <code>/{match.path}</code>
                      </td>
                      <td>
                        {new Date(match.snapshot.startTime).toLocaleString()}
                      </td>
                      <td>{new Date(match.entry.modTime).toLocaleString()}</td>
                      <td>
                        <Show
                          when={match.current}
                          fallback={<span class="tag is-warning">deleted</span>}
                        >
                          <span class="tag is-success">current</span>
                        </Show>
                      </td>
                      <td>
                        <div class="buttons are-small is-right">
                          <button
                            class="button"
                            title="Browse the snapshot at this path"
                            on:click={() => browse(match)}
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
                            disabled={restoreSubmission.pending}
                            on:click={() => startRestore(match)}
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
                            title="Show the history of this path"
                            on:click={() => history(match)}
                          >
                            <span class="icon">
                              <i
                                class="fa-solid fa-clock-rotate-left"
                                aria-hidden="true"
                              ></i>
                            </span>
                          </button>
                        </div>
                      </td>
                    </tr>
                  )}
                </For>
              </tbody>
            </table>
          </Match>
        </Switch>
      </Suspense>
    </>
  );
}
