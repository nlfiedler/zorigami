//
// Copyright (c) 2026 Nathan Fiedler
//
import { createResource, For, Match, Show, Suspense, Switch } from 'solid-js';
import { A } from '@solidjs/router';
import { type TypedDocumentNode, gql } from '@apollo/client';
import { useApolloClient } from '../apollo-provider';
import {
  BackgroundOperation,
  TaskOutcome,
  type Mutation,
  type MutationDeleteCapturedErrorArgs,
  type Query,
  type TaskRun,
  type TaskStatus
} from 'zorigami/generated/graphql.ts';

const TASK_STATUS: TypedDocumentNode<Query, Record<string, never>> = gql`
  query TaskStatus {
    taskStatus {
      operation
      runs {
        datasetId
        startedAt
        finishedAt
        durationMillis
        outcome
        issueCount
        summary
      }
    }
  }
`;

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

const OPERATION_LABELS: Record<BackgroundOperation, string> = {
  [BackgroundOperation.Backup]: 'Backup',
  [BackgroundOperation.Prune]: 'Snapshot Pruning',
  [BackgroundOperation.RestoreTest]: 'Restore Testing',
  [BackgroundOperation.DatabaseScrub]: 'Database Scrub',
  [BackgroundOperation.PackPrune]: 'Pack Pruning',
  [BackgroundOperation.WorkspaceCleanup]: 'Workspace Cleanup'
};

function operationLabel(operation: BackgroundOperation): string {
  return OPERATION_LABELS[operation] ?? operation;
}

// Tag color per outcome. Skipped is deliberately not green: the task ran but
// verified nothing, which the user should not read as a pass.
const OUTCOME_TAGS: Record<TaskOutcome, { label: string; css: string }> = {
  [TaskOutcome.Success]: { label: 'Success', css: 'is-success' },
  [TaskOutcome.Issues]: { label: 'Issues', css: 'is-warning' },
  [TaskOutcome.Failed]: { label: 'Failed', css: 'is-danger' },
  [TaskOutcome.Skipped]: { label: 'Skipped', css: 'is-info is-light' }
};

// Each unit is floored rather than rounded: rounding the remainder up to 60
// renders "1m 60s", and rounding the seconds up renders "60.0 s".
function formatDuration(millis: number): string {
  if (millis < 1000) {
    return `${millis} ms`;
  }
  const totalSeconds = Math.floor(millis / 1000);
  if (totalSeconds < 60) {
    // floored to tenths; toFixed would round 59.96 back up to "60.0"
    return `${(Math.floor(millis / 100) / 10).toFixed(1)} s`;
  }
  const totalMinutes = Math.floor(totalSeconds / 60);
  if (totalMinutes < 60) {
    return `${totalMinutes}m ${totalSeconds % 60}s`;
  }
  return `${Math.floor(totalMinutes / 60)}h ${totalMinutes % 60}m`;
}

export function Status() {
  const client = useApolloClient();
  const [statusQuery, { refetch: refetchStatus }] = createResource(async () => {
    const { data } = await client.query({
      query: TASK_STATUS,
      fetchPolicy: 'network-only'
    });
    return data;
  });
  const [errorsQuery, { refetch: refetchErrors }] = createResource(async () => {
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
    refetchErrors();
  }

  async function clearAll() {
    if (!window.confirm('Delete all captured errors?')) {
      return;
    }
    await client.mutate({ mutation: CLEAR_CAPTURED_ERRORS });
    refetchErrors();
    // clearing the errors does not change the runs, but the issue counts
    // shown alongside them now point at rows that are gone
    refetchStatus();
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
              <span>Clear All Errors</span>
            </button>
          </div>
        </div>
      </nav>
      <div class="container mt-4">
        <h2 class="title is-5">Recent Activity</h2>
        <Suspense fallback={'...'}>
          <table class="table is-striped is-hoverable is-fullwidth has-text-left">
            <thead>
              <tr>
                <th>Task</th>
                <th>Last Run</th>
                <th>Duration</th>
                <th>Outcome</th>
                <th>Result</th>
              </tr>
            </thead>
            <tbody>
              <For each={statusQuery()?.taskStatus}>
                {(item) => <TaskStatusRows status={item} />}
              </For>
            </tbody>
          </table>
        </Suspense>

        <h2 class="title is-5 mt-6">Errors</h2>
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
                        <td>{operationLabel(item.operation)}</td>
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

interface TaskStatusRowsProps {
  status: TaskStatus;
}

// A task with no runs still gets a row. That a task has never run is the very
// thing an empty error log hides, so it must be visible rather than absent.
function TaskStatusRows(props: TaskStatusRowsProps) {
  return (
    <Switch>
      <Match when={props.status.runs.length === 0}>
        <tr>
          <td>{operationLabel(props.status.operation)}</td>
          <td colSpan="4" class="has-text-grey">
            Never run
          </td>
        </tr>
      </Match>
      <Match when={props.status.runs.length}>
        <For each={props.status.runs}>
          {(run) => <TaskRunRow operation={props.status.operation} run={run} />}
        </For>
      </Match>
    </Switch>
  );
}

interface TaskRunRowProps {
  operation: BackgroundOperation;
  run: TaskRun;
}

function TaskRunRow(props: TaskRunRowProps) {
  const tag = () => OUTCOME_TAGS[props.run.outcome];
  return (
    <tr>
      <td>
        {operationLabel(props.operation)}
        <Show when={props.run.datasetId}>
          {' '}
          <code>{props.run.datasetId}</code>
        </Show>
      </td>
      <td>{new Date(props.run.finishedAt).toLocaleString()}</td>
      <td>{formatDuration(Number(props.run.durationMillis))}</td>
      <td>
        <span class={`tag ${tag().css}`}>{tag().label}</span>
        <Show when={props.run.issueCount > 0}>
          {' '}
          <span class="has-text-grey">
            ({props.run.issueCount} issue
            {props.run.issueCount === 1 ? '' : 's'})
          </span>
        </Show>
      </td>
      <td>{props.run.summary}</td>
    </tr>
  );
}
