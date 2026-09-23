//
// Copyright (c) 2026 Nathan Fiedler
//
import { For, Match, Show, Switch } from 'solid-js';
import {
  action,
  useAction,
  useSubmission,
  type Submission
} from '@solidjs/router';
import { type TypedDocumentNode, gql } from '@apollo/client';
import { useApolloClient } from '../apollo-provider';
import {
  BackgroundOperation,
  TaskOutcome,
  type Mutation,
  type MutationStartTaskArgs,
  type Query,
  type TaskRun,
  type TaskStatus
} from 'zorigami/generated/graphql.ts';

export const TASK_STATUS: TypedDocumentNode<Query, Record<string, never>> = gql`
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

const START_TASK: TypedDocumentNode<Mutation, MutationStartTaskArgs> = gql`
  mutation StartTask($operation: BackgroundOperation!) {
    startTask(operation: $operation)
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

export function operationLabel(operation: BackgroundOperation): string {
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

interface TaskStatusTableProps {
  statuses: TaskStatus[];
  // invoked once a task has been queued, so the caller can refresh
  onStarted: () => void;
}

export function TaskStatusTable(props: TaskStatusTableProps) {
  // Not striped: each task spans several rows, one per dataset, and the
  // alternating shading would cut across those groups.
  return (
    <table class="table is-hoverable is-fullwidth has-text-left">
      <thead>
        <tr>
          <th>Task</th>
          <th>Dataset</th>
          <th>Last Run</th>
          <th>Duration</th>
          <th>Outcome</th>
          <th>Result</th>
          <th></th>
        </tr>
      </thead>
      <For each={props.statuses}>
        {(item) => (
          <tbody>
            <TaskStatusRows status={item} onStarted={props.onStarted} />
          </tbody>
        )}
      </For>
    </table>
  );
}

interface TaskStatusRowsProps {
  status: TaskStatus;
  onStarted: () => void;
}

// A task with no runs still gets a row. That a task has never run is the very
// thing an empty error log hides, so it must be visible rather than absent.
function TaskStatusRows(props: TaskStatusRowsProps) {
  const span = () => Math.max(props.status.runs.length, 1);
  const label = () => (
    <td rowSpan={span()}>{operationLabel(props.status.operation)}</td>
  );
  const button = () => (
    <td rowSpan={span()} class="has-text-right">
      <StartTaskButton
        operation={props.status.operation}
        onStarted={props.onStarted}
      />
    </td>
  );
  return (
    <Switch>
      <Match when={props.status.runs.length === 0}>
        <tr>
          {label()}
          <td colSpan="5" class="has-text-grey">
            Never run
          </td>
          {button()}
        </tr>
      </Match>
      <Match when={props.status.runs.length}>
        <For each={props.status.runs}>
          {(run, index) => (
            <tr>
              <Show when={index() === 0}>{label()}</Show>
              <TaskRunCells run={run} />
              <Show when={index() === 0}>{button()}</Show>
            </tr>
          )}
        </For>
      </Match>
    </Switch>
  );
}

interface TaskRunCellsProps {
  run: TaskRun;
}

function TaskRunCells(props: TaskRunCellsProps) {
  const tag = () => OUTCOME_TAGS[props.run.outcome];
  return (
    <>
      <td>
        <Show when={props.run.datasetId} fallback="-">
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
    </>
  );
}

interface StartTaskButtonProps {
  operation: BackgroundOperation;
  onStarted: () => void;
}

function StartTaskButton(props: StartTaskButtonProps) {
  const client = useApolloClient();
  const startAction = action(
    async (): Promise<{ ok: boolean }> => {
      await client.mutate({
        mutation: START_TASK,
        variables: {
          operation: props.operation
        }
      });
      return { ok: true };
    },
    {
      name: 'startTask',
      onComplete: (s: Submission<any, any>) => {
        if (s.error) {
          console.error('start task failed:', s.error);
        } else {
          props.onStarted();
        }
      }
    }
  );
  const startTask = useAction(startAction);
  const startSubmission = useSubmission(startAction);

  return (
    <button
      class="button is-small"
      title={`Run ${operationLabel(props.operation)} now`}
      disabled={startSubmission.pending}
      on:click={() => startTask()}
    >
      <span class="icon">
        <i class="fa-solid fa-play"></i>
      </span>
      <span>Run Now</span>
    </button>
  );
}
