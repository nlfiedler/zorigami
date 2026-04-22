//
// Copyright (c) 2026 Nathan Fiedler
//
import { createResource, For, Match, Show, Suspense, Switch } from 'solid-js';
import {
  A,
  action,
  useAction,
  useSubmission,
  type Submission
} from '@solidjs/router';
import { type TypedDocumentNode, gql } from '@apollo/client';
import { useApolloClient } from '../apollo-provider';
import { AutoRefreshCheckbox } from '../components/refresh-checkbox';
import {
  type BackupState,
  type Dataset,
  type Query,
  type QuerySnapshotCountArgs,
  type Schedule,
  type Mutation,
  type MutationStartBackupArgs,
  type MutationStopBackupArgs
} from 'zorigami/generated/graphql.ts';

const ALL_DATASETS: TypedDocumentNode<Query, Record<string, never>> = gql`
  query {
    datasets {
      id
      basepath
      status {
        status
        changedFiles
        packsUploaded
        filesUploaded
        bytesUploaded
      }
      latestSnapshot {
        fileCount
      }
      schedules {
        frequency
        timeRange {
          startTime
          stopTime
        }
        weekOfMonth
        dayOfWeek
        dayOfMonth
      }
    }
  }
`;

const CAPTURED_ERROR_COUNT: TypedDocumentNode<Query, Record<string, never>> =
  gql`
    query {
      capturedErrorCount
    }
  `;

export function Home() {
  const client = useApolloClient();
  const [datasetsQuery, { refetch: refetchDatasets }] = createResource(
    async () => {
      const { data } = await client.query({ query: ALL_DATASETS });
      return data;
    }
  );
  const [errorCountQuery, { refetch: refetchErrorCount }] = createResource(
    async () => {
      const { data } = await client.query({
        query: CAPTURED_ERROR_COUNT,
        fetchPolicy: 'network-only'
      });
      return data;
    }
  );
  const refetch = () => {
    refetchDatasets();
    refetchErrorCount();
  };
  const sortedDatasets = () => {
    // the datasets returned from the server are in no particular order
    const sorted = [];
    for (const dataset of datasetsQuery()?.datasets ?? []) {
      sorted.push(dataset);
    }
    sorted.sort((a, b) => a.id.localeCompare(b.id));
    return sorted;
  };
  const errorCount = () => {
    const n = errorCountQuery()?.capturedErrorCount;
    return n ? Number(n) : 0;
  };

  return (
    <>
      <nav class="m-4 level">
        <div class="level-left">
          <div class="level-item">
            <AutoRefreshCheckbox refetch={refetch} />
          </div>
        </div>
        <Show when={errorCount() > 0}>
          <div class="level-right">
            <div class="level-item">
              <A class="button is-danger is-light" href="/errors">
                <span class="icon">
                  <i class="fa-solid fa-triangle-exclamation"></i>
                </span>
                <span>Errors ({errorCount()})</span>
              </A>
            </div>
          </div>
        </Show>
      </nav>
      <div class="container mt-4">
        <Suspense fallback={'...'}>
          <Switch>
            <Match when={sortedDatasets().length === 0}>
              <NoDatasetsHelp />
            </Match>
            <Match when={sortedDatasets().length}>
              <div class="container">
                <div class="grid is-col-min-20">
                  <For each={sortedDatasets()}>
                    {(item) => (
                      <div class="cell">
                        <DatasetCard dataset={item} />
                      </div>
                    )}
                  </For>
                </div>
              </div>
            </Match>
          </Switch>
        </Suspense>
      </div>
    </>
  );
}

interface DatasetCardProps {
  dataset: Dataset;
}

function DatasetCard(props: DatasetCardProps) {
  return (
    <div class="card">
      <header class="card-header">
        <p class="card-header-title">Dataset</p>
        <A href={`/datasets/${props.dataset.id}`}>
          <button class="card-header-icon">
            <span class="icon">
              <i class="fas fa-angle-right"></i>
            </span>
          </button>
        </A>
      </header>
      <div class="card-content">
        <table class="table is-striped is-fullwidth" style="text-align: start">
          <tbody>
            <tr>
              <td>Base Path</td>
              <td>{props.dataset.basepath}</td>
            </tr>
            <tr>
              <td>Status</td>
              <td>{props.dataset.status.status}</td>
            </tr>
            <Schedules schedules={props.dataset.schedules} />
            <Show when={props.dataset.status.startTime}>
              <tr>
                <td>Started</td>
                <td>
                  {new Date(props.dataset.status.startTime).toLocaleString()}
                </td>
              </tr>
            </Show>
            <Show when={props.dataset.status.status === 'STOPPING'}>
              <tr>
                <td>Stop Requested</td>
                <td>backup will stop soon...</td>
              </tr>
            </Show>
            <Show when={props.dataset.status.endTime}>
              <tr>
                <td>Finished</td>
                <td>
                  {new Date(props.dataset.status.endTime).toLocaleString()}
                </td>
              </tr>
            </Show>
            <Show when={props.dataset.latestSnapshot?.fileCount}>
              <tr>
                <td>Total Files</td>
                <td>{props.dataset.latestSnapshot?.fileCount}</td>
              </tr>
            </Show>
            <tr>
              <td>Files Changed</td>
              <td>{props.dataset.status.changedFiles}</td>
            </tr>
            <tr>
              <td>Files Uploaded</td>
              <td>{props.dataset.status.filesUploaded}</td>
            </tr>
            <tr>
              <td>Bytes Uploaded</td>
              <td>{props.dataset.status.bytesUploaded}</td>
            </tr>
            <tr>
              <td>Packs Uploaded</td>
              <td>{props.dataset.status.packsUploaded}</td>
            </tr>
            <Show when={props.dataset.status.errors}>
              <tr>
                <td>Error Message</td>
                <td>{props.dataset.status.errors.join(', ')}</td>
              </tr>
            </Show>
            <SnapshotCount datasetId={props.dataset.id} />
          </tbody>
        </table>
      </div>
      <footer class="card-footer">
        <StartButton
          datasetId={props.dataset.id}
          status={props.dataset.status}
        />
        <StopButton
          datasetId={props.dataset.id}
          status={props.dataset.status}
        />
      </footer>
    </div>
  );
}

// Convert seconds-since-midnight into a time string for input field.
function formatTime(value: number): string {
  const hours = Math.floor(value / 3600);
  const minutes = Math.floor((value % 3600) / 60);
  const hh = String(hours).padStart(2, '0');
  const mm = String(minutes).padStart(2, '0');
  return `${hh}:${mm}`;
}

function formatSchedule(schedule: Schedule) {
  if (schedule.frequency === 'HOURLY') {
    return 'hourly';
  } else if (schedule.frequency === 'DAILY') {
    if (schedule.timeRange) {
      if (schedule.timeRange.startTime && schedule.timeRange.stopTime) {
        const start = formatTime(schedule.timeRange.startTime);
        const stop = formatTime(schedule.timeRange.stopTime);
        return `daily from ${start} until ${stop}`;
      } else if (schedule.timeRange.startTime) {
        const start = formatTime(schedule.timeRange.startTime);
        return `daily from ${start}`;
      }
    }
    return 'daily';
  }
  // WEEKLY and MONTHLY are not yet supported in the web interface
  return '(unsupported)';
}

interface SchedulesProps {
  schedules: Schedule[];
}

function Schedules(props: SchedulesProps) {
  return (
    <For each={props.schedules}>
      {(item, index) => (
        <tr>
          <td>Schedule {index() == 0 ? '' : index() + 1}</td>
          <td>{formatSchedule(item)}</td>
        </tr>
      )}
    </For>
  );
}

const SNAPSHOT_COUNTS: TypedDocumentNode<Query, QuerySnapshotCountArgs> = gql`
  query CountSnapshots($id: String!) {
    snapshotCount(id: $id) {
      count
      newest
      oldest
    }
  }
`;

interface SnapshotCountProps {
  datasetId: string;
}

function SnapshotCount(props: SnapshotCountProps) {
  const client = useApolloClient();
  const [countsQuery] = createResource(async () => {
    const { data } = await client.query({
      query: SNAPSHOT_COUNTS,
      variables: {
        id: props.datasetId
      }
    });
    return data;
  });

  return (
    <Suspense fallback={'...'}>
      <tr>
        <td>Snapshot Count</td>
        <td>{countsQuery()?.snapshotCount.count}</td>
      </tr>
      <Show when={countsQuery()?.snapshotCount.newest}>
        <tr>
          <td>Latest Snapshot</td>
          <td>
            {new Date(countsQuery()?.snapshotCount.newest).toLocaleString()}
          </td>
        </tr>
      </Show>
      <Show when={countsQuery()?.snapshotCount.oldest}>
        <tr>
          <td>Oldest Snapshot</td>
          <td>
            {new Date(countsQuery()?.snapshotCount.oldest).toLocaleString()}
          </td>
        </tr>
      </Show>
    </Suspense>
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
          <article class="message">
            <div class="message-header">
              <p>No Pack Stores</p>
            </div>
            <div class="message-body">
              <p>
                Before defining a new dataset, visit the{' '}
                <a href="/stores">Stores</a> page to configure a pack store,
                then visit the <a href="/datasets">Datasets</a> page to
                configure a dataset to be backed up.
              </p>
              <p>
                If you wish to restore from a previous backup, first visit the{' '}
                <a href="/stores">Stores</a> page to configure a pack store,
                then visit the <a href="/restore">Restore</a> page to restore
                from the pack store.
              </p>
            </div>
          </article>
        </Match>
        <Match when={storesQuery()?.stores.length}>
          <article class="message">
            <div class="message-header">
              <p>No Data Sets</p>
            </div>
            <div class="message-body">
              Visit the <a href="/datasets">Datasets</a> page to configure a
              dataset to be backed up.
            </div>
          </article>
        </Match>
      </Switch>
    </Suspense>
  );
}

const START_BACKUP: TypedDocumentNode<Mutation, MutationStartBackupArgs> = gql`
  mutation StartBackup($id: String!) {
    startBackup(id: $id)
  }
`;

interface StartButtonProps {
  datasetId: string;
  status: BackupState;
}

function StartButton(props: StartButtonProps) {
  const client = useApolloClient();
  const startAction = action(
    async (): Promise<{ ok: boolean }> => {
      await client.mutate({
        mutation: START_BACKUP,
        variables: {
          id: props.datasetId
        }
      });
      return { ok: true };
    },
    {
      name: 'startBackup',
      onComplete: (s: Submission<any, any>) => {
        if (s.error) {
          console.error('start backup failed:', s.error);
        }
      }
    }
  );
  const startStart = useAction(startAction);
  const startSubmission = useSubmission(startAction);

  return (
    <button
      class="button card-footer-item"
      disabled={props.status.status === 'RUNNING' || startSubmission.pending}
      on:click={() => startStart()}
    >
      <span class="icon">
        <i class="fa-solid fa-play"></i>
      </span>
      <span>Start Backup</span>
    </button>
  );
}

const STOP_BACKUP: TypedDocumentNode<Mutation, MutationStopBackupArgs> = gql`
  mutation StopBackup($id: String!) {
    stopBackup(id: $id)
  }
`;

interface StopButtonProps {
  datasetId: string;
  status: BackupState;
}

function StopButton(props: StopButtonProps) {
  const client = useApolloClient();
  const stopAction = action(
    async (): Promise<{ ok: boolean }> => {
      await client.mutate({
        mutation: STOP_BACKUP,
        variables: {
          id: props.datasetId
        }
      });
      return { ok: true };
    },
    {
      name: 'stopBackup',
      onComplete: (s: Submission<any, any>) => {
        if (s.error) {
          console.error('stop backup failed:', s.error);
        }
      }
    }
  );
  const startStop = useAction(stopAction);
  const stopSubmission = useSubmission(stopAction);

  return (
    <button
      class="button card-footer-item"
      disabled={props.status.status !== 'RUNNING' || stopSubmission.pending}
      on:click={() => startStop()}
    >
      <span class="icon">
        <i class="fa-solid fa-stop"></i>
      </span>
      <span>Stop Backup</span>
    </button>
  );
}
