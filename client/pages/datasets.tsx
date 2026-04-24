//
// Copyright (c) 2026 Nathan Fiedler
//
import {
  createEffect,
  createMemo,
  createResource,
  createSignal,
  type Accessor,
  For,
  Match,
  type Setter,
  Show,
  Suspense,
  Switch
} from 'solid-js';
import {
  action,
  useAction,
  useLocation,
  useNavigate,
  useParams,
  useSubmission,
  type Submission
} from '@solidjs/router';
import { type TypedDocumentNode, gql } from '@apollo/client';
import { useApolloClient } from '../apollo-provider';
import {
  type Dataset,
  type DatasetInput,
  Frequency,
  type Mutation,
  type MutationDeleteDatasetArgs,
  type MutationUpdateDatasetArgs,
  type Query,
  type QueryDatasetArgs,
  type Schedule,
  type SnapshotRetention,
  SnapshotRetentionPolicy,
  type Store
} from 'zorigami/generated/graphql.ts';

const ALL_DATASETS: TypedDocumentNode<Query, Record<string, never>> = gql`
  query {
    datasets {
      id
      basepath
      status {
        status
      }
    }
  }
`;

const NEW_DATASET: TypedDocumentNode<Mutation, Record<string, never>> = gql`
  mutation {
    newDataset {
      id
    }
  }
`;

export function DatasetsPage(props: any) {
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
  // listen for path changes and cause the store list to refresh in case a store
  // was deleted, which does not directly impact this component
  const location = useLocation();
  // the pathname is not actually used, just listening for route changes
  createEffect(() => refetch(location.pathname));
  const newDatasetAction = action(
    async (): Promise<Dataset> => {
      const result = await client.mutate({
        mutation: NEW_DATASET
      });
      return result.data?.newDataset!;
    },
    {
      name: 'newDataset',
      onComplete: (s: Submission<any, any>) => {
        if (s.error) {
          console.error('new dataset failed:', s.error);
        } else {
          refetch();
          navigate(`/datasets/${s.result.id}`);
        }
      }
    }
  );
  const startNewDataset = useAction(newDatasetAction);

  return (
    <div class="m-4">
      <nav class="level">
        <div class="level-left">
          <div class="level-item">
            <button class="button" on:click={() => startNewDataset()}>
              <span class="icon">
                <i class="fa-solid fa-circle-plus"></i>
              </span>
              <span>New Dataset</span>
            </button>
          </div>
        </div>
      </nav>
      <div class="my-4 columns">
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
                            navigate(`/datasets/${dataset.id}`);
                          }}
                        >
                          <div class="list-item-content">
                            <div class="list-item-title">
                              {dataset.basepath}
                            </div>
                            <div class="list-item-description">
                              Status: {dataset.status.status}
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
    </div>
  );
}

export function Datasets() {
  return (
    <div class="m-4">
      <p>
        Select a data set to view its details, or use the button to create a new
        data set.
      </p>
    </div>
  );
}

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
                store, then return here to create a new data set.
              </div>
            </div>
          </div>
        </Match>
        <Match when={storesQuery()?.stores.length}>
          <div class="list-item">
            <div class="list-item-content">
              <div class="list-item-title">No Data Sets</div>
              <div class="list-item-description">
                Use the <strong>New Dataset</strong> button in the upper-left
                corner to create one of several types of pack stores.
              </div>
            </div>
          </div>
        </Match>
      </Switch>
    </Suspense>
  );
}

const GET_DATASET: TypedDocumentNode<Query, QueryDatasetArgs> = gql`
  query Dataset($id: String!) {
    dataset(id: $id) {
      id
      basepath
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
      chunkSize
      packSize
      stores
      excludes
      retention {
        policy
        value
      }
    }
  }
`;

export function DatasetDetails() {
  const params = useParams();
  const navigate = useNavigate();
  const client = useApolloClient();
  // BUG: useParams() and createResource() fail to refresh when the id path
  // parameter changes, but createEffect() will show that a change occurs;
  // work-around with useLocation() and refetch() to force the data refresh
  // (https://github.com/solidjs/solid/discussions/1745)
  const [datasetQuery, { refetch }] = createResource(
    () => params.id,
    async (id: string) => {
      const { data } = await client.query({
        query: GET_DATASET,
        variables: { id }
      });
      return data;
    }
  );
  const location = useLocation();
  // the pathname is not actually used, just listening for route changes
  createEffect(() => refetch(location.pathname));
  const deletedAction = action(async () => {
    // the current dataset was deleted, navigate away
    navigate('/datasets');
  });
  const startDeleted = useAction(deletedAction);
  const changedAction = action(async () => {
    // force the window to reload to show the changes to the dataset, not just
    // in this details pane, but in the list on the side
    window.location.reload();
  });
  const startChanged = useAction(changedAction);

  // use Show vs Suspense since our form needs the data in order to build out
  // the various elements that depend on whatever data is available; the keyed
  // attribute is necessary for Show to rebuild when the URI changes
  return (
    <Show when={datasetQuery()} fallback="..." keyed>
      <DatasetForm
        dataset={datasetQuery()?.dataset!}
        deleted={() => startDeleted()}
        changed={() => startChanged()}
      />
    </Show>
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

interface DatasetFormProps {
  dataset: Dataset;
  deleted: () => void;
  changed: () => void;
}

function DatasetForm(props: DatasetFormProps) {
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

  const [basepath, setBasepath] = createSignal(props.dataset.basepath);
  const [excludes, setExcludes] = createSignal(
    props.dataset.excludes.join(', ')
  );
  const [chunksize, setChunksize] = createSignal(
    Math.floor(props.dataset.chunkSize / 1_048_576)
  );
  const [packsize, setPacksize] = createSignal(
    Math.floor(props.dataset.packSize / 1_048_576)
  );
  const [selectedStores, setSelectedStores] = createSignal<Set<string>>(
    new Set(props.dataset.stores),
    {
      // avoid having to create a new set in order for SolidJS to notice
      equals: (prev, next) => prev.size !== next.size
    }
  );
  const [schedules, setSchedules] = createSignal<Schedule[]>(
    props.dataset.schedules
  );
  const [retention, setRetention] = createSignal(props.dataset.retention);
  const buildDataset = (): DatasetInput => {
    return {
      id: props.dataset.id,
      basepath: basepath(),
      schedules: schedules(),
      chunkSize: (chunksize() * 1_048_576).toString(),
      packSize: (packsize() * 1_048_576).toString(),
      stores: Array.from(selectedStores()),
      excludes: excludes()
        .split(',')
        .map((e) => e.trim())
        .filter((e) => e.length > 0),
      retention: {
        policy: retention().policy,
        value: retention().value
      }
    };
  };

  const [basepathError, setBasepathError] = createSignal('');
  const [chunksizeError, setChunksizeError] = createSignal('');
  const [packsizeError, setPacksizeError] = createSignal('');
  const [storesError, setStoresError] = createSignal('');
  const [invalid, setInvalid] = createSignal(false);
  const validate = () => {
    setBasepathError('');
    setChunksizeError('');
    setPacksizeError('');
    setStoresError('');
    setInvalid(false);
    if (basepath().length === 0) {
      setBasepathError('Base path cannot be empty.');
      setInvalid(true);
    }
    if (chunksize() < 1 || chunksize() > 16) {
      setChunksizeError('Chunk size must be between 1 and 16.');
      setInvalid(true);
    }
    if (packsize() < 16 || packsize() > 256) {
      setPacksizeError('Pack size must be between 16 and 256.');
      setInvalid(true);
    }
    if (selectedStores().size === 0) {
      setStoresError('At least one store must be selected.');
      setInvalid(true);
    }
  };

  return (
    <>
      <h2 class="m-4 title">Dataset</h2>
      <div class="m-4">
        <DatasetActions
          datasetId={props.dataset.id}
          invalid={invalid}
          build={buildDataset}
          deleted={props.deleted}
          changed={props.changed}
        />
      </div>
      <div class="m-4">
        <div class="mb-2 field is-horizontal">
          <div class="field-label is-normal">
            <label class="label" for="basepath-input">
              Base Path
            </label>
          </div>
          <div class="field-body">
            <div class="field">
              <p class="control is-expanded has-icons-left">
                <input
                  class="input"
                  type="text"
                  id="basepath-input"
                  placeholder="Path to the local storage."
                  value={basepath()}
                  on:input={(ev) => setBasepath(ev.currentTarget.value)}
                  on:blur={() => validate()}
                  on:change={() => validate()}
                />
                <span class="icon is-small is-left">
                  <i class="fa-solid fa-folder"></i>
                </span>
              </p>
              <Show when={basepathError().length > 0}>
                <p class="help is-danger">{basepathError()}</p>
              </Show>
            </div>
          </div>
        </div>

        <div class="mb-2 field is-horizontal">
          <div class="field-label is-normal">
            <label class="label" for="excludes-input">
              File Exclusions
            </label>
          </div>
          <div class="field-body">
            <div class="field">
              <p class="control is-expanded has-icons-left">
                <input
                  class="input"
                  type="text"
                  id="excludes-input"
                  placeholder="Comma-separated file and directory exclusions."
                  value={excludes()}
                  on:blur={(ev) => setExcludes(ev.currentTarget.value)}
                  on:change={(ev) => setExcludes(ev.currentTarget.value)}
                />
                <span class="icon is-small is-left">
                  <i class="fa-solid fa-file-circle-minus"></i>
                </span>
              </p>
              <p class="help">
                File patterns to exclude from backup, separated by commas.
              </p>
            </div>
          </div>
        </div>

        <div class="mb-2 field is-horizontal">
          <div class="field-label is-normal">
            <label class="label" for="chunksize-input">
              Chunk Size (MB)
            </label>
          </div>
          <div class="field-body">
            <div class="field">
              <p class="control is-expanded has-icons-left">
                <input
                  class="input"
                  type="number"
                  id="chunksize-input"
                  min="1"
                  max="16"
                  value={chunksize()}
                  on:blur={(ev) => {
                    setChunksize(ev.currentTarget.valueAsNumber);
                    validate();
                  }}
                  on:change={(ev) => {
                    setChunksize(ev.currentTarget.valueAsNumber);
                    validate();
                  }}
                />
                <span class="icon is-small is-left">
                  <i class="fa-solid fa-file-fragment"></i>
                </span>
              </p>
              <Show when={chunksizeError().length > 0}>
                <p class="help is-danger">{chunksizeError()}</p>
              </Show>
            </div>
          </div>
        </div>

        <div class="mb-2 field is-horizontal">
          <div class="field-label is-normal">
            <label class="label" for="packsize-input">
              Pack Size (MB)
            </label>
          </div>
          <div class="field-body">
            <div class="field">
              <p class="control is-expanded has-icons-left">
                <input
                  class="input"
                  type="number"
                  id="packsize-input"
                  min="16"
                  max="256"
                  step="16"
                  value={packsize()}
                  on:blur={(ev) => {
                    setPacksize(ev.currentTarget.valueAsNumber);
                    validate();
                  }}
                  on:change={(ev) => {
                    setPacksize(ev.currentTarget.valueAsNumber);
                    validate();
                  }}
                />
                <span class="icon is-small is-left">
                  <i class="fa-solid fa-box"></i>
                </span>
              </p>
              <Show when={packsizeError().length > 0}>
                <p class="help is-danger">{packsizeError()}</p>
              </Show>
            </div>
          </div>
        </div>

        <div class="mb-2 field is-horizontal">
          <div class="field-label">
            <label class="label">Pack Stores</label>
          </div>
          <div class="field-body">
            <div class="field is-narrow">
              <div class="control">
                <div class="checkboxes">
                  <Show when={sortedStores()} fallback={'...'}>
                    <StoreSelector
                      stores={sortedStores()}
                      selected={selectedStores}
                      setSelected={setSelectedStores}
                      validate={() => validate()}
                    />
                  </Show>
                </div>
              </div>
              <Show when={storesError().length > 0}>
                <p class="help is-danger">{storesError()}</p>
              </Show>
            </div>
          </div>
        </div>

        <Scheduler schedules={schedules} setSchedules={setSchedules} />

        <SnapshotRetentionForm
          retention={retention}
          setRetention={setRetention}
        />
      </div>
    </>
  );
}

interface StoreSelectorProps {
  stores: Store[];
  selected: Accessor<Set<string>>;
  setSelected: Setter<Set<string>>;
  validate: () => void;
}

function StoreSelector(props: StoreSelectorProps) {
  return (
    <For each={props.stores}>
      {(item) => (
        <label class="checkbox">
          <input
            type="checkbox"
            name="store"
            checked={props.selected().has(item.id)}
            on:change={() => {
              props.setSelected((v) => {
                if (v.has(item.id)) {
                  v.delete(item.id);
                } else {
                  v.add(item.id);
                }
                return v;
              });
              props.validate();
            }}
          />
          {`[${item.storeType}] ${item.label}`}
        </label>
      )}
    </For>
  );
}

function makeDefaultSchedule(): Schedule {
  return {
    frequency: Frequency.Hourly
  };
}

// Convert seconds-since-midnight into a time string for input field.
function formatTime(value: number | undefined): string {
  if (value) {
    const hours = Math.floor(value / 3600);
    const minutes = Math.floor((value % 3600) / 60);
    const hh = String(hours).padStart(2, '0');
    const mm = String(minutes).padStart(2, '0');
    return `${hh}:${mm}`;
  } else {
    return '00:00';
  }
}

// Convert the 'hh:mm' string from time into seconds-since-midnight.
function parseTime(value: string): number {
  const [hh, mm] = value.split(':');
  const hours = Number.parseInt(hh!);
  const minutes = Number.parseInt(mm!);
  return hours * 3600 + minutes * 60;
}

interface SchedulerProps {
  schedules: Accessor<Schedule[]>;
  setSchedules: Setter<Schedule[]>;
}

function Scheduler(props: SchedulerProps) {
  return (
    <>
      <For
        each={props.schedules()}
        fallback={
          <ManualSchedule
            addDefault={() => props.setSchedules([makeDefaultSchedule()])}
          />
        }
      >
        {(item, index) => (
          <ScheduleForm
            index={index()}
            schedule={item}
            update={(schedule: Schedule) => {
              props.setSchedules((l) => {
                // avoid rebuilding the array as that makes updating the value
                // using createEffect() very difficult; SolidJS will not notice
                // this change and that is just fine
                l[index()] = schedule;
                return l;
              });
            }}
            addNew={() => {
              props.setSchedules((l) => {
                const idx = index() + 1;
                const start = l.slice(0, idx);
                const middle = [makeDefaultSchedule()];
                const end = l.slice(idx);
                return start.concat(middle, end);
              });
            }}
            remove={() => {
              props.setSchedules((l) => l.filter((_, i) => i != index()));
            }}
          />
        )}
      </For>
    </>
  );
}

interface ScheduleFormProps {
  index: number;
  schedule: Schedule;
  update: (schedule: Schedule) => void;
  addNew: () => void;
  remove: () => void;
}

function ScheduleForm(props: ScheduleFormProps) {
  const [frequency, setFrequency] = createSignal(props.schedule.frequency);
  const [startTime, setStartTime] = createSignal(
    formatTime(props.schedule.timeRange?.startTime)
  );
  const [stopTime, setStopTime] = createSignal(
    formatTime(props.schedule.timeRange?.stopTime)
  );
  // createEffect() is dangerous here, but the parent carefully avoids
  // causing a rebuild by surgically updating the one array element
  createEffect(() => {
    props.update({
      frequency: frequency(),
      timeRange: {
        startTime: parseTime(startTime()),
        stopTime: parseTime(stopTime())
      }
    });
  });
  const timeDisabled = createMemo(() => frequency() == Frequency.Hourly);

  return (
    <>
      <div class="mb-2 field is-horizontal">
        <div class="field-label is-normal">
          <label class="label">Schedule</label>
        </div>
        <div class="field-body">
          <div class="field">
            <div class="control is-expanded">
              <div class="radios is-fullwidth">
                <label class="radio">
                  <input
                    type="radio"
                    name={`frequency-${props.index}`}
                    checked={frequency() == Frequency.Hourly}
                    on:change={() => setFrequency(Frequency.Hourly)}
                  />
                  Hourly
                </label>
                <label class="radio">
                  <input
                    type="radio"
                    name={`frequency-${props.index}`}
                    checked={frequency() == Frequency.Daily}
                    on:change={() => setFrequency(Frequency.Daily)}
                  />
                  Daily
                </label>
              </div>
            </div>
          </div>
          <div class="field">
            <p class="control has-icons-left">
              <input
                class="input"
                type="time"
                name={`start-time-${props.index}`}
                disabled={timeDisabled()}
                value={startTime()}
                on:change={(event) => {
                  if (event.currentTarget.value) {
                    setStartTime(event.currentTarget.value);
                  } else {
                    setStartTime('');
                  }
                }}
              />
              <span class="icon is-left">
                <i class="fa-solid fa-hourglass-start"></i>
              </span>
            </p>
            <p class="help">Start time for daily backups.</p>
          </div>
          <div class="field">
            <p class="control has-icons-left">
              <input
                class="input"
                type="time"
                name={`stop-time-${props.index}`}
                disabled={timeDisabled()}
                value={stopTime()}
                on:change={(event) => {
                  if (event.currentTarget.value) {
                    setStopTime(event.currentTarget.value);
                  } else {
                    setStopTime('');
                  }
                }}
              />
              <span class="icon is-left">
                <i class="fa-solid fa-hourglass-end"></i>
              </span>
            </p>
            <p class="help">Stop time for daily backups.</p>
          </div>
          <div class="field">
            <div class="control">
              <button class="button" on:click={() => props.remove()}>
                <span class="icon">
                  <i class="fa-solid fa-minus"></i>
                </span>
                <span>Remove</span>
              </button>
            </div>
          </div>
        </div>
      </div>

      <div class="mb-2 field is-horizontal">
        <div class="field-body">
          <div class="field">
            <div class="control">
              <button class="button" on:click={() => props.addNew()}>
                <span class="icon">
                  <i class="fa-solid fa-plus"></i>
                </span>
                <span>Add Another</span>
              </button>
            </div>
          </div>
        </div>
      </div>
    </>
  );
}

interface ManualScheduleProps {
  addDefault: () => void;
}

function ManualSchedule(props: ManualScheduleProps) {
  return (
    <div class="mb-2 field is-horizontal">
      <div class="field-label is-normal">
        <label class="label">Schedule</label>
      </div>
      <div class="field-body">
        <div class="field">
          <div class="control">
            <span class="mx-4">
              No schedules defined, backup must be started manually.
            </span>
            <button class="button" on:click={() => props.addDefault()}>
              <span class="icon">
                <i class="fa-solid fa-circle-plus"></i>
              </span>
              <span>Create Schedule</span>
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

interface SnapshotRetentionFormProps {
  retention: Accessor<SnapshotRetention>;
  setRetention: Setter<SnapshotRetention>;
}

function SnapshotRetentionForm(props: SnapshotRetentionFormProps) {
  const [policy, setPolicy] = createSignal(props.retention().policy);
  const [value, setValue] = createSignal(props.retention().value);
  createEffect(() => {
    props.setRetention({
      policy: policy(),
      value: value()
    });
  });

  return (
    <>
      <div class="mb-2 field is-horizontal">
        <div class="field-label">
          <label class="label">Retention</label>
        </div>
        <div class="field-body">
          <div class="field is-narrow">
            <div class="control">
              <div class="radios">
                <label class="radio">
                  <input
                    type="radio"
                    name="retention"
                    checked={policy() === SnapshotRetentionPolicy.All}
                    on:change={() => setPolicy(SnapshotRetentionPolicy.All)}
                  />
                  All Snapshots
                </label>
                <label class="radio">
                  <input
                    type="radio"
                    name="retention"
                    checked={policy() === SnapshotRetentionPolicy.Count}
                    on:change={() => setPolicy(SnapshotRetentionPolicy.Count)}
                  />
                  Limited by Count
                </label>
                <label class="radio">
                  <input
                    type="radio"
                    name="retention"
                    checked={policy() === SnapshotRetentionPolicy.Days}
                    on:change={() => setPolicy(SnapshotRetentionPolicy.Days)}
                  />
                  Limited by Days
                </label>
                <label class="radio">
                  <input
                    type="radio"
                    name="retention"
                    checked={policy() === SnapshotRetentionPolicy.Auto}
                    on:change={() => setPolicy(SnapshotRetentionPolicy.Auto)}
                  />
                  Automatic
                </label>
              </div>
            </div>
          </div>
        </div>
      </div>

      <div class="mb-2 field is-horizontal">
        <div class="field-label is-normal">
          <label class="label" for="retention-count">
            Count Limit
          </label>
        </div>
        <div class="field-body">
          <div class="field">
            <div class="control">
              <p class="control">
                <input
                  class="input"
                  type="number"
                  id="retention-count"
                  min="1"
                  max="1024"
                  value={value()}
                  on:change={(ev) => setValue(ev.target.valueAsNumber)}
                  disabled={policy() !== SnapshotRetentionPolicy.Count}
                />
              </p>
            </div>
          </div>
        </div>
      </div>

      <div class="mb-2 field is-horizontal">
        <div class="field-label is-normal">
          <label class="label" for="retention-days">
            Days Limit
          </label>
        </div>
        <div class="field-body">
          <div class="field">
            <div class="control">
              <p class="control">
                <input
                  class="input"
                  type="number"
                  id="retention-days"
                  min="1"
                  max="1024"
                  value={value()}
                  on:change={(ev) => setValue(ev.target.valueAsNumber)}
                  disabled={policy() !== SnapshotRetentionPolicy.Days}
                />
              </p>
            </div>
          </div>
        </div>
      </div>
    </>
  );
}

interface DatasetActionsProps {
  datasetId: string;
  invalid: Accessor<boolean>;
  build: () => DatasetInput;
  deleted: () => void;
  changed: () => void;
}

// Row of buttons for taking action on the data set, with status messages to
// provide feedback on the success or failure of the operations.
function DatasetActions(props: DatasetActionsProps) {
  const [deleteErrorMsg, setDeleteErrorMsg] = createSignal('');
  const [saveErrorMsg, setSaveErrorMsg] = createSignal('');

  return (
    <>
      <nav class="mb-4 level">
        <div class="level-left">
          <div class="level-item">
            <DeleteDatasetButton
              datasetId={props.datasetId}
              setError={setDeleteErrorMsg}
              deleted={props.deleted}
            />
          </div>
        </div>
        <div class="level-right">
          <div class="level-item">
            <SaveDatasetButton
              disabled={props.invalid}
              build={props.build}
              setError={setSaveErrorMsg}
              saved={props.changed}
            />
          </div>
        </div>
      </nav>
      <Show when={deleteErrorMsg().length > 0}>
        <div class="notification is-warning">
          <button
            class="delete"
            on:click={() => setDeleteErrorMsg('')}
          ></button>
          {deleteErrorMsg()}
        </div>
      </Show>
      <Show when={saveErrorMsg().length > 0}>
        <div class="notification is-warning">
          <button class="delete" on:click={() => setSaveErrorMsg('')}></button>
          {saveErrorMsg()}
        </div>
      </Show>
    </>
  );
}

const UPDATE_DATASET: TypedDocumentNode<Mutation, MutationUpdateDatasetArgs> =
  gql`
    mutation UpdateDataset($dataset: DatasetInput!) {
      updateDataset(dataset: $dataset) {
        id
      }
    }
  `;

interface SaveDatasetButtonProps {
  disabled: Accessor<boolean>;
  build: () => DatasetInput;
  setError: Setter<string>;
  saved: () => void;
}

function SaveDatasetButton(props: SaveDatasetButtonProps) {
  const client = useApolloClient();
  const [success, setSuccess] = createSignal(false);
  const updateAction = action(
    async (): Promise<{ ok: boolean }> => {
      const input = props.build();
      await client.mutate({
        mutation: UPDATE_DATASET,
        variables: {
          dataset: input
        }
      });
      return { ok: true };
    },
    {
      name: 'updateDataset',
      onComplete: (s: Submission<any, any>) => {
        if (s.error) {
          console.error('update dataset failed:', s.error);
          const msg =
            s.error?.graphQLErrors?.[0]?.message ??
            s.error?.message ??
            String(s.error);
          props.setError(msg);
          setSuccess(false);
        } else {
          props.setError('');
          setSuccess(true);
          props.saved();
        }
      }
    }
  );
  const startUpdate = useAction(updateAction);
  const updateSubmission = useSubmission(updateAction);

  return (
    <button
      class="button is-primary"
      class:is-loading={updateSubmission.pending}
      class:is-success={success()}
      disabled={props.disabled()}
      on:click={() => startUpdate()}
    >
      <span class="icon">
        <i class={success() ? 'fas fa-check' : 'fa-solid fa-floppy-disk'}></i>
      </span>
      <span>Save</span>
    </button>
  );
}

const DELETE_DATASET: TypedDocumentNode<Mutation, MutationDeleteDatasetArgs> =
  gql`
    mutation DeleteDataset($id: String!) {
      deleteDataset(id: $id)
    }
  `;

interface DeleteDatasetButtonProps {
  datasetId: string;
  setError: Setter<string>;
  deleted: () => void;
}

function DeleteDatasetButton(props: DeleteDatasetButtonProps) {
  const client = useApolloClient();
  const deleteAction = action(
    async (): Promise<{ ok: boolean }> => {
      await client.mutate({
        mutation: DELETE_DATASET,
        variables: {
          id: props.datasetId
        }
      });
      return { ok: true };
    },
    {
      name: 'deleteDataset',
      onComplete: (s: Submission<any, any>) => {
        if (s.error) {
          console.error('delete dataset failed:', s.error);
          const msg =
            s.error?.graphQLErrors?.[0]?.message ??
            s.error?.message ??
            String(s.error);
          props.setError(msg);
        } else {
          props.setError('');
          props.deleted();
        }
      }
    }
  );
  const startDelete = useAction(deleteAction);
  const deleteSubmission = useSubmission(deleteAction);

  return (
    <button
      class="button is-danger"
      disabled={deleteSubmission.pending}
      on:click={() => startDelete()}
    >
      <span class="icon">
        <i class="fa-solid fa-trash-can"></i>
      </span>
      <span>Delete</span>
    </button>
  );
}
