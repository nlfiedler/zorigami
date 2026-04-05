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
  OptionalTextInput,
  RequiredHiddenInput,
  RequiredTextInput
} from '../components/text-field.tsx';
import useClickOutside from '../hooks/use-click-outside.ts';
import {
  type Mutation,
  type MutationNewStoreArgs,
  type MutationDeleteStoreArgs,
  type MutationTestStoreArgs,
  type MutationUpdateStoreArgs,
  type Property,
  type Query,
  type QueryStoreArgs,
  type PackRetention,
  type Store,
  type StoreInput,
  PackRetentionPolicy
} from 'zorigami/generated/graphql.ts';

// see server/src/domain/entities.rs::StoreType for the available types
type StoreType = {
  kind: string;
  label: string;
  properties: { name: string; value: string }[];
};
const STORE_TYPES: StoreType[] = [
  {
    kind: 'amazon',
    label: 'Amazon',
    properties: [
      { name: 'region', value: 'us-east-1' },
      { name: 'access_key', value: 'EXAMPLE_ACCESS_KEY' },
      { name: 'secret_key', value: 'EXAMPLE_SECRET_KEY' },
      { name: 'storage', value: 'STANDARD_IA' }
    ]
  },
  {
    kind: 'azure',
    label: 'Azure',
    properties: [
      { name: 'account', value: 'my-storage' },
      { name: 'access_key', value: 'EXAMPLE_ACCESS_KEY' },
      { name: 'access_tier', value: 'Cool' },
      { name: 'custom_uri', value: '' }
    ]
  },
  {
    kind: 'google',
    label: 'Google',
    properties: [
      { name: 'credentials', value: '/path/to/credentials.json' },
      { name: 'project', value: 'example-project-123' },
      { name: 'region', value: 'us-west1' },
      { name: 'storage', value: 'NEARLINE' }
    ]
  },
  {
    kind: 'local',
    label: 'Local',
    properties: [{ name: 'basepath', value: '.' }]
  },
  {
    kind: 'minio',
    label: 'MinIO',
    properties: [
      { name: 'region', value: 'us-west-1' },
      { name: 'endpoint', value: 'http://192.168.1.1:9000' },
      { name: 'access_key', value: 'EXAMPLE_ACCESS_KEY' },
      { name: 'secret_key', value: 'EXAMPLE_SECRET_KEY' }
    ]
  },
  {
    kind: 'sftp',
    label: 'SFTP',
    properties: [
      { name: 'address', value: '127.0.0.1:22' },
      { name: 'username', value: 'scott' },
      { name: 'password', value: 'tiger' },
      { name: 'basepath', value: '.' }
    ]
  }
];

const NEW_STORE: TypedDocumentNode<Mutation, MutationNewStoreArgs> = gql`
  mutation NewStore(
    $kind: String!
    $label: String!
    $properties: [PropertyInput!]!
  ) {
    newStore(kind: $kind, label: $label, properties: $properties) {
      id
    }
  }
`;

const DELETE_STORE: TypedDocumentNode<Mutation, MutationDeleteStoreArgs> = gql`
  mutation DeleteStore($id: String!) {
    deleteStore(id: $id)
  }
`;

interface DeleteStoreButtonProps {
  storeId: string;
  deleted: () => void;
}

function DeleteStoreButton(props: DeleteStoreButtonProps) {
  const client = useApolloClient();
  const deleteAction = action(
    async (): Promise<{ ok: boolean }> => {
      await client.mutate({
        mutation: DELETE_STORE,
        variables: {
          id: props.storeId
        }
      });
      return { ok: true };
    },
    {
      name: 'deleteStore',
      onComplete: (s: Submission<any, any>) => {
        if (s.error) {
          console.error('delete store failed:', s.error);
        } else {
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

const TEST_STORE: TypedDocumentNode<Mutation, MutationTestStoreArgs> = gql`
  mutation TestStore($store: StoreInput!) {
    testStore(store: $store)
  }
`;

interface TestStoreButtonProps {
  build: () => StoreInput;
  setError: Setter<string>;
}

function TestStoreButton(props: TestStoreButtonProps) {
  const client = useApolloClient();
  const [success, setSuccess] = createSignal(false);
  const testAction = action(
    async (): Promise<string> => {
      const input = props.build();
      const result = await client.mutate({
        mutation: TEST_STORE,
        variables: {
          store: input
        }
      });
      return result.data?.testStore!;
    },
    {
      name: 'testStore',
      onComplete: (s: Submission<any, any>) => {
        if (s.error) {
          console.error('test store failed:', s.error);
          props.setError(s.error);
          setSuccess(false);
        } else if (s.result === 'OK') {
          props.setError('');
          setSuccess(true);
        } else {
          console.error('test store error:', s.result);
          props.setError(s.result);
          setSuccess(false);
        }
      }
    }
  );
  const startTest = useAction(testAction);
  const testSubmission = useSubmission(testAction);

  return (
    <button
      class="button"
      class:is-loading={testSubmission.pending}
      class:is-success={success()}
      disabled={testSubmission.pending}
      on:click={() => startTest()}
    >
      <span class="icon">
        <i class="fa-solid fa-satellite-dish"></i>
      </span>
      <span>Test</span>
    </button>
  );
}

const UPDATE_STORE: TypedDocumentNode<Mutation, MutationUpdateStoreArgs> = gql`
  mutation UpdateStore($store: StoreInput!) {
    updateStore(store: $store) {
      id
    }
  }
`;

interface SaveStoreButtonProps {
  disabled: Accessor<boolean>;
  build: () => StoreInput;
  setError: Setter<string>;
  saved: () => void;
}

function SaveStoreButton(props: SaveStoreButtonProps) {
  const client = useApolloClient();
  const [success, setSuccess] = createSignal(false);
  const updateAction = action(
    async (): Promise<{ ok: boolean }> => {
      const input = props.build();
      await client.mutate({
        mutation: UPDATE_STORE,
        variables: {
          store: input
        }
      });
      return { ok: true };
    },
    {
      name: 'updateStore',
      onComplete: (s: Submission<any, any>) => {
        if (s.error) {
          console.error('update store failed:', s.error);
          props.setError(s.error);
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

interface StoreActionsProps {
  storeId: string;
  invalid: Accessor<boolean>;
  build: () => StoreInput;
  deleted: () => void;
  changed: () => void;
}

// Row of buttons for taking action on the store, with status messages to
// provide feedback on the success or failure of the operations.
function StoreActions(props: StoreActionsProps) {
  const [testErrorMsg, setTestErrorMsg] = createSignal('');
  const [saveErrorMsg, setSaveErrorMsg] = createSignal('');

  return (
    <>
      <nav class="mb-4 level">
        <div class="level-left">
          <div class="level-item">
            <DeleteStoreButton
              storeId={props.storeId}
              deleted={props.deleted}
            />
          </div>
        </div>
        <div class="level-right">
          <div class="level-item">
            <TestStoreButton build={props.build} setError={setTestErrorMsg} />
          </div>
          <div class="level-item">
            <SaveStoreButton
              disabled={props.invalid}
              build={props.build}
              setError={setSaveErrorMsg}
              saved={props.changed}
            />
          </div>
        </div>
      </nav>
      <Show when={testErrorMsg().length > 0}>
        <div class="notification is-warning">
          <button class="delete" on:click={() => setTestErrorMsg('')}></button>
          {testErrorMsg()}
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

const ALL_STORES: TypedDocumentNode<Query, Record<string, never>> = gql`
  query {
    stores {
      id
      storeType
      label
    }
  }
`;

export function StoresPage(props: any) {
  const navigate = useNavigate();
  const [dropdownOpen, setDropdownOpen] = createSignal(false);
  let dropdownRef: HTMLDivElement | undefined;
  useClickOutside(
    () => dropdownRef,
    () => setDropdownOpen(false)
  );
  const client = useApolloClient();
  const [storesQuery, { refetch }] = createResource(async () => {
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
  // listen for path changes and cause the store list to refresh in case a store
  // was deleted, which does not directly impact this component
  const location = useLocation();
  // the pathname is not actually used, just listening for route changes
  createEffect(() => refetch(location.pathname));
  const newStoreAction = action(
    async (type: StoreType): Promise<Store> => {
      const result = await client.mutate({
        mutation: NEW_STORE,
        variables: {
          kind: type.kind,
          label: type.label,
          properties: type.properties
        }
      });
      return result.data?.newStore!;
    },
    {
      name: 'newStore',
      onComplete: (s: Submission<any, any>) => {
        if (s.error) {
          console.error('new store failed:', s.error);
        } else {
          refetch();
          navigate(`/stores/${s.result.id}`);
        }
      }
    }
  );
  const startCreate = useAction(newStoreAction);

  return (
    <div class="m-4">
      <nav class="level">
        <div class="level-left">
          <div class="level-item">
            <div
              class="dropdown"
              class:is-active={dropdownOpen()}
              ref={(el: HTMLDivElement) => (dropdownRef = el)}
            >
              <div class="dropdown-trigger">
                <button
                  class="button"
                  on:click={() => setDropdownOpen((v) => !v)}
                  aria-haspopup="true"
                  aria-controls="dropdown-menu"
                >
                  <span class="icon">
                    <i class="fa-solid fa-circle-plus"></i>
                  </span>
                  <span>New Store</span>
                </button>
              </div>
              <div class="dropdown-menu" id="dropdown-menu" role="menu">
                <div class="dropdown-content">
                  <For each={STORE_TYPES}>
                    {(item) => (
                      <a
                        class="dropdown-item"
                        on:click={() => {
                          startCreate(item);
                          setDropdownOpen(false);
                        }}
                      >
                        {item.label}
                      </a>
                    )}
                  </For>
                </div>
              </div>
            </div>
          </div>
        </div>
      </nav>
      <div class="my-4 columns">
        <div class="column is-one-quarter">
          <div class="box">
            <div class="list has-hoverable-list-items">
              <Suspense fallback={'...'}>
                <Switch>
                  <Match when={storesQuery()?.stores.length === 0}>
                    <div class="list-item">
                      <div class="list-item-content">
                        <div class="list-item-title">No Pack Stores</div>
                        <div class="list-item-description">
                          Use the <strong>New Store</strong> button in the
                          upper-left corner to create one of several types of
                          pack stores.
                        </div>
                      </div>
                    </div>
                  </Match>
                  <Match when={storesQuery()?.stores.length}>
                    <For each={sortedStores()}>
                      {(store) => (
                        <div
                          class="list-item"
                          on:click={() => {
                            navigate(`/stores/${store.id}`);
                          }}
                        >
                          <div class="list-item-content">
                            <div class="list-item-title">{store.label}</div>
                            <div class="list-item-description">
                              {store.storeType}
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

export function Stores() {
  return (
    <div class="m-4">
      <p>
        Select a pack store to view its details, or use the dropdown to create a
        new pack store.
      </p>
    </div>
  );
}

const GET_STORE: TypedDocumentNode<Query, QueryStoreArgs> = gql`
  query Store($id: String!) {
    store(id: $id) {
      id
      storeType
      label
      properties {
        name
        value
      }
      retention {
        policy
        value
      }
    }
  }
`;

export function StoreDetails() {
  const params = useParams();
  const navigate = useNavigate();
  const client = useApolloClient();
  // BUG: useParams() and createResource() fail to refresh when the id path
  // parameter changes, but createEffect() will show that a change occurs;
  // work-around with useLocation() and refetch() to force the data refresh
  // (https://github.com/solidjs/solid/discussions/1745)
  const [storeQuery, { refetch }] = createResource(
    () => params.id,
    async (id: string) => {
      const { data } = await client.query({
        query: GET_STORE,
        variables: { id }
      });
      return data;
    }
  );
  const location = useLocation();
  // the pathname is not actually used, just listening for route changes
  createEffect(() => refetch(location.pathname));
  const deletedAction = action(async () => {
    // the current store was deleted, navigate away
    navigate('/stores');
  });
  const startDeleted = useAction(deletedAction);
  const changedAction = action(async () => {
    // force the window to reload to show the changes to the pack store, not
    // just in this details pane, but in the list on the side
    window.location.reload();
  });
  const startChanged = useAction(changedAction);

  // use Show vs Suspense since our form needs the data in order to build out
  // the various elements that depend on whatever data is available; the keyed
  // attribute is necessary for Show to rebuild when the URI changes
  return (
    <Show when={storeQuery()} fallback="..." keyed>
      <Switch
        fallback={
          <div>
            Unknown pack store type{' '}
            <code>{storeQuery()?.store!.storeType}</code>
          </div>
        }
      >
        <Match when={storeQuery()?.store!.storeType === 'amazon'}>
          <AmazonStoreForm
            store={storeQuery()?.store!}
            deleted={() => startDeleted()}
            changed={() => startChanged()}
          />
        </Match>
        <Match when={storeQuery()?.store!.storeType === 'azure'}>
          <AzureStoreForm
            store={storeQuery()?.store!}
            deleted={() => startDeleted()}
            changed={() => startChanged()}
          />
        </Match>
        <Match when={storeQuery()?.store!.storeType === 'google'}>
          <GoogleStoreForm
            store={storeQuery()?.store!}
            deleted={() => startDeleted()}
            changed={() => startChanged()}
          />
        </Match>
        <Match when={storeQuery()?.store!.storeType === 'local'}>
          <LocalStoreForm
            store={storeQuery()?.store!}
            deleted={() => startDeleted()}
            changed={() => startChanged()}
          />
        </Match>
        <Match when={storeQuery()?.store!.storeType === 'minio'}>
          <MinioStoreForm
            store={storeQuery()?.store!}
            deleted={() => startDeleted()}
            changed={() => startChanged()}
          />
        </Match>
        <Match when={storeQuery()?.store!.storeType === 'sftp'}>
          <SftpStoreForm
            store={storeQuery()?.store!}
            deleted={() => startDeleted()}
            changed={() => startChanged()}
          />
        </Match>
      </Switch>
    </Show>
  );
}

// Convert the outbound PackRetention object into the inbound form. Mostly this
// is to remove the __typename that the server adds to outbound values, but
// rejects for inbound values.
function buildPackRetention(value: PackRetention) {
  return {
    policy: value.policy,
    value: value.value
  };
}

interface PackRetentionFormProps {
  retention: Accessor<PackRetention>;
  setRetention: Setter<PackRetention>;
}

function PackRetentionForm(props: PackRetentionFormProps) {
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
          <label class="label" for="retention-all">
            Retention
          </label>
        </div>
        <div class="field-body">
          <div class="field is-narrow">
            <div class="control">
              <div class="radios">
                <label class="radio">
                  <input
                    type="radio"
                    id="retention-all"
                    name="retention"
                    checked={policy() === PackRetentionPolicy.All}
                    on:change={() => setPolicy(PackRetentionPolicy.All)}
                  />
                  All Packs
                </label>
                <label class="radio">
                  <input
                    type="radio"
                    id="retention-days"
                    name="retention"
                    checked={policy() === PackRetentionPolicy.Days}
                    on:change={() => setPolicy(PackRetentionPolicy.Days)}
                  />
                  Limited by Days
                </label>
              </div>
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
                  disabled={policy() !== PackRetentionPolicy.Days}
                />
              </p>
            </div>
          </div>
        </div>
      </div>
    </>
  );
}

interface LocalStoreFormProps {
  store: Store;
  deleted: () => void;
  changed: () => void;
}

function LocalStoreForm(props: LocalStoreFormProps) {
  const [label, setLabel] = createSignal(props.store.label);
  const [basepath, setBasepath] = createSignal(
    getProperty(props.store.properties, 'basepath')
  );
  const [retention, setRetention] = createSignal(props.store.retention);
  const buildStore = () => {
    return {
      id: props.store.id,
      storeType: props.store.storeType,
      label: label(),
      properties: [{ name: 'basepath', value: basepath() }],
      retention: buildPackRetention(retention())
    };
  };
  const invalid = createMemo(() => {
    return label().length === 0 || basepath().length === 0;
  });

  // use form tag to keep Chrome happy about any password fields
  return (
    <form>
      <h2 class="m-4 title">Attached Disk</h2>
      <div class="m-4">
        <StoreActions
          storeId={props.store.id}
          invalid={invalid}
          build={buildStore}
          deleted={props.deleted}
          changed={props.changed}
        />
      </div>
      <div class="m-4">
        <RequiredTextInput
          label="Label"
          name="label-input"
          field={label}
          setField={setLabel}
          placeholder="Descriptive label for the pack store."
          icon="fa-solid fa-quote-left"
        />
        <RequiredTextInput
          label="Base Path"
          name="basepath-input"
          field={basepath}
          setField={setBasepath}
          placeholder="Path to the local storage."
          icon="fa-solid fa-folder"
        />
        <PackRetentionForm retention={retention} setRetention={setRetention} />
      </div>
    </form>
  );
}

interface AmazonStoreFormProps {
  store: Store;
  deleted: () => void;
  changed: () => void;
}

function AmazonStoreForm(props: AmazonStoreFormProps) {
  const [label, setLabel] = createSignal(props.store.label);
  const [region, setRegion] = createSignal(
    getProperty(props.store.properties, 'region')
  );
  const [accessKey, setAccessKey] = createSignal(
    getProperty(props.store.properties, 'access_key')
  );
  const [secretKey, setSecretKey] = createSignal(
    getProperty(props.store.properties, 'secret_key')
  );
  const [storage, setStorage] = createSignal(
    getProperty(props.store.properties, 'storage')
  );
  const [retention, setRetention] = createSignal(props.store.retention);
  const buildStore = () => {
    return {
      id: props.store.id,
      storeType: props.store.storeType,
      label: label(),
      properties: [
        { name: 'region', value: region() },
        { name: 'access_key', value: accessKey() },
        { name: 'secret_key', value: secretKey() },
        { name: 'storage', value: storage() }
      ],
      retention: buildPackRetention(retention())
    };
  };
  const invalid = createMemo(() => {
    return (
      label().length === 0 ||
      region().length === 0 ||
      accessKey().length === 0 ||
      secretKey().length === 0
    );
  });

  // use form tag to keep Chrome happy about any password fields
  return (
    <form>
      <h2 class="m-4 title">Amazon S3</h2>
      <div class="m-4">
        <StoreActions
          storeId={props.store.id}
          invalid={invalid}
          build={buildStore}
          deleted={props.deleted}
          changed={props.changed}
        />
      </div>
      <div class="m-4">
        <RequiredTextInput
          label="Label"
          name="label-input"
          field={label}
          setField={setLabel}
          placeholder="Descriptive label for the pack store."
          icon="fa-solid fa-quote-left"
        />
        <RequiredTextInput
          label="Region"
          name="region-input"
          field={region}
          setField={setRegion}
          placeholder="Geographic region or availability zone."
          icon="fa-solid fa-globe"
        />
        <RequiredTextInput
          label="Access Key"
          name="access-input"
          field={accessKey}
          setField={setAccessKey}
          placeholder="Access key identifier."
          icon="fa-solid fa-circle-info"
        />
        <RequiredHiddenInput
          label="Secret Key"
          name="secret-input"
          field={secretKey}
          setField={setSecretKey}
          placeholder="Secret access key."
          icon="fa-solid fa-key"
        />

        <div class="mb-2 field is-horizontal">
          <div class="field-label is-normal">
            <label class="label" for="storage-input">
              Storage Class
            </label>
          </div>
          <div class="field-body">
            <div class="field is-narrow">
              <div class="control has-icons-left">
                <span class="select is-fullwidth">
                  <select
                    id="storage-input"
                    on:change={(ev) => setStorage(ev.target.value)}
                  >
                    <option selected={storage().toLowerCase() === 'standard'}>
                      STANDARD
                    </option>
                    <option
                      selected={storage().toLowerCase() === 'standard_ia'}
                    >
                      STANDARD_IA
                    </option>
                    <option selected={storage().toLowerCase() === 'glacier_ir'}>
                      GLACIER_IR
                    </option>
                  </select>
                </span>
                <span class="icon is-small is-left">
                  <i class="fas fa-hard-drive"></i>
                </span>
              </div>
            </div>
          </div>
        </div>

        <PackRetentionForm retention={retention} setRetention={setRetention} />
      </div>
    </form>
  );
}

interface AzureStoreFormProps {
  store: Store;
  deleted: () => void;
  changed: () => void;
}

function AzureStoreForm(props: AzureStoreFormProps) {
  const [label, setLabel] = createSignal(props.store.label);
  const [account, setAccount] = createSignal(
    getProperty(props.store.properties, 'account')
  );
  const [accessKey, setAccessKey] = createSignal(
    getProperty(props.store.properties, 'access_key')
  );
  const [accessTier, setAccessTier] = createSignal(
    getProperty(props.store.properties, 'access_tier')
  );
  const [customUri, setCustomUri] = createSignal(
    getProperty(props.store.properties, 'custom_uri')
  );
  const [retention, setRetention] = createSignal(props.store.retention);
  const buildStore = () => {
    return {
      id: props.store.id,
      storeType: props.store.storeType,
      label: label(),
      properties: [
        { name: 'account', value: account() },
        { name: 'access_key', value: accessKey() },
        { name: 'access_tier', value: accessTier() },
        { name: 'custom_uri', value: customUri() }
      ],
      retention: buildPackRetention(retention())
    };
  };
  const invalid = createMemo(() => {
    return (
      label().length === 0 || account().length === 0 || accessKey().length === 0
    );
  });

  // use form tag to keep Chrome happy about any password fields
  return (
    <form>
      <h2 class="m-4 title">Azure Blob Storage</h2>
      <div class="m-4">
        <StoreActions
          storeId={props.store.id}
          invalid={invalid}
          build={buildStore}
          deleted={props.deleted}
          changed={props.changed}
        />
      </div>
      <div class="m-4">
        <RequiredTextInput
          label="Label"
          name="label-input"
          field={label}
          setField={setLabel}
          placeholder="Descriptive label for the pack store."
          icon="fa-solid fa-quote-left"
        />
        <RequiredTextInput
          label="Account Name"
          name="account-input"
          field={account}
          setField={setAccount}
          placeholder="Name of the storage account."
          icon="fa-solid fa-cloud"
        />
        <RequiredHiddenInput
          label="Access Key"
          name="access-input"
          field={accessKey}
          setField={setAccessKey}
          placeholder="Access key."
          icon="fa-solid fa-key"
        />

        <div class="mb-2 field is-horizontal">
          <div class="field-label is-normal">
            <label class="label" for="tier-input">
              Access Tier
            </label>
          </div>
          <div class="field-body">
            <div class="field is-narrow">
              <div class="control has-icons-left">
                <span class="select is-fullwidth">
                  <select
                    id="tier-input"
                    on:change={(ev) => setAccessTier(ev.target.value)}
                  >
                    <option selected={accessTier().toLowerCase() === 'hot'}>
                      Hot
                    </option>
                    <option selected={accessTier().toLowerCase() === 'cool'}>
                      Cool
                    </option>
                  </select>
                </span>
                <span class="icon is-small is-left">
                  <i class="fas fa-hard-drive"></i>
                </span>
              </div>
            </div>
          </div>
        </div>

        <OptionalTextInput
          label="Custom URI"
          name="uri-input"
          field={customUri}
          setField={setCustomUri}
          placeholder="Custom URI."
          icon="fa-solid fa-link"
        />
        <PackRetentionForm retention={retention} setRetention={setRetention} />
      </div>
    </form>
  );
}

interface GoogleStoreFormProps {
  store: Store;
  deleted: () => void;
  changed: () => void;
}

function GoogleStoreForm(props: GoogleStoreFormProps) {
  const [label, setLabel] = createSignal(props.store.label);
  const [credentials, setCredentials] = createSignal(
    getProperty(props.store.properties, 'credentials')
  );
  const [project, setProject] = createSignal(
    getProperty(props.store.properties, 'project')
  );
  const [region, setRegion] = createSignal(
    getProperty(props.store.properties, 'region')
  );
  const [storage, setStorage] = createSignal(
    getProperty(props.store.properties, 'storage')
  );
  const [retention, setRetention] = createSignal(props.store.retention);
  const buildStore = () => {
    return {
      id: props.store.id,
      storeType: props.store.storeType,
      label: label(),
      properties: [
        { name: 'credentials', value: credentials() },
        { name: 'project', value: project() },
        { name: 'region', value: region() },
        { name: 'storage', value: storage() }
      ],
      retention: buildPackRetention(retention())
    };
  };
  const invalid = createMemo(() => {
    return (
      label().length === 0 ||
      credentials().length === 0 ||
      project().length === 0 ||
      region().length === 0
    );
  });

  // use form tag to keep Chrome happy about any password fields
  return (
    <form>
      <h2 class="m-4 title">Google Cloud Storage</h2>
      <div class="m-4">
        <StoreActions
          storeId={props.store.id}
          invalid={invalid}
          build={buildStore}
          deleted={props.deleted}
          changed={props.changed}
        />
      </div>
      <div class="m-4">
        <RequiredTextInput
          label="Label"
          name="label-input"
          field={label}
          setField={setLabel}
          placeholder="Descriptive label for the pack store."
          icon="fa-solid fa-quote-left"
        />
        <RequiredTextInput
          label="Credentials File"
          name="credentials-input"
          field={credentials}
          setField={setCredentials}
          placeholder="Path to JSON credentials file."
          icon="fa-solid fa-key"
        />
        <RequiredTextInput
          label="Project ID"
          name="project-id-input"
          field={project}
          setField={setProject}
          placeholder="Project identifier."
          icon="fa-solid fa-cloud"
        />
        <RequiredTextInput
          label="Region"
          name="region-input"
          field={region}
          setField={setRegion}
          placeholder="Geographic region or availability zone."
          icon="fa-solid fa-globe"
        />

        <div class="mb-2 field is-horizontal">
          <div class="field-label is-normal">
            <label class="label" for="storage-input">
              Storage Class
            </label>
          </div>
          <div class="field-body">
            <div class="field is-narrow">
              <div class="control has-icons-left">
                <span class="select is-fullwidth">
                  <select
                    id="storage-input"
                    on:change={(ev) => setStorage(ev.target.value)}
                  >
                    <option selected={storage().toLowerCase() === 'standard'}>
                      STANDARD
                    </option>
                    <option selected={storage().toLowerCase() === 'nearline'}>
                      NEARLINE
                    </option>
                    <option selected={storage().toLowerCase() === 'coldline'}>
                      COLDLINE
                    </option>
                  </select>
                </span>
                <span class="icon is-small is-left">
                  <i class="fas fa-hard-drive"></i>
                </span>
              </div>
            </div>
          </div>
        </div>

        <PackRetentionForm retention={retention} setRetention={setRetention} />
      </div>
    </form>
  );
}

interface MinioStoreFormProps {
  store: Store;
  deleted: () => void;
  changed: () => void;
}

function MinioStoreForm(props: MinioStoreFormProps) {
  const [label, setLabel] = createSignal(props.store.label);
  const [region, setRegion] = createSignal(
    getProperty(props.store.properties, 'region')
  );
  const [endpoint, setEndpoint] = createSignal(
    getProperty(props.store.properties, 'endpoint')
  );
  const [accessKey, setAccessKey] = createSignal(
    getProperty(props.store.properties, 'access_key')
  );
  const [secretKey, setSecretKey] = createSignal(
    getProperty(props.store.properties, 'secret_key')
  );
  const [retention, setRetention] = createSignal(props.store.retention);
  const buildStore = () => {
    return {
      id: props.store.id,
      storeType: props.store.storeType,
      label: label(),
      properties: [
        { name: 'region', value: region() },
        { name: 'endpoint', value: endpoint() },
        { name: 'access_key', value: accessKey() },
        { name: 'secret_key', value: secretKey() }
      ],
      retention: buildPackRetention(retention())
    };
  };
  const invalid = createMemo(() => {
    return (
      label().length === 0 ||
      region().length === 0 ||
      endpoint().length === 0 ||
      accessKey().length === 0 ||
      secretKey().length === 0
    );
  });

  // use form tag to keep Chrome happy about any password fields
  return (
    <form>
      <h2 class="m-4 title">MinIO Object Storage</h2>
      <div class="m-4">
        <StoreActions
          storeId={props.store.id}
          invalid={invalid}
          build={buildStore}
          deleted={props.deleted}
          changed={props.changed}
        />
      </div>
      <div class="m-4">
        <RequiredTextInput
          label="Label"
          name="label-input"
          field={label}
          setField={setLabel}
          placeholder="Descriptive label for the pack store."
          icon="fa-solid fa-quote-left"
        />
        <RequiredTextInput
          label="Region"
          name="region-input"
          field={region}
          setField={setRegion}
          placeholder="Geographic region or availability zone."
          icon="fa-solid fa-globe"
        />
        <RequiredTextInput
          label="Endpoint"
          name="endpoint-input"
          field={endpoint}
          setField={setEndpoint}
          placeholder="Endpoint URL."
          icon="fa-solid fa-link"
        />
        <RequiredTextInput
          label="Access Key"
          name="access-input"
          field={accessKey}
          setField={setAccessKey}
          placeholder="Access key identifier."
          icon="fa-solid fa-circle-info"
        />
        <RequiredHiddenInput
          label="Secret Key"
          name="secret-input"
          field={secretKey}
          setField={setSecretKey}
          placeholder="Secret access key."
          icon="fa-solid fa-key"
        />

        <PackRetentionForm retention={retention} setRetention={setRetention} />
      </div>
    </form>
  );
}

interface SftpStoreFormProps {
  store: Store;
  deleted: () => void;
  changed: () => void;
}

function SftpStoreForm(props: SftpStoreFormProps) {
  const [label, setLabel] = createSignal(props.store.label);
  const [address, setAddress] = createSignal(
    getProperty(props.store.properties, 'address')
  );
  const [username, setUsername] = createSignal(
    getProperty(props.store.properties, 'username')
  );
  const [password, setPassword] = createSignal(
    getProperty(props.store.properties, 'password')
  );
  const [basepath, setBasepath] = createSignal(
    getProperty(props.store.properties, 'basepath')
  );
  const [retention, setRetention] = createSignal(props.store.retention);
  const buildStore = () => {
    return {
      id: props.store.id,
      storeType: props.store.storeType,
      label: label(),
      properties: [
        { name: 'address', value: address() },
        { name: 'username', value: username() },
        { name: 'password', value: password() },
        { name: 'basepath', value: basepath() }
      ],
      retention: buildPackRetention(retention())
    };
  };
  const invalid = createMemo(() => {
    return (
      label().length === 0 ||
      address().length === 0 ||
      username().length === 0 ||
      password().length === 0 ||
      basepath().length === 0
    );
  });

  // use form tag to keep Chrome happy about any password fields
  return (
    <form>
      <h2 class="m-4 title">Secure FTP</h2>
      <div class="m-4">
        <StoreActions
          storeId={props.store.id}
          invalid={invalid}
          build={buildStore}
          deleted={props.deleted}
          changed={props.changed}
        />
      </div>
      <div class="m-4">
        <RequiredTextInput
          label="Label"
          name="label-input"
          field={label}
          setField={setLabel}
          placeholder="Descriptive label for the pack store."
          icon="fa-solid fa-quote-left"
        />
        <RequiredTextInput
          label="Remote Address"
          name="address-input"
          field={address}
          setField={setAddress}
          placeholder="Host and port of S-FTP server."
          icon="fa-solid fa-cloud"
        />
        <RequiredTextInput
          label="Username"
          name="username-input"
          field={username}
          setField={setUsername}
          placeholder="Name of user account."
          icon="fa-solid fa-user"
        />
        <RequiredHiddenInput
          label="Password"
          name="password-input"
          field={password}
          setField={setPassword}
          placeholder="Password for user account."
          icon="fa-solid fa-key"
        />
        <RequiredTextInput
          label="Base Path"
          name="basepath-input"
          field={basepath}
          setField={setBasepath}
          placeholder="Path for remote storage."
          icon="fa-solid fa-folder"
        />

        <PackRetentionForm retention={retention} setRetention={setRetention} />
      </div>
    </form>
  );
}

function getProperty(properties: Property[], name: string): string {
  const entry = properties.find((e) => e.name === name);
  if (entry) {
    return entry.value;
  }
  return '';
}
