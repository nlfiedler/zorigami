//
// Copyright (c) 2026 Nathan Fiedler
//
import { createResource, createSignal, Show, Suspense } from 'solid-js';
import {
  action,
  useAction,
  useSubmission,
  type Submission
} from '@solidjs/router';
import { type TypedDocumentNode, gql } from '@apollo/client';
import { useApolloClient } from '../apollo-provider';
import {
  type BucketNamingPolicyInput,
  BucketPolicyKind,
  type Mutation,
  type MutationSetBucketNamingPolicyArgs,
  type Query
} from 'zorigami/generated/graphql.ts';

const CONFIGURATION: TypedDocumentNode<Query, Record<string, never>> = gql`
  query {
    configuration {
      hostname
      username
      computerId
      computerBucket
      bucketNaming {
        policy
        days
        limit
      }
    }
  }
`;

const SET_BUCKET_NAMING_POLICY: TypedDocumentNode<
  Mutation,
  MutationSetBucketNamingPolicyArgs
> = gql`
  mutation SetBucketNamingPolicy($policy: BucketNamingPolicyInput) {
    setBucketNamingPolicy(policy: $policy) {
      bucketNaming {
        policy
        days
        limit
      }
    }
  }
`;

export function Settings() {
  const client = useApolloClient();
  const [confQuery] = createResource(async () => {
    const { data } = await client.query({ query: CONFIGURATION });
    return data;
  });
  return (
    <Suspense fallback={'...'}>
      <div class="section">
        <h2 class="title mt-4">Backup Configuration</h2>
        <ul>
          <li>
            <strong>Hostname:</strong> {confQuery()?.configuration.hostname}
          </li>
          <li>
            <strong>Username:</strong> {confQuery()?.configuration.username}
          </li>
          <li>
            <strong>Computer ID:</strong>{' '}
            {confQuery()?.configuration.computerId}
          </li>
          <li>
            <strong>Database Bucket:</strong>{' '}
            {confQuery()?.configuration.computerBucket}
          </li>
        </ul>
      </div>
      <Show when={confQuery()}>
        {(conf) => (
          <BucketNamingForm bucketNaming={conf().configuration.bucketNaming} />
        )}
      </Show>
    </Suspense>
  );
}

interface BucketNamingFormProps {
  bucketNaming:
    | {
        policy: BucketPolicyKind;
        days?: number | null;
        limit?: number | null;
      }
    | null
    | undefined;
}

function BucketNamingForm(props: BucketNamingFormProps) {
  const initial = props.bucketNaming;
  const [policy, setPolicy] = createSignal<BucketPolicyKind>(
    initial?.policy ?? BucketPolicyKind.RandomPool
  );
  const [limit, setLimit] = createSignal<number>(initial?.limit ?? 100);
  const [days, setDays] = createSignal<number>(initial?.days ?? 1);
  const [errorMsg, setErrorMsg] = createSignal('');
  const [success, setSuccess] = createSignal(false);

  const client = useApolloClient();
  const saveAction = action(
    async (): Promise<{ ok: boolean }> => {
      const input: BucketNamingPolicyInput = { policy: policy() };
      const kind = policy();
      if (
        kind === BucketPolicyKind.RandomPool ||
        kind === BucketPolicyKind.ScheduledRandomPool
      ) {
        input.limit = limit();
      }
      if (
        kind === BucketPolicyKind.Scheduled ||
        kind === BucketPolicyKind.ScheduledRandomPool
      ) {
        input.days = days();
      }
      await client.mutate({
        mutation: SET_BUCKET_NAMING_POLICY,
        variables: { policy: input }
      });
      return { ok: true };
    },
    {
      name: 'setBucketNamingPolicy',
      onComplete: (s: Submission<any, any>) => {
        if (s.error) {
          console.error('set bucket naming policy failed:', s.error);
          setErrorMsg(String(s.error.message ?? s.error));
          setSuccess(false);
        } else {
          setErrorMsg('');
          setSuccess(true);
        }
      }
    }
  );
  const startSave = useAction(saveAction);
  const saveSubmission = useSubmission(saveAction);

  const limitEnabled = () =>
    policy() === BucketPolicyKind.RandomPool ||
    policy() === BucketPolicyKind.ScheduledRandomPool;
  const daysEnabled = () =>
    policy() === BucketPolicyKind.Scheduled ||
    policy() === BucketPolicyKind.ScheduledRandomPool;
  const limitInvalid = () => limitEnabled() && limit() < 0;
  const daysInvalid = () => daysEnabled() && days() < 0;

  return (
    <div class="section">
      <div class="container">
        <form on:submit={(ev) => ev.preventDefault()}>
          <h2 class="title mt-4">Bucket Naming Policy</h2>
          <nav class="mb-4 level">
            <div class="level-right">
              <div class="level-item">
                <button
                  type="button"
                  class="button is-primary"
                  classList={{
                    'is-loading': saveSubmission.pending,
                    'is-success': success()
                  }}
                  disabled={limitInvalid() || daysInvalid()}
                  on:click={() => startSave()}
                >
                  <span class="icon">
                    <i
                      class={
                        success() ? 'fas fa-check' : 'fa-solid fa-floppy-disk'
                      }
                    ></i>
                  </span>
                  <span>Save</span>
                </button>
              </div>
            </div>
          </nav>
          <Show when={errorMsg().length > 0}>
            <div class="notification is-warning">
              <button class="delete" on:click={() => setErrorMsg('')}></button>
              {errorMsg()}
            </div>
          </Show>
          <div class="mb-2 field is-horizontal">
            <div class="field-label is-normal">
              <label class="label" for="policy-input">
                Naming Policy
              </label>
            </div>
            <div class="field-body">
              <div class="field is-narrow">
                <div class="control">
                  <span class="select is-fullwidth">
                    <select
                      id="policy-input"
                      on:change={(ev) =>
                        setPolicy(ev.target.value as BucketPolicyKind)
                      }
                    >
                      <option
                        value={BucketPolicyKind.RandomPool}
                        selected={policy() === BucketPolicyKind.RandomPool}
                      >
                        Random Pool
                      </option>
                      <option
                        value={BucketPolicyKind.Scheduled}
                        selected={policy() === BucketPolicyKind.Scheduled}
                      >
                        Scheduled
                      </option>
                      <option
                        value={BucketPolicyKind.ScheduledRandomPool}
                        selected={
                          policy() === BucketPolicyKind.ScheduledRandomPool
                        }
                      >
                        Scheduled Random Pool
                      </option>
                    </select>
                  </span>
                </div>
              </div>
            </div>
          </div>
          <div class="mb-2 field is-horizontal">
            <div class="field-label is-normal">
              <label class="label" for="limit-input">
                Bucket Limit
              </label>
            </div>
            <div class="field-body">
              <div class="field is-narrow">
                <div class="control">
                  <input
                    id="limit-input"
                    class="input"
                    classList={{ 'is-danger': limitInvalid() }}
                    type="number"
                    min="1"
                    value={limit()}
                    disabled={!limitEnabled()}
                    on:input={(ev) => {
                      const parsed = Number.parseInt(ev.target.value, 10);
                      setLimit(Number.isNaN(parsed) ? 0 : parsed);
                    }}
                  />
                </div>
                <Show
                  when={limitInvalid()}
                  fallback={
                    <p class="help">Maximum number of buckets to create.</p>
                  }
                >
                  <p class="help is-danger">
                    Bucket limit must not be negative.
                  </p>
                </Show>
              </div>
            </div>
          </div>
          <div class="mb-2 field is-horizontal">
            <div class="field-label is-normal">
              <label class="label" for="days-input">
                Days
              </label>
            </div>
            <div class="field-body">
              <div class="field is-narrow">
                <div class="control">
                  <input
                    id="days-input"
                    class="input"
                    classList={{ 'is-danger': daysInvalid() }}
                    type="number"
                    min="1"
                    value={days()}
                    disabled={!daysEnabled()}
                    on:input={(ev) => {
                      const parsed = Number.parseInt(ev.target.value, 10);
                      setDays(Number.isNaN(parsed) ? 0 : parsed);
                    }}
                  />
                </div>
                <Show
                  when={daysInvalid()}
                  fallback={<p class="help">Days between new buckets.</p>}
                >
                  <p class="help is-danger">Days must not be negative.</p>
                </Show>
              </div>
            </div>
          </div>
        </form>
      </div>
    </div>
  );
}
