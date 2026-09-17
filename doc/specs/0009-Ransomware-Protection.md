# Ransomware Protection

Zorigami today offers no defense against an attacker who gains control of the
machine running the server. Such an attacker inherits everything the backup
process holds: full-access credentials to every pack store, and unrestricted
access to an unauthenticated GraphQL control plane. With those, the backups can
be deleted directly at the store, or destroyed indirectly by weakening retention
policy and letting the scheduled pruner do the work. The headline mitigation is
**immutable backups** (storage-side WORM), but immutability alone is
insufficient: it only holds if zorigami's own identity cannot override the
retention that protects the objects. This spec lays out the work in three tiers,
ordered so that each tier reinforces the one before it.

## Threat Model

The protection target is the realistic ransomware scenario: **the host running
the zorigami server is compromised.** The attacker then possesses, simultaneously:

1. **The pack-store credentials.** Every store currently authenticates with an
   identity that can delete. The deployment docs provision `AmazonS3FullAccess`,
   Azure `Storage Blob Data Contributor`, and GCS `Storage Admin`. The
   `PackDataSource` trait (`server/src/domain/sources.rs:186`) exposes
   `delete_object` / `delete_bucket`, and all six backends implement
   unconditional deletes (S3/MinIO `delete_object`, Azure `blob_client.delete`,
   GCS `objects().delete`, local `fs::remove_file`, SFTP `sftp.unlink`).

2. **The control plane.** `server/src/main.rs` serves the GraphQL API with
   `.allow_any_origin()` and **no authentication or authorization**. The
   unauthenticated `updateStore` / `updateDataset` mutations can rewrite
   retention policy; the pruner (`server/src/tasks/prune.rs`) then enforces
   whatever it finds on the next scheduled run. Setting
   `SnapshotRetention::COUNT(1)` or `PackRetention::DAYS(0)` weaponizes
   zorigami's own pruning against its backups.

Out of scope: protecting the *source* data on the host being backed up (that is
the host's problem, not the backup tool's), and protecting against a compromise
of the cloud provider account itself.

The design principle that follows from this model: **the immutability guarantee
must live somewhere the compromised host's credentials cannot reach.** That is
why Tier 1 (storage-side compliance locks) and Tier 2 (credential separation)
are both required — neither is sufficient alone.

---

## Tier 1 — Storage-Side Immutability (WORM) — ✅ Implemented

Shipped across `4d4ce5e` (S3/MinIO), `9a6ef7c` (Azure), `7a65fbf` (GCS), plus
follow-up hardening (`13e103c`, `356c6be`, `2225742`). See git history for
details; the design below reflects what was built.

This is the load-bearing change. Pack files are content-addressed and never
mutated after upload, which maps cleanly onto Write-Once-Read-Many storage.

### Object-lock support per backend

- **S3 (`store_amazon`)** — S3 Object Lock in **compliance mode** with a
  per-object retention period. In compliance mode no principal, including the
  account root, can delete or overwrite an object before its retention expires.
  Set via `x-amz-object-lock-mode` / `x-amz-object-lock-retain-until-date` on
  the `store_pack` PutObject (`stores/store_amazon/src/lib.rs:187`). Object Lock
  requires a versioned bucket with lock enabled at creation time.
- **MinIO (`store_minio`)** — same S3 Object Lock API; gated on the bucket being
  created with object lock enabled.
- **Azure (`store_azure`)** — time-based immutability policy on the container (or
  version-level immutability), in **locked** state. Applied through the new
  official SDK's immutability-policy API on upload
  (`stores/store_azure/src/lib.rs`, `store_pack`).
- **GCS (`store_google`)** — bucket retention policy and/or per-object holds via
  the storage hub on `store_pack` (`stores/store_google/src/lib.rs`).
- **SFTP (`store_sftp`) and local (`store_local`)** — no WORM primitive exists.
  These remain explicitly "unprotected" tiers; the UI and docs must say so.

### Configuration schema

Add an immutability setting to the `Store` entity's `properties` map (the same
mechanism each backend already uses for per-store config — see
`server/src/domain/entities.rs:251`). Proposed key: `lock_days` (absent or `0`
means no lock, preserving current behavior and keeping the change
backward-compatible for existing stores). The lock window must be **greater than
or equal to** the store's `PackRetention::DAYS` value (see Pruning below).

### Pruning interaction — the main code change

Object Lock makes deletes **fail** until retention expires. The pruner deletes
old packs to reclaim space (`prune_pack_locations` →
`PackRepository::delete_pack` in `server/src/tasks/prune.rs`), so two things must
change:

1. **Align lock window with retention.** `lock_days >= PackRetention::DAYS`, so
   that by the time the pruner wants to delete an object its lock has already
   expired. Validate this invariant when a store is created or updated; reject
   configurations where a lock would outlive its retention and silently wedge
   pruning forever.
2. **Tolerate still-locked deletes.** A delete that fails because the object is
   still under retention must be treated as a soft, retryable outcome — the
   location is kept and retried on the next run — not as a hard error that aborts
   the prune pass. This mirrors the existing per-location failure handling
   introduced for pack pruning (see `doc/specs/0005-Pack-Pruning.md`), which
   already captures per-object failures via `ErrorOperation::PackPrune` and
   continues. The new case is distinguishing "locked, retry later" from a genuine
   error so it does not spam the error log.

### Bucket/container provisioning

Object Lock (S3/MinIO) and locked immutability (Azure) generally must be enabled
at bucket/container **creation** time, or require an explicit one-time enable
step. The store's ensure-bucket-exists path needs to create the bucket with lock
enabled when `lock_days > 0`. Document the manual provisioning steps in
`doc/DEPLOY.md` for stores created out-of-band.

---

## Tier 2 — Credential Separation (Defense in Depth) — ✅ Implemented

Shipped in `f5b4cf5` (backend append-only credentials) and `faf9e59` (frontend
exposure of the setting). See git history for details; the design below
reflects what was built.

Even with compliance-mode locks, the backup path should not hold delete rights at
all. Splitting the identity means a compromised host can append new backups but
cannot issue any delete, making the storage lock a backstop rather than the only
line of defense.

- **Backup / upload identity** — `PutObject` (and bucket-list / get for restore)
  only, **no** `DeleteObject` / `DeleteBucket`. This is the credential the
  long-running server holds.
- **Pruning** — either (a) a separate, restricted identity used only by the prune
  task and ideally not resident on the same always-on process, or (b) drop
  app-driven deletion of locked objects entirely and lean on storage-native
  lifecycle/expiration rules to reclaim space after the lock window. Option (b)
  removes the delete capability from zorigami altogether and is the stronger
  posture where the provider supports lifecycle expiration of expired-lock
  objects.

### Implementation notes

- The `Store` `properties` schema already carries per-backend credentials; a
  second optional credential set (or a flag selecting "upload-only") fits the
  same map. Default behavior (single full-access credential) is preserved when
  the new fields are absent.
- IAM/role guidance per provider belongs in `doc/DEPLOY.md`: example least-
  privilege policies granting `s3:PutObject` without `s3:DeleteObject`, an Azure
  custom role without blob-delete, and a GCS role without `storage.objects.delete`.
- This tier is independent of Tier 1 and can ship separately, but is far more
  valuable once Tier 1 locks exist, since lifecycle-based reclamation depends on
  the lock window.

---

## Tier 3 — Control-Plane Hardening

Independent of storage, the unauthenticated API is a direct path to data
destruction and to weakening the policies the other tiers depend on.

**Status:** no auth/session/token infrastructure exists anywhere in the
codebase today (confirmed by full-text search of `server/src`). This section
is now an implementation-ready design, not just a proposal; decisions below
were made explicitly rather than left open, per the "Auth scheme choice" item
in Open Questions.

### Authentication — static bearer token

**Decision:** a single shared secret via a new `API_TOKEN` env var, checked as
an `Authorization: Bearer <token>` header. Rejected alternatives:
username/password+sessions (needs a user store and password hashing for a
tool with exactly one operator — unjustified complexity) and mTLS (strong, but
cert issuance/rotation is heavy operational burden for a self-hosted
single-user tool, and doesn't fit a browser GraphiQL session). This matches
the project's existing config style (`LazyLock` + `std::env::var`, e.g.
`ERROR_RETENTION_DAYS` in `server/src/main.rs:50-55`) and the CORS layer
already whitelists the `AUTHORIZATION` header (`main.rs:199`) without anything
reading it yet.

- **Config:** `API_TOKEN` (optional). Absent = auth disabled, matching this
  project's existing opt-in pattern for security controls (`lock_days`,
  `append_only`) — preserves today's zero-config local dev experience but
  means any network-exposed deployment **must** set it; call this out
  prominently in `doc/DEPLOY.md` and in the top-level env var table in
  `CLAUDE.md`.
- **Scope of the gate:** the whole `/graphql` endpoint (queries *and*
  mutations), not just mutations. The spec's "at minimum" bar only requires
  mutations, but reliably distinguishing a query from a mutation
  pre-execution would mean parsing the GraphQL document before handing it to
  Juniper — not worth the complexity when uniform gating is strictly stronger
  and simpler. `/graphiql` (the IDE page) and `/liveness` stay reachable
  unauthenticated: GraphiQL is static HTML with no data of its own (the
  operator pastes the bearer token into its own header panel to issue
  queries), and `/liveness` is a health check.
- **Where it's enforced:** the `graphql()` handler (`main.rs:105-118`). Add an
  `HttpRequest` parameter to read the `Authorization` header before calling
  `data.execute(&st, &ctx)`; on a missing/mismatched token, return `401`
  without touching `GraphContext` or the schema at all. This is coarser than a
  per-resolver check but is one call site instead of ~15 resolvers
  (`server/src/preso/graphql.rs:1469-1683`) each needing a guard.
- **Comparison:** constant-time, via the `subtle` crate (new dependency;
  `server/Cargo.toml` has no auth/crypto crate today beyond `rand`/`sha1` and
  transitive `ring`). A plain `==` on a secret risks a timing side-channel;
  `subtle::ConstantTimeEq` is a small, no-std-friendly, well-audited way to
  avoid adding a maintenance burden.
- **Audit context:** thread the caller's remote address (`HttpRequest::peer_addr()`)
  into `GraphContext` (`server/src/preso/graphql.rs:28-46`) alongside the
  existing `datasource`/`leader`/`errors` fields, so audit log lines below can
  record *where* a destructive/policy-weakening call came from. There is no
  per-user identity with a single shared token, so this is the only "who"
  signal available — document that limitation rather than implying more
  attribution than the scheme provides.
- **CORS:** drop `.allow_any_origin()` (`main.rs:197`) unconditionally — it
  undermines the bearer-token header allowlist that's already there
  (`main.rs:199`) by letting any origin send it cross-site. Default to
  same-origin only (the frontend is served by the same Actix app in
  production, so no explicit allowed-origin is needed there). Add an optional
  `CORS_ALLOWED_ORIGINS` env var (comma-separated) for the one legitimate
  cross-origin case: running the Vite dev server on a different port against
  a local backend.
- **Frontend:** `client/apollo-provider.tsx:9-24` builds a bare `HttpLink`
  with no auth link today, and there is no login/token-entry UI anywhere
  under `client/pages/` or `client/components/`. Add: (a) a token field on
  the Settings page, persisted to `localStorage`; (b) an Apollo `setContext`
  link (from `@apollo/client/link/context`), composed before the `HttpLink`,
  that reads the token from `localStorage` and sets the `Authorization`
  header; (c) minimal 401 handling — an error link that surfaces "unauthorized,
  set your API token in Settings" instead of a silent/confusing GraphQL error.
  This is real new UI, not just plumbing — track it as its own step.

### Privileged, retention-weakening operations

Treat the following as privileged even among authenticated callers, because they
are the levers an attacker pulls to make zorigami delete its own data:

- `updateStore` / `updateDataset` when they **reduce** retention
  (`PackRetention` or `SnapshotRetention` toward fewer/shorter), or reduce a
  store's `lock_days`.
- `deleteDataset` (`server/src/domain/usecases/delete_dataset.rs:19-24` —
  currently a one-line passthrough with no safeguards) and `deleteStore`
  (`server/src/domain/usecases/delete_store.rs:19-35` — already guards against
  deleting an in-use store).

**Decision — hard reject, no override, for retention/lock-window reduction.**
`updateStore`/`updateDataset` must unconditionally refuse a request that would
weaken retention or shorten `lock_days`; there is no `confirm: true` argument
and no separate admin token that pushes it through. This is deliberately the
same "hardest single thing to do" bar the spec calls for: silent retention
reduction through the always-listening API is exactly the attack this tier
closes, and a second in-band override defeats that. If an operator has a
genuine, deliberate need to shorten retention, they do it out-of-band (direct
database edit, or delete-and-recreate the store/dataset — both of which are
themselves authenticated and audit-logged, and neither is achievable silently
through a single API call). This guard does **not** apply to `deleteDataset` /
`deleteStore` themselves — outright deletion is already a visible, singular,
authenticated action (not a config edit that looks routine), so for those two
the Tier 3 requirement is simply: require authentication (above) and audit-log
every call (below). `delete_store.rs`'s existing in-use guard is unaffected.

Implementation, following the existing "fetch existing, compare old vs new,
reject on weakening" shape already used for `lock_days`/`append_only` in
`update_store.rs:39-61`:

- **`update_store.rs`** — add a `PackRetention` comparison next to the
  existing `lock_days`/`append_only` checks (same `if let Some(existing) =
  self.repo.get_store(&store.id)?` block). Ordering: `ALL` is strongest
  (infinite retention); `ALL → DAYS(n)` is always a reduction; `DAYS(n) →
  DAYS(m)` is a reduction iff `m < n`. Add a
  `PackRetention::is_weaker_than(&self, other: &PackRetention) -> bool` helper
  in `server/src/domain/entities.rs` next to the enum (line 242) to keep the
  comparison out of the use case.
- **`update_dataset.rs`** — currently has *no* old-vs-new comparison at all
  before overwriting `retention` (`dataset.retention = params.retention` at
  line 64), unlike its sibling basepath-change guard (lines 44-49). Add the
  same shape: fetch the existing dataset (already done at line 42 for the
  basepath check), compare `SnapshotRetention`. Ordering is not fully linear
  (`ALL`/`COUNT(n)`/`DAYS(n)`/`AUTO` are different dimensions), so the rule is
  conservative by design: `ALL → anything else` is a reduction; `COUNT(n) →
  COUNT(m)` or `DAYS(n) → DAYS(m)` are reductions iff `m < n`; any change that
  *switches policy type* (e.g. `COUNT(1000) → DAYS(7)`, or anything `→ AUTO`)
  is also treated as a reduction and rejected, because it's not provably safe
  and "hard reject, no override" means ambiguous cases fail closed rather than
  being guessed at. Add the mirroring `SnapshotRetention::is_weaker_than(...)`
  helper next to that enum (line 370).
- Both use cases currently have **zero logging** in `call()` — add the audit
  logging described next as part of this same change, not as a follow-up.

### Audit logging — log lines, no persisted store

**Decision:** follow the existing background-task convention
(`log::warn!`/`info!` in `server/src/tasks/prune.rs` and `leader.rs`, e.g.
`prune.rs:625` `warn!("pack-prune: {}", msg)`) rather than adding a persisted,
queryable audit table. A SQLite-backed `AuditRepository` cloned from
`ErrorRepositoryImpl` (`server/src/data/repositories/errors.rs`) was
considered — it would survive log rotation and be inspectable from the UI —
but is materially more work (new entity, repository trait, two data-source
impls, GraphQL type, its own retention/pruning) for a single-operator tool
that presumably already centralizes logs. Revisit if that assumption turns
out wrong in practice.

- **Tag:** `"audit: "` prefix, mirroring the existing `"pack-prune: "` /
  `"scrub: "` style, so these lines are easy to grep out of mixed log output.
- **What gets logged, and where:**
  - `delete_dataset.rs` — one line per call: dataset id, remote address,
    outcome (there's no existing guard to log a rejection for, so this is
    always a success line until/unless a future guard is added here).
  - `delete_store.rs` — one line per call, including the existing in-use
    rejection (currently silent beyond the returned `Err`).
  - `update_store.rs` / `update_dataset.rs` — one line whenever a call
    *touches* `retention`, `lock_days`, or `append_only`, whether accepted or
    rejected, with old → new value and outcome. This is the line that makes a
    retention-weakening *attempt* observable even though it's blocked, per
    the spec's original intent — the value here is evidence of an attack
    attempt, not just a record of successful changes.
  - `delete_captured_error` / `clear_captured_errors`
    (`server/src/preso/graphql.rs:1673,1679`) — lower priority, but worth
    including since they're destructive on the error log; flagged as a
    stretch item, not blocking the rest of Tier 3.
- **Caveat to document:** with a single shared bearer token there is no
  per-user identity, so "who" in the audit line is only ever "an
  authenticated caller from `<remote addr>`" — do not design log lines or
  docs to imply richer attribution than that.

---

## Suggested Sequencing

1. **Tier 1, steps 1–2** — per-object retention on upload for S3/Azure/GCS/MinIO,
   plus the pruner change to skip still-locked objects. This delivers genuine
   WORM and is the highest-value increment. ✅ Done.
2. **Tier 2** — split the backup credential from any delete capability, so the
   locks hold even under host compromise. ✅ Done.
3. **Tier 3** — authentication on the API and a hard guard on
   retention-weakening, closing the self-destruct path through the API. In
   progress; suggested landing order within the tier, mirroring how Tiers 1/2
   shipped in incremental passes:
   1. Backend auth: `API_TOKEN`, the `graphql()` handler gate, CORS
      tightening, `subtle` dependency.
   2. Retention-reduction guards + audit logging in `update_store.rs` /
      `update_dataset.rs` / `delete_dataset.rs` / `delete_store.rs`.
   3. Frontend: token entry UI, Apollo `setContext` link, 401 handling.
   4. `doc/DEPLOY.md` — new "Authentication" section (same shape as the
      existing Immutable Backups / Append-Only Credentials sections), plus
      the `CLAUDE.md` env var table.

Tiers can land independently, but the protection is only complete with all three:
Tier 1 makes objects undeletable, Tier 2 ensures the compromised host cannot use
zorigami's own credentials to delete, and Tier 3 ensures it cannot rewrite the
policy that makes Tier 1 work.

## Risks and Open Questions

- **Storage cost.** Compliance-mode locks prevent early deletion, so a misjudged
  lock window directly inflates the bill. Lock duration should be a conscious,
  documented per-store decision tied to `PackRetention`.
- **Operational rigidity.** Compliance mode is deliberately unforgiving — a
  fat-fingered `lock_days` cannot be undone before expiry. Consider whether
  governance mode (which a privileged identity *can* override) is an acceptable
  weaker default for some users, with compliance mode opt-in.
- **Provisioning friction.** Enabling Object Lock / immutability typically
  requires bucket-creation-time flags or manual setup; existing buckets may not
  be retrofittable, forcing users to create new stores.
- **Azurite / MinIO test parity.** Verify the local test doubles
  (`containers/docker-compose.yml`) honor object-lock semantics, or mark those
  integration tests as requiring a real account.
- **Auth scheme choice.** ~~Tier 3 needs a decision on the credential
  model~~ Decided: a static bearer token (`API_TOKEN`). See Tier 3 above for
  the rationale and the rejected alternatives (session-based auth, mTLS).
- **Retention-guard false positives.** The `SnapshotRetention` "policy type
  switch is always a reduction" rule in Tier 3 is deliberately conservative
  and will reject some legitimate changes (e.g. `COUNT(1000) → DAYS(365)`
  that a user genuinely intends to be equivalent or stronger). Since there is
  no override by design, the only path forward for those is
  delete-and-recreate the dataset. Revisit if this proves too blunt in
  practice.
- **Single shared token has no per-user attribution.** Audit log lines can
  only say "an authenticated caller from `<remote addr>`," not who. Acceptable
  for a single-operator tool; would need revisiting if zorigami ever grows
  multi-user deployments.
