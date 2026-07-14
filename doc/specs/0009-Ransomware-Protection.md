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

## Tier 1 — Storage-Side Immutability (WORM)

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

## Tier 2 — Credential Separation (Defense in Depth)

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

### Authentication

Introduce authentication on the GraphQL endpoint (`server/src/main.rs`,
`server/src/preso/graphql.rs`). At minimum, gate all **mutations** behind a
credential; queries may stay open if desired, but mutations must not be callable
anonymously. The CORS `.allow_any_origin()` posture should be tightened in
tandem.

### Privileged, retention-weakening operations

Treat the following as privileged even among authenticated callers, because they
are the levers an attacker pulls to make zorigami delete its own data:

- `updateStore` / `updateDataset` when they **reduce** retention
  (`PackRetention` or `SnapshotRetention` toward fewer/shorter), or reduce a
  store's `lock_days`.
- `deleteDataset` (`server/src/domain/usecases/delete_dataset.rs` — currently no
  safeguards) and `deleteStore`
  (`server/src/domain/usecases/delete_store.rs` — already guards against deleting
  an in-use store).

Proposed guard: refuse to *shorten* retention or shorten a lock window without an
explicit out-of-band confirmation (a config flag, a separate admin token, or a
mandatory cooling-off period). Silent retention reduction is the cleanest attack
and should be the hardest single thing to do.

### Audit logging

Log every destructive or policy-weakening operation (who, when, what changed,
old → new value) so that a retention change is observable after the fact even if
it is not blocked.

---

## Suggested Sequencing

1. **Tier 1, steps 1–2** — per-object retention on upload for S3/Azure/GCS/MinIO,
   plus the pruner change to skip still-locked objects. This delivers genuine
   WORM and is the highest-value increment.
2. **Tier 2** — split the backup credential from any delete capability, so the
   locks hold even under host compromise.
3. **Tier 3** — authentication on mutations and a guard on retention-weakening,
   closing the self-destruct path through the API.

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
- **Auth scheme choice.** Tier 3 needs a decision on the credential model (static
  token, OIDC, mTLS) consistent with how zorigami is typically deployed
  (localhost-bound by default, optionally network-exposed via `HOST`).
