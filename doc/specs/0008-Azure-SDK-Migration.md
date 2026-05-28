# Azure SDK Migration

The Azure pack store (`stores/store_azure`) is built on the community-led Azure Rust SDK (`azure_core`, `azure_storage`, `azure_storage_blobs`), which has been frozen at version `0.21.0` for some time. In parallel, Microsoft has shipped an officially supported Azure SDK for Rust whose foundational crates have now reached `1.0.0` GA. The old crates will not receive further releases — including security or authentication fixes — so the Azure pack store should be migrated onto the new SDK.

## Background

There are two distinct Azure Rust SDKs on crates.io:

1. **Community SDK (frozen at 0.21.0)** — `azure_core`, `azure_storage` (umbrella), `azure_storage_blobs` (plural), `azure_storage_queues`, `azure_storage_datalake`. This is what `store_azure` uses today.
2. **Official Microsoft SDK (1.0 GA)** — `azure_core` 1.0, `azure_identity` 1.0, `azure_storage_blob` 1.0 (**singular**), `azure_storage_queue` 1.0, plus the `azure_security_keyvault_*` 1.0 crates. The `azure_storage` umbrella crate is gone; each service lives in its own top-level crate.

For our blob-only use case, all the pieces we need exist in the new SDK — no missing functionality blocks the migration.

## Why This Is a Rewrite, Not a Bump

A `cargo outdated` upgrade is not viable because:

- The `azure_storage` umbrella that we currently import from (`StorageCredentials`, `CloudLocation`, `ErrorKind`) has no direct replacement; auth and configuration are reshaped across `azure_core` and `azure_identity` in the new SDK.
- `azure_storage_blob` follows Microsoft's pan-language SDK guidelines (fluent builders, separate `*ClientOptions` configuration types, streaming bodies via `azure_core::http::Body` / `ResponseBody`). The shape is quite different from the `azure_storage_blobs::prelude::*` API we use today.
- Every blob call site (~27 across roughly 568 lines of `stores/store_azure/src/lib.rs`) needs to be re-expressed against the new types.

## Proposal

Rewrite `stores/store_azure/src/lib.rs` against the new SDK while leaving the `store_core` trait surface and the `Configuration::properties` schema (`account`, `access_key`, `access_tier`, `custom_uri`) unchanged so that no migration is required for existing pack stores in the database.

### Dependency Changes

In `stores/store_azure/Cargo.toml`:

- Remove: `azure_core = "0.21.0"`, `azure_storage = "0.21.0"`, `azure_storage_blobs = "0.21.0"`.
- Add: `azure_core = "1"`, `azure_storage_blob = "1"`, and `azure_identity = "1"` if needed for `StorageSharedKeyCredential` / connection-string-style auth. Remove the comment about keeping the azure crates in sync — the new SDK ships coordinated 1.0 crates.

### Implementation Scope

Each of the following needs to be re-expressed against the new SDK; the existing behavior should be preserved:

1. **Client construction.** Build a `BlobServiceClient` (or equivalent) from the configured account name and access key, honoring the optional `custom_uri` override used today for Azurite. Match the existing retry policy (today configured via `RetryOptions`).
2. **Container create / ensure-exists.** Today via `container_client.create().await`, mapping `ContainerAlreadyExists` to a no-op.
3. **Blob upload.** Today via `put_block_blob` with the in-memory body and an MD5 hash (`Hash::MD5`). The new SDK uploads via `BlobClient::upload` with a `RequestContent` / streaming body; the MD5 integrity check needs to be re-applied through whatever headers/options the new API exposes.
4. **Blob download.** Today via `get().into_stream()`; the new SDK exposes `BlobClient::download` returning a `Response` whose body can be streamed into `tokio::fs`.
5. **Blob listing.** Today via `container_client.list_blobs().into_stream()` consuming `BlobItem::Blob`. The new SDK exposes a paged iterator with its own item type.
6. **Blob delete.** Straightforward equivalent in the new SDK.
7. **Error mapping.** The inner `azure_core::Error` matching used to detect `StatusCode::Conflict` for `CollisionError`, and the `ErrorKind`-based dispatch in the error-conversion path, both need to be ported to the new SDK's error types.
8. **MD5 helpers.** The local `md5sum_blob` helper that returns `[u8; 16]` is independent of the SDK and stays as-is.

### Testing

The existing integration tests run against Azurite via `containers/docker-compose.yml`. They should pass unchanged once the rewrite is complete. If the new SDK exposes Azurite-targeted helpers (e.g. emulator endpoint constants), prefer those over hand-built URIs.

### Out of Scope

- Switching authentication mechanisms (e.g. moving to managed identity or `DefaultAzureCredential`). The store currently uses shared-key auth and should continue to do so; broader auth options can be added later.
- Migrating the `azure_storage_queues` or `azure_storage_datalake` crates — we don't use them.

## Risks

- **API maturity.** The new SDK is fresh at 1.0; edge cases (e.g. streaming-upload checksum verification, retry-on-throttle behavior) may require more glue code than the older crates needed.
- **Azurite compatibility.** The new SDK targets the production Azure API; verify Azurite parity for every operation we use, particularly container creation and listing with continuation tokens.
- **Effort.** Roughly half a day to a day of focused work plus an integration-test pass, depending on how cleanly the new streaming-body API maps to our existing `tokio::fs` chunked upload/download pattern.
