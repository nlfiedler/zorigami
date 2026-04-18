# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Project Is

Zorigami is a backup and restore application. Users define **Datasets** (directories to back up), which get stored as encrypted **Packs** in a remote **Store** (S3, Azure, GCS, MinIO, SFTP, or local). The backend exposes a GraphQL API; the frontend is a SolidJS app.

## Build & Development Commands

### Backend (Rust)

```bash
cargo build                        # Build all workspace crates
cargo test                         # Run all tests
cargo test <test_name>             # Run a single test by name
cargo test -p <crate> <test_name>  # Run a test in a specific crate
RUST_LOG=info cargo run            # Start the server (http://localhost:3000)
```

### Frontend (TypeScript + SolidJS)

```bash
bun run codegen     # Regenerate TypeScript types from GraphQL schema
bunx vite build     # Build frontend
```

### Regenerating the GraphQL SDL (after modifying graphql.rs)

```bash
env GENERATE_SDL=public/schema.graphql cargo run
bun run codegen
```

### Code Coverage

```bash
cargo install grcov
rustup component add llvm-tools
export RUSTFLAGS="-Cinstrument-coverage"
export LLVM_PROFILE_FILE="zorigami-%p-%m.profraw"
cargo clean && cargo build && cargo test
grcov . -s . --binary-path ./target/debug/ -t html --branch --ignore-not-existing -o ./target/debug/coverage/
```

## Architecture

The backend follows clean architecture with three layers:

- **`server/src/preso/`** — Presentation layer. GraphQL schema and resolvers (Juniper + Actix-web). This is the entry point for all client interactions.
- **`server/src/domain/`** — Business logic. Entities, repository traits, use cases, and domain services. No I/O here—only interfaces.
- **`server/src/data/`** — Data access. RocksDB-backed implementations of the domain repository/source traits.

### Background Tasks (`server/src/tasks/`)

Tasks run concurrently and are supervised by a "leader" actor:

- `backup.rs` — Walks filesystem, chunks files (FastCDC content-defined chunking), deduplicates, encrypts into EXAF Packs, uploads to Store.
- `restore.rs` — Downloads Packs from Store, decrypts, reconstructs files.
- `prune.rs` — Removes old Snapshots based on retention policy (count-based, time-based, or Time Machine-style).
- `schedule.rs` — Polls every 5 minutes; triggers backup/prune based on Dataset schedules.
- `leader.rs` — Supervises all tasks; coordinates start/stop lifecycle.

### Storage Crates (`stores/`)

Each `store_*` crate implements the `store_core` trait for a different backend: `store_amazon`, `store_azure`, `store_google`, `store_minio`, `store_sftp`, `store_local`.

### Database Crates (`database/`)

`database_core` defines abstract interfaces; `database_rocks` implements them with RocksDB. The server uses the database via the domain's `EntityDataSource` trait.

### Frontend (`client/`)

SolidJS pages communicate via Apollo Client (GraphQL). Pages: Home, Datasets, Snapshots, Stores, Restore, Settings. Components are in `client/components/`.

## Key Domain Concepts

- **Dataset** — A directory to back up, with a schedule and associated Store.
- **Snapshot** — A point-in-time backup; references a Tree.
- **Tree** — A directory listing with file references and metadata.
- **Chunk** — A content-addressed piece of a file (FastCDC).
- **Pack** — An encrypted EXAF archive containing chunks; uploaded to the Store.
- **Store** — A remote storage backend.

## Testing Notes

- Integration tests for stores require Docker Compose services: `containers/docker-compose.yml` (Azurite, sftp, MinIO/RustFS).
- Tests that must not run in parallel use the `serial_test` crate.
- Test database path defaults to `../tmp/test/database`.
- Environment variables for forcing task states in tests: `RESTORE_ALWAYS_PENDING=1`, `RESTORE_ALWAYS_PROCESSING=1`.

## Environment Variables

| Variable | Purpose |
|---|---|
| `RUST_LOG` | Log level (e.g., `info`, `debug`) |
| `DB_PATH` | Override RocksDB database path |
| `HOST` / `PORT` | Server bind address/port |
| `PRUNE_INTERVAL_HOURS` | Pruning check interval |
| `GENERATE_SDL` | Path to write GraphQL SDL file on startup |
| `RESTORE_ALWAYS_PENDING` | Keep restores in pending state (testing) |
| `RESTORE_ALWAYS_PROCESSING` | Keep restores in processing state (testing) |
