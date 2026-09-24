# Path History

From the snapshots browse page (`/snapshots/.../browse/...`), selecting a file or directory will enable a button in the toolbar (next to **Restore**) named **History**, which will navigate to a new page that shows every snapshot in which that path had changed. For both files and directories, they are considered _changed_ if their checksum in the tree record is different from one snapshot to the next.

Selecting more than one file should disable the **History** button, there is little value in retrieving the history of multiple entries at the same time.

## Functional Requirements

1. In the `TreeViewer` component in @client/pages/snapshots.tsx, add a **History** button to the toolbar, immediately to the right of the **Restore** button.
1. The **History** button is enabled only when exactly one entry is selected. With zero or more than one selection, it is disabled. There is no implicit "history of the current directory" when nothing is selected; the user must select the entry whose history they want.
1. Clicking **History** navigates to a new path history page for the selected entry, whose full path is the current breadcrumb path joined with the selected entry name.
1. The path history page lists, newest first, every snapshot in which the path was _added_, _changed_, or _removed_ relative to the preceding (older) snapshot. Snapshots in which the path is unchanged are elided.
1. The oldest snapshot in which the path exists is always listed, as "added".
1. If the path is absent from a snapshot but present in the preceding one, that snapshot is listed as "removed". If the path later reappears, that snapshot is listed as "added" again.
1. If the entry changes type (e.g. a file becomes a directory), that snapshot is listed as "changed".
1. Snapshots that have not finished (no `endTime`) are excluded from the history.
1. Each row offers:
   - a link to browse that snapshot, opened at the directory containing the path (not applicable to "removed" rows);
   - a **Restore** action that restores that version of the entry, using the existing `restoreFiles` mutation (not applicable to "removed" rows);
   - for directories, a link to compare that snapshot against the previous listed version, opened at that directory.
1. Indicate "added", "removed", and "changed" with the same status icons used by the snapshot compare view (spec 0007).
1. If the path has no history at all (e.g. the URL was hand-edited), show a "no history for this path" message in place of an empty table.

## Design Details

### Change Detection

An entry in snapshot _N_ is compared to the entry with the same path in the next older snapshot _N-1_ by its full `reference` value, the same rule used by snapshot diffing (spec 0007). This covers all reference kinds uniformly: `file-` and `tree-` references carry a checksum, while `link-` and `small-` references embed the link target or file content, so comparing the whole reference is the correct test for every kind.

Note that a directory's tree digest incorporates each child's mode, uid/gid, ctime, and mtime, while a file's reference covers only its content. As a result, a directory will show as "changed" when a child's metadata alone changes (e.g. `touch`), whereas a file will not show as "changed" when only its own metadata changes. This is acceptable: the history only reports that the directory's reference changed, and makes no attempt to explain which child changed or in what manner. The user can use the compare link to find out.

### Server

Computing the history on the client would require one `tree` query per directory level per snapshot, which does not scale to datasets with hundreds of snapshots. Instead, the server provides a single query.

1. Add a use case `server/src/domain/usecases/get_path_history.rs` (registered in `usecases.rs`) that takes a dataset identifier and a path.
   - Walk the snapshot chain from the dataset head via `parent`, as `GetSnapshots` does, skipping unfinished snapshots.
   - For each snapshot, resolve the path by descending from the root tree one component at a time, recording the tree digest at each level.
   - Short-circuit: if the digest of any ancestor tree is identical to the digest at the same level in the previously examined snapshot, the entry is necessarily unchanged, so no further tree lookups are needed for that snapshot.
   - Emit a record for each snapshot where the entry was added, changed, or removed, per the rules above.
   - Unit tests use `MockRecordRepository`, covering: unchanged, changed, removed, re-added, type change, path through a missing intermediate directory, and the short-circuit path.
1. In `server/src/preso/graphql.rs`, add a query:

   ```graphql
   pathHistory(dataset: String!, path: String!): [PathVersion!]!
   ```

   where `PathVersion` contains:
   - `snapshot`: the snapshot digest;
   - `startTime` / `endTime` of that snapshot, so the client does not need a second query;
   - `change`: an enum of `ADDED`, `CHANGED`, `REMOVED`;
   - `entry`: the `TreeEntry` at that path, or `null` when removed;
   - `parent`: the digest of the tree containing the entry, or `null` when removed. This is the `tree` argument for `restoreFiles`.
1. Regenerate the SDL and client types:

   ```bash
   env GENERATE_SDL=public/schema.graphql cargo run
   bun run codegen
   ```

### Paths in URLs

Currently the directory the user is viewing in `TreeViewer` is held only in component state (`store.paths`); the URL is just `/snapshots/:id/browse/:sid`. Both the history page and the links back out of it need the path in the URL.

1. Add a route `/snapshots/:id/history/*path` in @client/index.tsx. Path components are individually URI-encoded, since entry names may contain characters that are not URL-safe.
1. Extend the browse route to accept an optional path, `/snapshots/:id/browse/:sid/*path`, and have `TreeViewer` seed `store.paths` from it by walking the trees from the root, reusing the existing re-walk logic that runs when the snapshot changes. Navigating within `TreeViewer` (descending, **Up**, breadcrumbs) updates the URL. This also makes browse locations bookmarkable and makes the browser Back button work within a tree.
1. Similarly extend the compare route to accept an optional starting path, `/snapshots/:id/compare/:digestA/:digestB/*path`, for the directory compare links on the history page.

### Client

1. Add the **History** button to `TreeViewer` using the `fa-clock-rotate-left` icon.
1. Change the **Restore** button icon in `TreeViewer` (@client/pages/snapshots.tsx) and in the snapshot compare view (@client/pages/diffs.tsx) from `fa-clock-rotate-left` to `fa-trash-arrow-up`, so that the two buttons are visually distinct. The navbar brand icon is unaffected.
1. Add the path history page in a new file, @client/pages/history.tsx, following the structure of @client/pages/diffs.tsx. It shows the full path as a heading, then a table with the columns: status icon, snapshot date, name (with entry type icon), modification time, reference, and the per-row action buttons (browse, restore, compare). The status and entry type icons are shared with the compare view.
