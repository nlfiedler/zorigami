# Snapshot Search

Finding a particular file or directory in the backups is currently only possible by browsing snapshots one directory at a time, which is impractical when the entry is not in the latest snapshot, or when the user does not remember where it was. The objective of this change is to let the user search every snapshot of a dataset for entries whose name (or path) matches a simple wildcard pattern, such as `*.avi`, and then browse, restore, or view the history of any match.

## Functional Requirements

1. In the `Snapshots` component in @client/pages/snapshots.tsx, add a **Search** button to the toolbar, immediately to the right of the **Compare** button. It is always enabled (when the dataset has snapshots) and navigates to the search page for that dataset.
1. The search page has a text field for the pattern and a **Search** button; pressing Enter in the field also submits. An empty pattern disables the button.
1. The search covers every finished snapshot (one with an `endTime`) of the dataset. Unfinished snapshots are excluded, as with path history (spec 0012).
1. Pattern syntax:
   - `*` matches any sequence of characters, `?` matches any single character, and `[abc]` matches a character class. These are provided by the `globset` crate, which the backup exclusions already use; only `*` needs to be documented in the page help text.
   - If the pattern contains no `/`, it is matched against the entry _name_ only, at any depth. `*.avi` finds every `.avi` file anywhere in the dataset.
   - If the pattern contains a `/`, it is matched against the full path of the entry, relative to the dataset base path. A leading `/` is ignored. In this mode `*` does not cross a `/`, while `**` matches any number of directories, e.g. `photos/2019/*.jpg` or `photos/**/*.jpg`.
   - Matching is case-insensitive, so `*.avi` also matches `MOVIE.AVI`.
   - A pattern without wildcards is an exact (case-insensitive) name or path match.
   - An invalid pattern (e.g. an unclosed `[`) produces an error message on the page, not an empty result.
1. Files, directories, symbolic links, and small (inline) files are all eligible to match.
1. Each distinct matching path appears in the results exactly once, however many snapshots it appears in. The row describes the entry as found in the _newest_ snapshot that contains the path.
1. The results are sorted by path, ascending.
1. Each row shows: the entry type icon (as in the compare and history views), the full path, the date of the newest snapshot containing it, the entry modification time, and a tag that reads **current** if the path exists in the latest snapshot, or **deleted** if it does not.
1. Each row offers:
   - **Browse**: opens that newest snapshot at the directory containing the entry;
   - **Restore**: restores that version of the entry, using the existing `restoreFiles` mutation, then navigates to the restore page as `TreeViewer` does;
   - **History**: opens the path history page (spec 0012) for that path.
1. The number of results is capped (see below). When the cap is reached, the page shows a notice above the table saying that the results are incomplete and the pattern should be narrowed.
1. If nothing matches, show a "no entries match this pattern" message in place of an empty table.
1. The pattern is kept in the URL query string, so that a search can be bookmarked, and so that returning from Browse or History with the browser Back button shows the same results.
1. Searching may take a noticeable amount of time on a large dataset, so the page shows a progress indicator while the query is running, and the **Search** button is disabled until it completes.

## Design Details

### Search Algorithm

Walking every tree of every snapshot naively would take time proportional to the number of snapshots times the size of the dataset. Because trees are content-addressed, most of that work is redundant: a directory whose tree digest has not changed between snapshots contains exactly the same entries.

1. Walk the snapshot chain from the dataset head via `parent`, newest first, as `GetPathHistory` does, skipping unfinished snapshots. The first finished snapshot is the _latest_ one.
1. For each snapshot, walk its trees depth-first from the root tree, tracking the path of each directory.
1. Maintain a set of visited `(directory path, tree digest)` pairs. On reaching a directory whose pair has already been visited, skip it and everything beneath it. This is safe because the snapshots are walked newest first: every path beneath that directory was already recorded, with a newer (or the same) snapshot. The path is part of the key because the same tree may appear at more than one location (e.g. a moved directory, or two empty directories), and each location produces different paths.
   - In the common case, an unchanged root tree means the entire snapshot is skipped after a single comparison, and a snapshot that changed a few files only visits the directories along the paths to those files.
1. For each entry in a visited tree, build its path and test it against the pattern. If it matches and the path is not already in the results, record it with the current snapshot. Directories are descended whether or not they themselves match.
1. Maintain the results in a map keyed by path, so that the "already recorded" test is cheap. Since snapshots are processed newest first, the first time a path is recorded is always its newest occurrence.
1. A path "exists in the latest snapshot" if and only if it was recorded while walking the latest snapshot.
1. Once the number of results reaches the limit, stop walking and report that the results were truncated. Because the walk is newest first, the truncated results favor entries in recent snapshots, which is the most likely thing the user wants.
1. A tree referenced by a snapshot but missing from the database is an error, as it is in `GetPathHistory`.

Memory use is bounded by the number of distinct directory versions (the visited set) plus the result limit, which is acceptable.

### Server

1. Add `PathMatch` and `PathSearch` entities to `server/src/domain/entities.rs`, alongside `PathVersion`:
   - `PathMatch`
     - `path`: slash-separated path relative to the dataset base path;
     - `snapshot`: the newest `Snapshot` containing the path;
     - `entry`: the `TreeEntry` at that path in that snapshot;
     - `parent`: digest of the tree containing the entry (the `tree` argument for `restoreFiles`);
     - `current`: `true` if the path exists in the latest finished snapshot.
   - `PathSearch`
     - `matches`: `Vec<PathMatch>`, sorted by path;
     - `truncated`: `true` if the walk stopped at the limit.
1. Add a use case `server/src/domain/usecases/search_snapshots.rs` (registered in `usecases.rs`) whose `Params` holds the dataset identifier, the pattern, and the limit.
   - Build the matcher with `globset::GlobBuilder::new(pattern).case_insensitive(true).literal_separator(true)`. Trim a leading `/` first, and decide between name and path mode based on whether the pattern contains `/`.
   - An empty (or all-whitespace) pattern and a glob syntax error are both errors.
   - Implement the walk described above.
   - Unit tests use `MockRecordRepository`, following the `Fixture` approach in `get_path_history.rs`, covering: a name match at several depths; a path-mode match, including `*` not crossing `/` and `**` crossing it; case insensitivity; a path found only in an older snapshot (`current` is `false`, and the snapshot is the newest one containing it); a path that changed between snapshots (only the newest version is reported); a skipped unchanged snapshot (assert the number of `get_tree` calls, as the path history short-circuit test does); the same tree at two locations yields both paths; unfinished snapshots are ignored; truncation at the limit; and an invalid pattern.
1. In `server/src/preso/graphql.rs`, add a query:

   ```graphql
   searchSnapshots(dataset: String!, pattern: String!, limit: Int): PathSearch!
   ```

   The `limit` defaults to 500 and is clamped to the range 1-5000. `PathMatch` exposes `path`, `snapshot` (the `Snapshot` object, from which the client selects `checksum` and `startTime`), `entry`, `parent`, and `current`, mirroring `PathVersion`.
1. Regenerate the SDL and client types:

   ```bash
   env GENERATE_SDL=public/schema.graphql cargo run
   bun run codegen
   ```

### Client

1. Add a route `/snapshots/:id/search` in @client/index.tsx, mapped to a new `SnapshotSearch` component in a new file, @client/pages/search.tsx. The pattern is read from, and written to, the `pattern` query parameter (via `useSearchParams`); submitting the form updates the query parameter, and the query runs whenever the parameter is non-empty.
1. Add the **Search** button to the `Snapshots` toolbar using the `fa-magnifying-glass` icon.
1. The results table follows the structure of @client/pages/history.tsx: the entry type icon, path, snapshot date, modification time, the **current** / **deleted** tag (`is-success` / `is-light` tags respectively), and the per-row action buttons. Reuse `referenceIconClass` from @client/pages/diffs.tsx and `encodePath` from @client/paths.ts rather than duplicating them.
1. Browse navigates to `/snapshots/:id/browse/:sid/<parent directory>`, and History navigates to `/snapshots/:id/history/<path>`, both with each path component URI-encoded via `encodePath`.
1. Restore calls `restoreFiles` with `tree: parent`, `entry: entry.name`, `filepath: path`, and `dataset: id`, exactly as the history page does, and navigates to `/restore` on success.
1. Use `fetchPolicy: 'network-only'` for the search query, so that a new snapshot is reflected in repeated searches.

## Out of Scope

- Searching across all datasets at once.
- Searching by attributes other than the name or path (size, dates, content).
- Regular expressions.
- Streaming or paginating results; the result cap and the truncation notice take the place of pagination.

Once implemented, remove the "search snapshots" entry from @TODO.org.
