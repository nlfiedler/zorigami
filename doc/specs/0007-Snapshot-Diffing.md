# Snapshot Diffing

Viewing differences between two snapshots is not currently possible with the web interface. The objective of this change is to make it easy for the user to see what files and directories were added, removed, or changed between any two snapshots.

## Functional Requirements

From the snapshots page, in particular the `Snapshots` component in @client/pages/snapshots.tsx, the user will select two snapshots then click on a new "Compare" button. The "Compare" button will change the view from the list of snapshots to a detailed view of the root tree of the snapshots, clearly showing what was added, removed, or changed. The user will be able to navigate through the directory structure by clicking on rows that represent a tree that was either added or changed.

## Design Details

The `Snapshots` component will need to introduce checkboxes to each row in the table that lists the snapshots. The user may select up to two checkboxes, at which point a new button named "Compare" will become enabled. Once two checkboxes are selected, the other checkboxes become disabled. As long as two checkboxes are selected, the "Compare" button will be enabled. The "Compare" button will be in a new `nav` "level" above the snapshots table.

Clicking on the "Compare" button will navigate to a new route (`/snapshots/:id/compare/:digestA/:digestB` seems good) that maps to a new tree-diff-viewer component. The tree-diff-viewer will be provided with the snapshot digests, A and B, where A is the snapshot with an earlier start time and B is the snapshot with a later start time. The tree-diff-viewer will then use those snapshot digests to get the root tree for each snapshot, and then build out the tree-diff-viewer table as follows.

Note: an entry in A is matched with an entry in B by the entry `name` field.

1. For every entry in A and B for which their `reference` is different, indicate that the entry was "changed".
1. For every entry in B that is not in A, indicate that this entry was "added".
1. For every entry in A that is not in B, indicate that this entry was "removed".
1. Entries that have not changed in any way should be elided, they are not interesting.
1. The compare-by-`reference` rule applies uniformly to all reference kinds (`file-`, `tree-`, `link-`, `small-`); non-tree entries are diffed as leaves with no drill-in.
1. If the entry was "changed" and both A and B represent trees (`reference` starts with `tree-`), then the user can navigate to those trees. Navigating fetches the tree from each of A and B for the matching entries and updates the tree-diff-viewer with this new pair of trees.
1. If the entry was "changed" and the type of the entries is different, and either one is a tree, then navigation is not enabled for that entry.
1. If the entry was "added" and is a tree, the user can navigate into it; only the B side exists at that level (and any nested levels), so the A side is treated as empty and every entry inside is shown as "added".
1. If the entry was "removed" and is a tree, the user can navigate into it; only the A side exists at that level (and any nested levels), so the B side is treated as empty and every entry inside is shown as "removed".
1. Add a breadcrumb trail above the table, like in `TreeViewer` component in @client/pages/snapshots.tsx, by which the user can navigate back to parent trees.
1. Add an "Up" button that navigates to the parent tree pair, like in `TreeViewer`.
1. If, at the current level, every entry in A and B has been elided (i.e. there are no added, removed, or changed entries to show), display a "no differences at this level" message in place of an empty table.

The tree-diff-viewer also supports restoring entries as an "undo" of changes between A and B:

1. Each row whose status is "changed" or "removed" has a checkbox that allows it to be selected for restore. Rows whose status is "added" are not selectable for restore (there is no A-side version to restore).
1. A "Restore" button (like the one in `TreeViewer`) is enabled when at least one selectable row is checked. Clicking it restores the A-side version of each selected entry, using the same `restoreFiles` mutation that `TreeViewer` uses.

Indicating that an entry was "added" should use a green color in some fashion, "removed" should use red, and "changed" would be blue. The intended styling is a row-level background tint (`has-background-success-light` / `has-background-danger-light` / `has-background-info-light`) plus a leading status icon column (e.g. fa icons for added/removed/changed), with the existing Name/Date/Reference columns retained. This is a starting point and may need revision to find the best presentation.
