//
// Copyright (c) 2026 Nathan Fiedler
//
use crate::domain::entities::{
    Checksum, PathChange, PathVersion, Snapshot, TreeEntry, TreeReference,
};
use crate::domain::repositories::RecordRepository;
use anyhow::{Error, anyhow};
use std::cmp;
use std::fmt;

///
/// Retrieve the snapshots in which a particular path was added, changed, or
/// removed, newest first. Unfinished snapshots are ignored.
///
pub struct GetPathHistory {
    repo: Box<dyn RecordRepository>,
}

impl GetPathHistory {
    pub fn new(repo: Box<dyn RecordRepository>) -> Self {
        Self { repo }
    }

    /// Resolve the path within the given root tree. If `prev` is given and
    /// any tree along the way is identical to the one at the same depth in
    /// `prev`, the remainder is known to be the same and the lookup stops.
    fn resolve(
        &self,
        root: &Checksum,
        names: &[&str],
        prev: Option<&Resolved>,
    ) -> Result<Resolved, Error> {
        let mut chain: Vec<Checksum> = Vec::new();
        let mut cursor = root.clone();
        for (depth, name) in names.iter().enumerate() {
            if let Some(prev) = prev
                && prev.chain.get(depth) == Some(&cursor)
            {
                chain.extend_from_slice(&prev.chain[depth..]);
                return Ok(Resolved {
                    chain,
                    entry: prev.entry.clone(),
                    parent: prev.parent.clone(),
                });
            }
            chain.push(cursor.clone());
            let tree = self
                .repo
                .get_tree(&cursor)?
                .ok_or_else(|| anyhow!(format!("missing tree: {:?}", cursor)))?;
            let Some(entry) = tree.entries.into_iter().find(|e| e.name == *name) else {
                return Ok(Resolved::absent(chain));
            };
            if depth == names.len() - 1 {
                return Ok(Resolved {
                    chain,
                    entry: Some(entry),
                    parent: Some(cursor),
                });
            }
            match entry.reference {
                TreeReference::TREE(digest) => cursor = digest,
                // an intermediate component is not a directory
                _ => return Ok(Resolved::absent(chain)),
            }
        }
        // unreachable given a non-empty path
        Ok(Resolved::absent(chain))
    }
}

/// Outcome of looking up the path within a single snapshot.
struct Resolved {
    /// Digests of the trees visited, starting with the root tree.
    chain: Vec<Checksum>,
    /// Entry at the path, if it exists.
    entry: Option<TreeEntry>,
    /// Digest of the tree containing the entry, if it exists.
    parent: Option<Checksum>,
}

impl Resolved {
    fn absent(chain: Vec<Checksum>) -> Self {
        Self {
            chain,
            entry: None,
            parent: None,
        }
    }
}

impl super::UseCase<Vec<PathVersion>, Params> for GetPathHistory {
    fn call(&self, params: Params) -> Result<Vec<PathVersion>, Error> {
        let names: Vec<&str> = params.path.split('/').filter(|s| !s.is_empty()).collect();
        if names.is_empty() {
            return Err(anyhow!("path must name an entry"));
        }
        let dataset = self
            .repo
            .get_dataset(&params.dataset)?
            .ok_or_else(|| anyhow!(format!("missing dataset: {:?}", params.dataset)))?;
        // resolve the path in every finished snapshot, newest first
        let mut resolved: Vec<(Snapshot, Resolved)> = Vec::new();
        let mut maybe_digest = dataset.snapshot;
        while let Some(digest) = maybe_digest {
            let snapshot = self
                .repo
                .get_snapshot(&digest)?
                .ok_or_else(|| anyhow!(format!("missing snapshot: {:?}", digest)))?;
            maybe_digest = snapshot.parent.clone();
            if snapshot.end_time.is_none() {
                continue;
            }
            let prev = resolved.last().map(|(_, r)| r);
            let found = self.resolve(&snapshot.tree, &names, prev)?;
            resolved.push((snapshot, found));
        }
        // compare each snapshot with the next older one
        let mut versions: Vec<PathVersion> = Vec::new();
        for (index, (snapshot, current)) in resolved.iter().enumerate() {
            let older = resolved.get(index + 1).and_then(|(_, r)| r.entry.as_ref());
            let change = match (&current.entry, older) {
                (Some(_), None) => PathChange::Added,
                (None, Some(_)) => PathChange::Removed,
                (Some(a), Some(b)) if a.reference != b.reference => PathChange::Changed,
                _ => continue,
            };
            versions.push(PathVersion {
                snapshot: snapshot.clone(),
                change,
                entry: current.entry.clone(),
                parent: current.parent.clone(),
            });
        }
        Ok(versions)
    }
}

pub struct Params {
    /// Identifier of the dataset whose snapshots are examined.
    dataset: String,
    /// Slash-separated path of the entry relative to the dataset base path.
    path: String,
}

impl Params {
    pub fn new(dataset: String, path: String) -> Self {
        Self { dataset, path }
    }
}

impl fmt::Display for Params {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Params({}, {})", self.dataset, self.path)
    }
}

impl cmp::PartialEq for Params {
    fn eq(&self, other: &Self) -> bool {
        self.dataset == other.dataset && self.path == other.path
    }
}

impl cmp::Eq for Params {}

#[cfg(test)]
mod tests {
    use super::super::UseCase;
    use super::*;
    use crate::domain::entities::{Dataset, Tree};
    use crate::domain::repositories::MockRecordRepository;
    use chrono::{TimeZone, Utc};
    use std::collections::HashMap;
    use std::path::Path;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn file_entry(name: &str, content: &str) -> TreeEntry {
        let digest = Checksum::sha1_from_bytes(content.as_bytes());
        TreeEntry::new(Path::new(name), TreeReference::FILE(digest))
    }

    fn tree_entry(name: &str, tree: &Tree) -> TreeEntry {
        TreeEntry::new(Path::new(name), TreeReference::TREE(tree.digest.clone()))
    }

    /// Builds a chain of snapshots and a mock repository serving them.
    struct Fixture {
        trees: HashMap<Checksum, Tree>,
        snapshots: HashMap<Checksum, Snapshot>,
        head: Option<Checksum>,
        order: Vec<Checksum>,
    }

    impl Fixture {
        fn new() -> Self {
            Self {
                trees: HashMap::new(),
                snapshots: HashMap::new(),
                head: None,
                order: Vec::new(),
            }
        }

        fn tree(&mut self, entries: Vec<TreeEntry>) -> Tree {
            let tree = Tree::new(entries, 0);
            self.trees.insert(tree.digest.clone(), tree.clone());
            tree
        }

        /// Add a snapshot on top of the current head, in chronological order.
        fn snapshot(&mut self, root: &Tree, finished: bool) -> Checksum {
            let mut snapshot =
                Snapshot::new(self.head.clone(), root.digest.clone(), Default::default());
            // give each snapshot a distinct, increasing start time
            let start = Utc
                .with_ymd_and_hms(2026, 1, 1, 0, 0, 0)
                .unwrap()
                .checked_add_signed(chrono::TimeDelta::hours(self.order.len() as i64))
                .unwrap();
            snapshot.set_start_time(start);
            if finished {
                snapshot.set_end_time(start);
            }
            let digest = snapshot.digest.clone();
            self.snapshots.insert(digest.clone(), snapshot);
            self.head = Some(digest.clone());
            self.order.push(digest.clone());
            digest
        }

        fn run(self, path: &str) -> (Result<Vec<PathVersion>, Error>, usize) {
            let mut dataset = Dataset::new(Path::new("/home/planet"));
            dataset.snapshot = self.head.clone();
            let dataset_id = dataset.id.clone();
            let trees = self.trees;
            let snapshots = self.snapshots;
            let tree_calls = Arc::new(AtomicUsize::new(0));
            let counter = tree_calls.clone();
            let mut mock = MockRecordRepository::new();
            mock.expect_get_dataset()
                .returning(move |_| Ok(Some(dataset.clone())));
            mock.expect_get_snapshot()
                .returning(move |d| Ok(snapshots.get(d).cloned()));
            mock.expect_get_tree().returning(move |d| {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(trees.get(d).cloned())
            });
            let usecase = GetPathHistory::new(Box::new(mock));
            let result = usecase.call(Params::new(dataset_id, path.to_owned()));
            (result, tree_calls.load(Ordering::SeqCst))
        }
    }

    fn changes(versions: &[PathVersion]) -> Vec<PathChange> {
        versions.iter().map(|v| v.change).collect()
    }

    #[test]
    fn test_path_history_no_snapshots() {
        let fixture = Fixture::new();
        let (result, _) = fixture.run("a.txt");
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_path_history_empty_path() {
        let fixture = Fixture::new();
        let (result, _) = fixture.run("/");
        assert!(result.is_err());
    }

    #[test]
    fn test_path_history_unchanged() {
        let mut fixture = Fixture::new();
        let a = file_entry("a.txt", "one");
        let root1 = fixture.tree(vec![a.clone()]);
        let root2 = fixture.tree(vec![a, file_entry("b.txt", "two")]);
        let s1 = fixture.snapshot(&root1, true);
        fixture.snapshot(&root2, true);
        let (result, _) = fixture.run("a.txt");
        let versions = result.unwrap();
        assert_eq!(changes(&versions), vec![PathChange::Added]);
        assert_eq!(versions[0].snapshot.digest, s1);
        assert_eq!(versions[0].parent, Some(root1.digest.clone()));
    }

    #[test]
    fn test_path_history_changed_removed_readded() {
        let mut fixture = Fixture::new();
        let root1 = fixture.tree(vec![file_entry("a.txt", "one")]);
        let root2 = fixture.tree(vec![file_entry("a.txt", "two")]);
        let root3 = fixture.tree(vec![file_entry("b.txt", "other")]);
        let root4 = fixture.tree(vec![file_entry("a.txt", "three")]);
        let s1 = fixture.snapshot(&root1, true);
        let s2 = fixture.snapshot(&root2, true);
        let s3 = fixture.snapshot(&root3, true);
        let s4 = fixture.snapshot(&root4, true);
        let (result, _) = fixture.run("a.txt");
        let versions = result.unwrap();
        assert_eq!(
            changes(&versions),
            vec![
                PathChange::Added,
                PathChange::Removed,
                PathChange::Changed,
                PathChange::Added
            ]
        );
        let digests: Vec<Checksum> = versions.iter().map(|v| v.snapshot.digest.clone()).collect();
        assert_eq!(digests, vec![s4, s3, s2, s1]);
        assert!(versions[1].entry.is_none());
        assert!(versions[1].parent.is_none());
        assert_eq!(versions[0].parent, Some(root4.digest.clone()));
    }

    #[test]
    fn test_path_history_type_change() {
        let mut fixture = Fixture::new();
        let inner = fixture.tree(vec![file_entry("x.txt", "x")]);
        let root1 = fixture.tree(vec![file_entry("a", "file")]);
        let root2 = fixture.tree(vec![tree_entry("a", &inner)]);
        fixture.snapshot(&root1, true);
        fixture.snapshot(&root2, true);
        let (result, _) = fixture.run("a");
        let versions = result.unwrap();
        assert_eq!(
            changes(&versions),
            vec![PathChange::Changed, PathChange::Added]
        );
        assert!(versions[0].entry.as_ref().unwrap().reference.is_tree());
    }

    #[test]
    fn test_path_history_nested_and_missing_intermediate() {
        let mut fixture = Fixture::new();
        let dir1 = fixture.tree(vec![file_entry("c.txt", "one")]);
        let dir2 = fixture.tree(vec![file_entry("c.txt", "two")]);
        let root1 = fixture.tree(vec![tree_entry("b", &dir1)]);
        // intermediate directory missing entirely
        let root2 = fixture.tree(vec![file_entry("z.txt", "z")]);
        // intermediate component is a file rather than a directory
        let root3 = fixture.tree(vec![file_entry("b", "not a dir")]);
        let root4 = fixture.tree(vec![tree_entry("b", &dir2)]);
        fixture.snapshot(&root1, true);
        fixture.snapshot(&root2, true);
        fixture.snapshot(&root3, true);
        fixture.snapshot(&root4, true);
        let (result, _) = fixture.run("/b/c.txt");
        let versions = result.unwrap();
        // root3 is also absent, same as root2, so it is elided
        assert_eq!(
            changes(&versions),
            vec![PathChange::Added, PathChange::Removed, PathChange::Added]
        );
        assert_eq!(versions[0].parent, Some(dir2.digest.clone()));
    }

    #[test]
    fn test_path_history_skips_unfinished() {
        let mut fixture = Fixture::new();
        let root1 = fixture.tree(vec![file_entry("a.txt", "one")]);
        let root2 = fixture.tree(vec![file_entry("a.txt", "two")]);
        fixture.snapshot(&root1, true);
        fixture.snapshot(&root2, false);
        let (result, _) = fixture.run("a.txt");
        assert_eq!(changes(&result.unwrap()), vec![PathChange::Added]);
    }

    #[test]
    fn test_path_history_short_circuit() {
        let mut fixture = Fixture::new();
        let dir = fixture.tree(vec![file_entry("c.txt", "one")]);
        let root1 = fixture.tree(vec![tree_entry("b", &dir), file_entry("x", "1")]);
        let root2 = fixture.tree(vec![tree_entry("b", &dir), file_entry("x", "2")]);
        let root3 = fixture.tree(vec![tree_entry("b", &dir), file_entry("x", "3")]);
        fixture.snapshot(&root1, true);
        fixture.snapshot(&root2, true);
        fixture.snapshot(&root3, true);
        let (result, tree_calls) = fixture.run("b/c.txt");
        assert_eq!(changes(&result.unwrap()), vec![PathChange::Added]);
        // newest snapshot: root3 and dir; the older two differ at the root
        // but share "b", so each loads only its root tree
        assert_eq!(tree_calls, 4);
    }

    #[test]
    fn test_path_history_identical_roots() {
        let mut fixture = Fixture::new();
        let dir = fixture.tree(vec![file_entry("c.txt", "one")]);
        let root = fixture.tree(vec![tree_entry("b", &dir)]);
        fixture.snapshot(&root, true);
        fixture.snapshot(&root, true);
        fixture.snapshot(&root, true);
        let (result, tree_calls) = fixture.run("b/c.txt");
        assert_eq!(changes(&result.unwrap()), vec![PathChange::Added]);
        // only the newest snapshot requires any tree lookups
        assert_eq!(tree_calls, 2);
    }

    #[test]
    fn test_path_history_missing_tree() {
        let mut fixture = Fixture::new();
        let root = Tree::new(vec![file_entry("a.txt", "one")], 0);
        // tree deliberately not registered with the fixture
        fixture.snapshot(&root, true);
        let (result, _) = fixture.run("a.txt");
        assert!(result.is_err());
    }
}
