//
// Copyright (c) 2026 Nathan Fiedler
//
use crate::domain::entities::{Checksum, PathMatch, PathSearch, TreeReference};
use crate::domain::repositories::RecordRepository;
use anyhow::{Error, anyhow};
use globset::{GlobBuilder, GlobMatcher};
use std::cmp;
use std::collections::{HashMap, HashSet};
use std::fmt;

///
/// Search every finished snapshot of a dataset for entries whose name (or
/// path, if the pattern contains a slash) matches a wildcard pattern. Each
/// matching path is reported once, as found in the newest snapshot that
/// contains it.
///
pub struct SearchSnapshots {
    repo: Box<dyn RecordRepository>,
}

impl SearchSnapshots {
    pub fn new(repo: Box<dyn RecordRepository>) -> Self {
        Self { repo }
    }
}

/// Compiled form of the search pattern.
struct Matcher {
    glob: GlobMatcher,
    /// Match against the full relative path rather than the entry name.
    by_path: bool,
}

impl Matcher {
    fn new(pattern: &str) -> Result<Self, Error> {
        if pattern.trim().is_empty() {
            return Err(anyhow!("pattern must not be empty"));
        }
        let by_path = pattern.contains('/');
        let trimmed = pattern.trim_start_matches('/');
        if trimmed.is_empty() {
            return Err(anyhow!("pattern must name an entry"));
        }
        let glob = GlobBuilder::new(trimmed)
            .case_insensitive(true)
            .literal_separator(true)
            .build()?
            .compile_matcher();
        Ok(Self { glob, by_path })
    }

    fn is_match(&self, name: &str, path: &str) -> bool {
        if self.by_path {
            self.glob.is_match(path)
        } else {
            self.glob.is_match(name)
        }
    }
}

impl super::UseCase<PathSearch, Params> for SearchSnapshots {
    fn call(&self, params: Params) -> Result<PathSearch, Error> {
        let matcher = Matcher::new(&params.pattern)?;
        let dataset = self
            .repo
            .get_dataset(&params.dataset)?
            .ok_or_else(|| anyhow!(format!("missing dataset: {:?}", params.dataset)))?;
        let mut results: HashMap<String, PathMatch> = HashMap::new();
        // directories already walked at a given location, whose contents
        // were necessarily recorded with a newer (or the same) snapshot
        let mut visited: HashSet<(String, Checksum)> = HashSet::new();
        let mut truncated = false;
        let mut latest = true;
        let mut maybe_digest = dataset.snapshot;
        'snapshots: while let Some(digest) = maybe_digest {
            let snapshot = self
                .repo
                .get_snapshot(&digest)?
                .ok_or_else(|| anyhow!(format!("missing snapshot: {:?}", digest)))?;
            maybe_digest = snapshot.parent.clone();
            if snapshot.end_time.is_none() {
                continue;
            }
            let mut pending: Vec<(String, Checksum)> = vec![(String::new(), snapshot.tree.clone())];
            while let Some((dirpath, tree_digest)) = pending.pop() {
                let key = (dirpath, tree_digest);
                if visited.contains(&key) {
                    continue;
                }
                let tree = self
                    .repo
                    .get_tree(&key.1)?
                    .ok_or_else(|| anyhow!(format!("missing tree: {:?}", key.1)))?;
                for entry in tree.entries.into_iter() {
                    let path = if key.0.is_empty() {
                        entry.name.clone()
                    } else {
                        format!("{}/{}", key.0, entry.name)
                    };
                    if let TreeReference::TREE(ref child) = entry.reference {
                        pending.push((path.clone(), child.clone()));
                    }
                    if matcher.is_match(&entry.name, &path) && !results.contains_key(&path) {
                        if results.len() >= params.limit {
                            truncated = true;
                            break 'snapshots;
                        }
                        results.insert(
                            path.clone(),
                            PathMatch {
                                path,
                                snapshot: snapshot.clone(),
                                entry,
                                parent: key.1.clone(),
                                current: latest,
                            },
                        );
                    }
                }
                visited.insert(key);
            }
            latest = false;
        }
        let mut matches: Vec<PathMatch> = results.into_values().collect();
        matches.sort_by(|a, b| a.path.cmp(&b.path));
        Ok(PathSearch { matches, truncated })
    }
}

pub struct Params {
    /// Identifier of the dataset whose snapshots are searched.
    dataset: String,
    /// Wildcard pattern to match against entry names or paths.
    pattern: String,
    /// Maximum number of matching paths to return.
    limit: usize,
}

impl Params {
    pub fn new(dataset: String, pattern: String, limit: usize) -> Self {
        Self {
            dataset,
            pattern,
            limit,
        }
    }
}

impl fmt::Display for Params {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Params({}, {}, {})",
            self.dataset, self.pattern, self.limit
        )
    }
}

impl cmp::PartialEq for Params {
    fn eq(&self, other: &Self) -> bool {
        self.dataset == other.dataset && self.pattern == other.pattern && self.limit == other.limit
    }
}

impl cmp::Eq for Params {}

#[cfg(test)]
mod tests {
    use super::super::UseCase;
    use super::*;
    use crate::domain::entities::{Dataset, Snapshot, Tree, TreeEntry};
    use crate::domain::repositories::MockRecordRepository;
    use chrono::{TimeZone, Utc};
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
    #[derive(Clone)]
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

        fn run(self, pattern: &str, limit: usize) -> (Result<PathSearch, Error>, usize) {
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
            let usecase = SearchSnapshots::new(Box::new(mock));
            let result = usecase.call(Params::new(dataset_id, pattern.to_owned(), limit));
            (result, tree_calls.load(Ordering::SeqCst))
        }
    }

    fn paths(search: &PathSearch) -> Vec<&str> {
        search.matches.iter().map(|m| m.path.as_str()).collect()
    }

    #[test]
    fn test_search_no_snapshots() {
        let fixture = Fixture::new();
        let (result, _) = fixture.run("*.avi", 100);
        let search = result.unwrap();
        assert!(search.matches.is_empty());
        assert!(!search.truncated);
    }

    #[test]
    fn test_search_invalid_pattern() {
        for pattern in ["", "   ", "/", "a[bc"] {
            let fixture = Fixture::new();
            let (result, _) = fixture.run(pattern, 100);
            assert!(result.is_err(), "pattern {:?} should be rejected", pattern);
        }
    }

    #[test]
    fn test_search_name_at_any_depth() {
        let mut fixture = Fixture::new();
        let deep = fixture.tree(vec![file_entry("c.avi", "c"), file_entry("c.txt", "t")]);
        let dir = fixture.tree(vec![file_entry("b.avi", "b"), tree_entry("deep", &deep)]);
        let root = fixture.tree(vec![file_entry("a.avi", "a"), tree_entry("dir", &dir)]);
        let s1 = fixture.snapshot(&root, true);
        let (result, _) = fixture.run("*.avi", 100);
        let search = result.unwrap();
        assert_eq!(paths(&search), vec!["a.avi", "dir/b.avi", "dir/deep/c.avi"]);
        assert!(search.matches.iter().all(|m| m.current));
        assert!(search.matches.iter().all(|m| m.snapshot.digest == s1));
        assert_eq!(search.matches[2].parent, deep.digest);
        assert_eq!(search.matches[1].parent, dir.digest);
    }

    #[test]
    fn test_search_matches_directories() {
        let mut fixture = Fixture::new();
        let photos = fixture.tree(vec![file_entry("x.jpg", "x")]);
        let root = fixture.tree(vec![tree_entry("Photos", &photos)]);
        fixture.snapshot(&root, true);
        let (result, _) = fixture.run("photos", 100);
        let search = result.unwrap();
        assert_eq!(paths(&search), vec!["Photos"]);
        assert!(search.matches[0].entry.reference.is_tree());
    }

    #[test]
    fn test_search_case_insensitive() {
        let mut fixture = Fixture::new();
        let root = fixture.tree(vec![file_entry("MOVIE.AVI", "m"), file_entry("x.txt", "x")]);
        fixture.snapshot(&root, true);
        let (result, _) = fixture.run("*.avi", 100);
        assert_eq!(paths(&result.unwrap()), vec!["MOVIE.AVI"]);
    }

    #[test]
    fn test_search_path_mode() {
        let mut fixture = Fixture::new();
        let deep = fixture.tree(vec![file_entry("c.jpg", "c")]);
        let year = fixture.tree(vec![file_entry("b.jpg", "b"), tree_entry("deep", &deep)]);
        let photos = fixture.tree(vec![tree_entry("2019", &year), file_entry("a.jpg", "a")]);
        let root = fixture.tree(vec![
            tree_entry("photos", &photos),
            file_entry("r.jpg", "r"),
        ]);
        fixture.snapshot(&root, true);
        // single star does not cross a slash
        let (result, _) = fixture.clone().run("photos/*.jpg", 100);
        assert_eq!(paths(&result.unwrap()), vec!["photos/a.jpg"]);
        let (result, _) = fixture.clone().run("photos/2019/*.jpg", 100);
        assert_eq!(paths(&result.unwrap()), vec!["photos/2019/b.jpg"]);
        // double star crosses any number of directories
        let (result, _) = fixture.clone().run("photos/**/*.jpg", 100);
        assert_eq!(
            paths(&result.unwrap()),
            vec![
                "photos/2019/b.jpg",
                "photos/2019/deep/c.jpg",
                "photos/a.jpg"
            ]
        );
        // leading slash anchors at the root
        let (result, _) = fixture.run("/*.jpg", 100);
        assert_eq!(paths(&result.unwrap()), vec!["r.jpg"]);
    }

    #[test]
    fn test_search_older_snapshots() {
        let mut fixture = Fixture::new();
        let root1 = fixture.tree(vec![file_entry("gone.avi", "g"), file_entry("a.avi", "1")]);
        let root2 = fixture.tree(vec![file_entry("gone.avi", "g"), file_entry("a.avi", "2")]);
        let root3 = fixture.tree(vec![file_entry("a.avi", "3")]);
        fixture.snapshot(&root1, true);
        let s2 = fixture.snapshot(&root2, true);
        let s3 = fixture.snapshot(&root3, true);
        let (result, _) = fixture.run("*.avi", 100);
        let search = result.unwrap();
        assert_eq!(paths(&search), vec!["a.avi", "gone.avi"]);
        // changed file reports only the newest version
        assert_eq!(search.matches[0].snapshot.digest, s3);
        assert!(search.matches[0].current);
        assert_eq!(
            search.matches[0].entry.reference,
            file_entry("a.avi", "3").reference
        );
        // removed file reports the newest snapshot that contains it
        assert_eq!(search.matches[1].snapshot.digest, s2);
        assert!(!search.matches[1].current);
        assert_eq!(search.matches[1].parent, root2.digest);
    }

    #[test]
    fn test_search_skips_unchanged_trees() {
        let mut fixture = Fixture::new();
        let dir = fixture.tree(vec![file_entry("c.txt", "one")]);
        let root1 = fixture.tree(vec![tree_entry("b", &dir), file_entry("x", "1")]);
        let root2 = fixture.tree(vec![tree_entry("b", &dir), file_entry("x", "2")]);
        fixture.snapshot(&root1, true);
        fixture.snapshot(&root2, true);
        fixture.snapshot(&root2, true);
        let (result, tree_calls) = fixture.run("c.txt", 100);
        assert_eq!(paths(&result.unwrap()), vec!["b/c.txt"]);
        // newest: root2 and dir; middle: root2 again is skipped; oldest:
        // root1 only, since "b" is unchanged at the same location
        assert_eq!(tree_calls, 3);
    }

    #[test]
    fn test_search_same_tree_two_locations() {
        let mut fixture = Fixture::new();
        let dir = fixture.tree(vec![file_entry("c.txt", "one")]);
        let root = fixture.tree(vec![tree_entry("a", &dir), tree_entry("b", &dir)]);
        fixture.snapshot(&root, true);
        let (result, _) = fixture.run("c.txt", 100);
        assert_eq!(paths(&result.unwrap()), vec!["a/c.txt", "b/c.txt"]);
    }

    #[test]
    fn test_search_skips_unfinished() {
        let mut fixture = Fixture::new();
        let root1 = fixture.tree(vec![file_entry("a.txt", "one")]);
        let root2 = fixture.tree(vec![file_entry("b.txt", "two")]);
        let s1 = fixture.snapshot(&root1, true);
        fixture.snapshot(&root2, false);
        let (result, _) = fixture.run("*.txt", 100);
        let search = result.unwrap();
        assert_eq!(paths(&search), vec!["a.txt"]);
        assert_eq!(search.matches[0].snapshot.digest, s1);
        // the latest finished snapshot is the one that counts as current
        assert!(search.matches[0].current);
    }

    #[test]
    fn test_search_truncated() {
        let mut fixture = Fixture::new();
        let root = fixture.tree(vec![
            file_entry("a.txt", "a"),
            file_entry("b.txt", "b"),
            file_entry("c.txt", "c"),
        ]);
        fixture.snapshot(&root, true);
        let (result, _) = fixture.clone().run("*.txt", 2);
        let search = result.unwrap();
        assert_eq!(search.matches.len(), 2);
        assert!(search.truncated);
        // exactly at the limit is not truncated
        let (result, _) = fixture.run("*.txt", 3);
        let search = result.unwrap();
        assert_eq!(search.matches.len(), 3);
        assert!(!search.truncated);
    }

    #[test]
    fn test_search_missing_tree() {
        let mut fixture = Fixture::new();
        let root = Tree::new(vec![file_entry("a.txt", "one")], 0);
        // tree deliberately not registered with the fixture
        fixture.snapshot(&root, true);
        let (result, _) = fixture.run("a.txt", 100);
        assert!(result.is_err());
    }
}
