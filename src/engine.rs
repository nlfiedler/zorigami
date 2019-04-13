//
// Copyright (c) 2019 Nathan Fiedler
//
use super::core;
use super::database::Database;
use base64::encode;
use failure::{err_msg, Error};
use std::collections::VecDeque;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use xattr;

///
/// Take a snapshot of the directory structure at the given path. The parent, if
/// `Some`, specifies the snapshot that will be recorded as the parent of this
/// new snapshot.
///
pub fn take_snapshot(
    basepath: &Path,
    parent: Option<core::Checksum>,
    dbase: &Database,
) -> Result<core::Checksum, Error> {
    let start_time = SystemTime::now();
    let tree = scan_tree(basepath, dbase)?;
    let mut snap = core::Snapshot::new(parent, tree.checksum());
    snap = snap.start_time(start_time);
    snap = snap.file_count(tree.file_count);
    let sha1 = snap.checksum();
    dbase.insert_snapshot(&sha1, &snap)?;
    Ok(sha1)
}

///
/// Output from the `find_changed_files()` function.
///
pub struct ChangedFile {
    /// Relative file path of the changed file.
    pub path: PathBuf,
    /// Hash digest of the changed file.
    pub digest: core::Checksum,
}

impl ChangedFile {
    fn new(path: &Path, digest: core::Checksum) -> Self {
        Self {
            path: PathBuf::from(path),
            digest,
        }
    }
}

///
/// Created by calling `find_changed_files()` with the checksum for
/// two snapshots, one earlier and the other later.
///
pub struct ChangedFilesIter {
    /// Queue of pending paths to visit, where the path is relative, the first
    /// checksum is the left tree (earlier), and the second is the right tree
    /// (later).
    queue: Vec<(PathBuf, core::Tree, core::Tree)>,
    /// Position within left tree currently being iterated.
    left_idx: usize,
    /// Position within right tree currently being iterated.
    right_idx: usize,
}

impl ChangedFilesIter {
    fn new(left_tree: core::Tree, right_tree: core::Tree) -> Self {
        let queue = vec![(PathBuf::from("."), left_tree, right_tree)];
        Self {
            queue,
            left_idx: 0,
            right_idx: 0,
        }
    }
}

impl Iterator for ChangedFilesIter {
    type Item = Result<ChangedFile, Error>;

    fn next(&mut self) -> Option<Result<ChangedFile, Error>> {
        // TODO: how to do the yield* in Rust?
        //       breadth-first nested iterator (TreeWalker)
        // TODO: write the TreeWalker iterator first
        //       convert addAllFilesUnder() to TreeWalker, uses queue for breadth-first traversal
        // TODO: write unit tests for the tree walker
        // TODO: convert the rest of findChangedFiles()
        None
    }
}

///
/// Returns an `Iterator` that yields `ChangedFile` for files that were added or
/// changed between the two snapshots. Only files are considered, as changes to
/// directories are already recorded in the database and saved separately.
/// Ignores anything that is not a file or a directory. May return files that
/// were processed earlier, so the caller must filter out files that have record
/// entries in the database.
///
pub fn find_changed_files(
    dbase: &Database,
    snapshot1: core::Checksum,
    snapshot2: core::Checksum,
) -> Result<ChangedFilesIter, Error> {
    let snap1doc = dbase
        .get_snapshot(&snapshot1)?
        .ok_or_else(|| err_msg(format!("missing snapshot: {:?}", snapshot1)))?;
    let tree1doc = dbase
        .get_tree(&snap1doc.tree)?
        .ok_or_else(|| err_msg(format!("missing tree: {:?}", snap1doc.tree)))?;
    let snap2doc = dbase
        .get_snapshot(&snapshot2)?
        .ok_or_else(|| err_msg(format!("missing snapshot: {:?}", snapshot2)))?;
    let tree2doc = dbase
        .get_tree(&snap2doc.tree)?
        .ok_or_else(|| err_msg(format!("missing tree: {:?}", snap2doc.tree)))?;
    Ok(ChangedFilesIter::new(tree1doc, tree2doc))
}

// async function* addAllFilesUnder(basepath: string, ref: string): AsyncIterableIterator<[string, string]> {
//   const tree = await database.getTree(ref)
//   const entries: TreeEntry[] = tree.entries
//   for (let entry of entries) {
//     if (modeToType(entry.mode) === FileType.DIR) {
//       yield* addAllFilesUnder(path.join(basepath, entry.name), entry.reference)
//     } else if (modeToType(entry.mode) === FileType.REG) {
//       yield [path.join(basepath, entry.name), entry.reference]
//     }
//   }
// }
pub struct TreeWalker<'a> {
    /// Reference to Database for fetching records.
    dbase: &'a Database,
    /// Queue of pending paths to visit, where the path is relative, the
    /// checksum is the tree to be visited.
    queue: VecDeque<(PathBuf, core::Checksum)>,
    /// Current path being visited.
    path: Option<PathBuf>,
    /// Tree currently being visited.
    tree: Option<core::Tree>,
    /// Position within tree currently being iterated.
    entry_idx: usize,
}

impl<'a> TreeWalker<'a> {
    pub fn new(dbase: &'a Database, basepath: &str, tree: core::Checksum) -> Self {
        let mut queue = VecDeque::new();
        queue.push_back((PathBuf::from(basepath), tree));
        Self {
            dbase,
            queue,
            path: None,
            tree: None,
            entry_idx: 0,
        }
    }
}

impl<'a> Iterator for TreeWalker<'a> {
    type Item = Result<ChangedFile, Error>;

    fn next(&mut self) -> Option<Result<ChangedFile, Error>> {
        // loop until we produce a result for the caller
        loop {
            // if we have a tree and are not done with it, iterate on it
            if let Some(tree) = self.tree.as_ref() {
                while self.entry_idx < tree.entries.len() {
                    let base = self.path.as_ref().unwrap();
                    let entry = &tree.entries[self.entry_idx];
                    self.entry_idx += 1;
                    if entry.reference.is_tree() {
                        // enqueue the tree
                        let sum = entry.reference.checksum().unwrap();
                        let mut path = PathBuf::from(base);
                        path.push(&entry.name);
                        self.queue.push_back((path, sum));
                    } else if entry.reference.is_file() {
                        // return the file
                        let sum = entry.reference.checksum().unwrap();
                        let mut path = PathBuf::from(base);
                        path.push(&entry.name);
                        let changed = ChangedFile::new(&path, sum);
                        return Some(Ok(changed));
                    }
                }
            }
            // the tree is done, check the queue for more
            if let Some((base, sum)) = self.queue.pop_front() {
                // dequeue the next entry, fetch the tree
                let result = self.dbase.get_tree(&sum);
                if result.is_err() {
                    return Some(Err(err_msg(format!("failed to get tree: {:?}", sum))));
                }
                let opt = result.unwrap();
                if opt.is_none() {
                    return Some(Err(err_msg(format!("missing tree: {:?}", sum))));
                }
                // update the tree, index, and path fields
                self.tree = opt;
                self.entry_idx = 0;
                self.path = Some(base);
            } else {
                break;
            }
        }
        None
    }
}

///
/// Read the symbolic link value. This results in an error if the referenced
/// path is not a symbolic link.
///
fn read_link(path: &Path) -> Result<String, Error> {
    let value = fs::read_link(path)?;
    Ok(encode(value.to_str().unwrap()))
}

///
/// Create a `Tree` for the given path, recursively descending into child
/// directories. Any new trees found, as identified by their hash digest, will
/// be inserted into the database. The same is true for any files found, and
/// their extended attributes. The return value itself will also be added to the
/// database. The result will be that everything new will have been added as new
/// records.
///
fn scan_tree(basepath: &Path, dbase: &Database) -> Result<core::Tree, Error> {
    let mut entries: Vec<core::TreeEntry> = Vec::new();
    let mut file_count = 0;
    for entry in fs::read_dir(basepath)? {
        let entry = entry?;
        let file_type = entry.metadata()?.file_type();
        let path = entry.path();
        if file_type.is_dir() {
            let scan = scan_tree(&path, dbase)?;
            file_count += scan.file_count;
            let digest = scan.checksum();
            let tref = core::TreeReference::TREE(digest);
            let ent = process_path(&path, tref, dbase)?;
            entries.push(ent);
        } else if file_type.is_symlink() {
            let link = read_link(&path)?;
            let tref = core::TreeReference::LINK(link);
            let ent = process_path(&path, tref, dbase)?;
            entries.push(ent);
        } else if file_type.is_file() {
            let digest = core::checksum_file(&path)?;
            let tref = core::TreeReference::FILE(digest);
            let ent = process_path(&path, tref, dbase)?;
            entries.push(ent);
            file_count += 1;
        }
    }
    let tree = core::Tree::new(entries, file_count);
    let digest = tree.checksum();
    dbase.insert_tree(&digest, &tree)?;
    Ok(tree)
}

///
/// Create a `TreeEntry` record for this path, which may include storing
/// extended attributes in the database.
///
fn process_path(
    fullpath: &Path,
    reference: core::TreeReference,
    dbase: &Database,
) -> Result<core::TreeEntry, Error> {
    let mut entry = core::TreeEntry::new(fullpath, reference)?;
    entry = entry.mode(fullpath);
    entry = entry.owners(fullpath);
    if xattr::SUPPORTED_PLATFORM {
        let xattrs = xattr::list(fullpath)?;
        for name in xattrs {
            let nm = name
                .to_str()
                .ok_or_else(|| err_msg(format!("invalid UTF-8 in filename: {:?}", fullpath)))?;
            let value = xattr::get(fullpath, &name)?;
            if value.is_some() {
                let digest = core::checksum_data_sha1(value.as_ref().unwrap());
                dbase.insert_xattr(&digest, value.as_ref().unwrap())?;
                entry.xattrs.insert(nm.to_owned(), digest);
            }
        }
    }
    Ok(entry)
}

#[cfg(test)]
mod tests {
    use super::core::*;
    use super::*;
    use std::collections::HashMap;
    #[cfg(target_family = "unix")]
    use std::os::unix::fs;
    #[cfg(target_family = "windows")]
    use std::os::windows::fs;
    use std::time::SystemTime;
    use tempfile::tempdir;

    #[test]
    fn test_read_link() -> Result<(), Error> {
        let outdir = tempdir()?;
        let link = outdir.path().join("mylink");
        let target = "link_target_is_meaningless";
        // cfg! macro doesn't work for this case it seems so we have this
        // redundant use of the cfg directive instead
        #[cfg(target_family = "unix")]
        fs::symlink(&target, &link)?;
        #[cfg(target_family = "windows")]
        fs::symlink_file(&target, &link)?;
        let actual = read_link(&link)?;
        assert_eq!(actual, "bGlua190YXJnZXRfaXNfbWVhbmluZ2xlc3M=");
        Ok(())
    }

    #[test]
    fn test_checksum_tree() {
        let tref1 = TreeReference::FILE(Checksum::SHA1("cafebabe".to_owned()));
        let entry1 = core::TreeEntry {
            name: String::from("madoka.kaname"),
            fstype: core::EntryType::FILE,
            mode: Some(0o644),
            uid: Some(100),
            gid: Some(100),
            user: Some(String::from("user")),
            group: Some(String::from("group")),
            ctime: SystemTime::UNIX_EPOCH,
            mtime: SystemTime::UNIX_EPOCH,
            reference: tref1,
            xattrs: HashMap::new(),
        };
        let tref2 = TreeReference::FILE(Checksum::SHA1("babecafe".to_owned()));
        let entry2 = core::TreeEntry {
            name: String::from("homura.akemi"),
            fstype: core::EntryType::FILE,
            mode: Some(0o644),
            uid: Some(100),
            gid: Some(100),
            user: Some(String::from("user")),
            group: Some(String::from("group")),
            ctime: SystemTime::UNIX_EPOCH,
            mtime: SystemTime::UNIX_EPOCH,
            reference: tref2,
            xattrs: HashMap::new(),
        };
        let tref3 = TreeReference::FILE(Checksum::SHA1("babebabe".to_owned()));
        let entry3 = core::TreeEntry {
            name: String::from("sayaka.miki"),
            fstype: core::EntryType::FILE,
            mode: Some(0o644),
            uid: Some(100),
            gid: Some(100),
            user: Some(String::from("user")),
            group: Some(String::from("group")),
            ctime: SystemTime::UNIX_EPOCH,
            mtime: SystemTime::UNIX_EPOCH,
            reference: tref3,
            xattrs: HashMap::new(),
        };
        let tree = core::Tree::new(vec![entry1, entry2, entry3], 2);
        // would look something like this, if we used "now" instead of unix epoch
        // 644 100:100 1552877320 1552877320 sha1-babecafe homura.akemi
        // 644 100:100 1552877320 1552877320 sha1-cafebabe madoka.kaname
        // 644 100:100 1552877320 1552877320 sha1-babebabe sayaka.miki
        let result = tree.to_string();
        // results should be sorted lexicographically by filename
        assert!(result.find("homura").unwrap() < result.find("madoka").unwrap());
        assert!(result.find("madoka").unwrap() < result.find("sayaka").unwrap());
        let sum = tree.checksum();
        // because the timestamps are always 0, sha1 is always the same
        assert_eq!(
            sum.to_string(),
            "sha1-086f6c6ba3e51882c4fd55fc9733316c4ee1b15d"
        );
    }
}
