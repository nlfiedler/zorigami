//
// Copyright (c) 2019 Nathan Fiedler
//
use super::core;
use super::database::Database;
use base64::encode;
use failure::{err_msg, Error};
use std::cmp;
use std::collections::{HashMap, HashSet, VecDeque};
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
#[derive(Debug)]
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
pub struct ChangedFilesIter<'a> {
    /// Reference to Database for fetching records.
    dbase: &'a Database,
    /// Queue of pending paths to visit, where the path is relative, the first
    /// checksum is the left tree (earlier in time), and the second is the right
    /// tree (later in time).
    queue: VecDeque<(PathBuf, core::Checksum, core::Checksum)>,
    /// Nested iterator for visiting an entire new subdirectory.
    walker: Option<TreeWalker<'a>>,
    /// Current path being visited.
    path: Option<PathBuf>,
    /// Left tree currently being visited.
    left_tree: Option<core::Tree>,
    /// Position within left tree currently being iterated.
    left_idx: usize,
    /// Right tree currently being visited.
    right_tree: Option<core::Tree>,
    /// Position within right tree currently being iterated.
    right_idx: usize,
}

impl<'a> ChangedFilesIter<'a> {
    fn new(dbase: &'a Database, left_tree: core::Checksum, right_tree: core::Checksum) -> Self {
        let mut queue = VecDeque::new();
        queue.push_back((PathBuf::from("."), left_tree, right_tree));
        Self {
            dbase,
            queue,
            walker: None,
            path: None,
            left_tree: None,
            left_idx: 0,
            right_tree: None,
            right_idx: 0,
        }
    }
}

impl<'a> Iterator for ChangedFilesIter<'a> {
    type Item = Result<ChangedFile, Error>;

    fn next(&mut self) -> Option<Result<ChangedFile, Error>> {
        //
        // the flow is slightly complicated because we are sometimes nesting an
        // iterator inside this iterator, so the control flow will break out of
        // inner loops to return to this top loop
        //
        loop {
            // if we are iterating on a new subtree, return the next entry
            if let Some(iter) = self.walker.as_mut() {
                let opt = iter.next();
                if opt.is_some() {
                    return opt;
                }
                // inner iterator is done, carry on with the next step
                self.walker.take();
            }
            // is there a left and right tree? iterate on that
            if self.left_tree.is_some() && self.right_tree.is_some() {
                let left_tree = self.left_tree.as_ref().unwrap();
                let right_tree = self.right_tree.as_ref().unwrap();
                while self.left_idx < left_tree.entries.len()
                    && self.right_idx < right_tree.entries.len()
                {
                    let base = self.path.as_ref().unwrap();
                    let left_entry = &left_tree.entries[self.left_idx];
                    let right_entry = &right_tree.entries[self.right_idx];
                    if left_entry.name < right_entry.name {
                        // file or directory has been removed, nothing to do
                        self.left_idx += 1;
                    } else if left_entry.name > right_entry.name {
                        // file or directory has been added
                        self.right_idx += 1;
                        if right_entry.fstype.is_dir() {
                            // a new tree: add every file contained therein
                            let mut path = PathBuf::from(base);
                            path.push(&right_entry.name);
                            let sum = right_entry.reference.checksum().unwrap();
                            self.walker = Some(TreeWalker::new(self.dbase, &path, sum));
                            // return to the main loop
                            break;
                        } else if right_entry.fstype.is_file() {
                            // return the file
                            let sum = right_entry.reference.checksum().unwrap();
                            let mut path = PathBuf::from(base);
                            path.push(&right_entry.name);
                            let changed = ChangedFile::new(&path, sum);
                            return Some(Ok(changed));
                        }
                    } else if left_entry.reference != right_entry.reference {
                        // they have the same name but differ somehow
                        self.left_idx += 1;
                        self.right_idx += 1;
                        let left_is_dir = left_entry.fstype.is_dir();
                        let left_is_file = left_entry.fstype.is_file();
                        let left_is_link = left_entry.fstype.is_link();
                        let right_is_dir = right_entry.fstype.is_dir();
                        let right_is_file = right_entry.fstype.is_file();
                        if left_is_dir && right_is_dir {
                            // tree A & B: add both trees to the queue
                            let left_sum = left_entry.reference.checksum().unwrap();
                            let right_sum = right_entry.reference.checksum().unwrap();
                            let mut path = PathBuf::from(base);
                            path.push(&left_entry.name);
                            self.queue.push_back((path, left_sum, right_sum));
                        } else if (left_is_file || left_is_dir || left_is_link) && right_is_file {
                            // new file or a changed file
                            let sum = right_entry.reference.checksum().unwrap();
                            let mut path = PathBuf::from(base);
                            path.push(&right_entry.name);
                            let changed = ChangedFile::new(&path, sum);
                            return Some(Ok(changed));
                        } else if (left_is_file || left_is_link) && right_is_dir {
                            // now a directory, add everything under it
                            let mut path = PathBuf::from(base);
                            path.push(&right_entry.name);
                            let sum = right_entry.reference.checksum().unwrap();
                            self.walker = Some(TreeWalker::new(self.dbase, &path, sum));
                            // return to the main loop
                            break;
                        }
                    // ignore everything else
                    } else {
                        // they are the same
                        self.left_idx += 1;
                        self.right_idx += 1;
                    }
                }
                // catch everything else in the new snapshot as long as we do
                // not have a nested iterator to process
                while self.walker.is_none() && self.right_idx < right_tree.entries.len() {
                    let base = self.path.as_ref().unwrap();
                    let right_entry = &right_tree.entries[self.right_idx];
                    self.right_idx += 1;
                    if right_entry.fstype.is_dir() {
                        // a new tree: add every file contained therein
                        let mut path = PathBuf::from(base);
                        path.push(&right_entry.name);
                        let sum = right_entry.reference.checksum().unwrap();
                        self.walker = Some(TreeWalker::new(self.dbase, &path, sum));
                    } else if right_entry.fstype.is_file() {
                        // return the file
                        let sum = right_entry.reference.checksum().unwrap();
                        let mut path = PathBuf::from(base);
                        path.push(&right_entry.name);
                        let changed = ChangedFile::new(&path, sum);
                        return Some(Ok(changed));
                    }
                }
            }
            if self.walker.is_some() {
                // restart at the top when we have a subtree to iterate
                continue;
            }
            // Either we just started or we finished these trees, pop the queue
            // to get the next set and loop around.
            if let Some((base, left_sum, right_sum)) = self.queue.pop_front() {
                // dequeue the next entry, fetch the tree
                let result = self.dbase.get_tree(&left_sum);
                if result.is_err() {
                    return Some(Err(err_msg(format!("failed to get tree: {:?}", left_sum))));
                }
                let opt = result.unwrap();
                if opt.is_none() {
                    return Some(Err(err_msg(format!("missing tree: {:?}", left_sum))));
                }
                self.left_tree = opt;
                self.left_idx = 0;
                let result = self.dbase.get_tree(&right_sum);
                if result.is_err() {
                    return Some(Err(err_msg(format!("failed to get tree: {:?}", right_sum))));
                }
                let opt = result.unwrap();
                if opt.is_none() {
                    return Some(Err(err_msg(format!("missing tree: {:?}", right_sum))));
                }
                self.right_tree = opt;
                self.right_idx = 0;
                self.path = Some(base);
            } else {
                break;
            }
        }
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
    let snap2doc = dbase
        .get_snapshot(&snapshot2)?
        .ok_or_else(|| err_msg(format!("missing snapshot: {:?}", snapshot2)))?;
    Ok(ChangedFilesIter::new(dbase, snap1doc.tree, snap2doc.tree))
}

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
    pub fn new(dbase: &'a Database, basepath: &Path, tree: core::Checksum) -> Self {
        let mut queue = VecDeque::new();
        queue.push_back((basepath.to_owned(), tree));
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
        // DirEntry.metadata() does not follow symlinks
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

/// Builds pack files by splitting incoming files into chunks.
pub struct PackBuilder<'a> {
    /// Reference to Database for fetching records.
    dbase: &'a Database,
    /// Preferred size of pack files in bytes.
    pack_size: u64,
    /// Preferred size of chunks in bytes.
    chunk_size: u64,
    /// Map of file checksum to the chunks it contains that have not yet been
    /// uploaded in a pack file.
    file_chunks: HashMap<core::Checksum, Vec<core::Chunk>>,
    /// Those chunks that have been packed using this builder.
    packed_chunks: HashSet<core::Checksum>,
    /// Those chunks that have been uploaded previously.
    done_chunks: HashSet<core::Checksum>,
}

impl<'a> PackBuilder<'a> {
    /// Create a new builder with the desired size.
    pub fn new(dbase: &'a Database, pack_size: u64) -> Self {
        // Use the pack size as a guide for determining the chunk sizes. Limit
        // the size of chunks to something reasonable that will keep most files
        // intact, but also allow for keeping pack files close to their desired
        // size, whatever that might be.
        let chunk_size = cmp::max(1_048_576, cmp::min(pack_size / 4, 4_194_304));
        Self {
            dbase,
            pack_size,
            chunk_size,
            file_chunks: HashMap::new(),
            packed_chunks: HashSet::new(),
            done_chunks: HashSet::new(),
        }
    }

    /// Override the calculated chunk size with a non-standard value. Primarily
    /// used for testing with smaller files.
    pub fn chunk_size(mut self, chunk_size: u64) -> Self {
        self.chunk_size = chunk_size;
        self
    }

    /// Return number of files in this pack, primarily for testing.
    pub fn file_count(&self) -> usize {
        self.file_chunks.len()
    }

    /// Return number of chunks in this pack, primarily for testing. This does
    /// not consider done or packed chunks.
    pub fn chunk_count(&self) -> usize {
        let mut count: usize = 0;
        for chunks in self.file_chunks.values() {
            count += chunks.len();
        }
        count
    }

    /// Add the given file to this builder, splitting into chunks as necessary,
    /// and using the database to find duplicate chunks.
    pub fn add_file(&mut self, path: &Path, file_digest: core::Checksum) -> Result<(), Error> {
        if self.file_chunks.contains_key(&file_digest) {
            // do not bother processing a file we have already seen; once the
            // files have been completely uploaded, we rely on the database to
            // detect duplicate chunks
            return Ok(());
        }
        let attr = fs::metadata(path)?;
        let file_size = attr.len();
        let chunks = if file_size > self.chunk_size {
            // split large files into chunks, add chunks to the list
            core::find_file_chunks(path, self.chunk_size)?
        } else {
            let mut chunk = core::Chunk::new(file_digest.clone(), 0, file_size as usize);
            chunk = chunk.filepath(path);
            vec![chunk]
        };
        // find chunks that have already been recorded in the database
        chunks.iter().for_each(|chunk| {
            let result = self.dbase.get_chunk(&chunk.digest);
            if let Ok(value) = result {
                if value.is_some() {
                    self.done_chunks.insert(chunk.digest.clone());
                }
            }
        });
        // save the chunks under the digest of the file they came from to make
        // it easy to save everything to the database later
        self.file_chunks.insert(file_digest, chunks);
        Ok(())
    }

    /// Return true if this builder has chunks to pack.
    pub fn has_chunks(&self) -> bool {
        !self.file_chunks.is_empty()
    }

    /// If the builder has not files and chunks, then clear the cache of
    /// "packed" and "done" chunks to conserve space.
    pub fn clear_cache(&mut self) {
        if self.file_chunks.is_empty() {
            self.packed_chunks.clear();
            self.done_chunks.clear();
        }
    }

    /// Return true if this builder is ready to produce a pack.
    pub fn is_full(&self) -> bool {
        // This approach seemed better than tracking the size as a field, and
        // possibly making a mistake and not realizing for a long time.
        let mut total_size: u64 = 0;
        for chunks in self.file_chunks.values() {
            for chunk in chunks {
                let already_done = self.done_chunks.contains(&chunk.digest);
                let already_packed = self.packed_chunks.contains(&chunk.digest);
                if !already_done && !already_packed {
                    total_size += chunk.length as u64;
                }
            }
        }
        total_size > self.pack_size
    }

    /// Write a pack file to the given path, encrypting using OpenPGP with the
    /// given passphrase. If nothing has been added to the builder, then nothing
    /// is written and an empty pack is returned.
    pub fn build_pack(&mut self, outfile: &Path, passphrase: &str) -> Result<Pack, Error> {
        let mut pack: Pack = Default::default();
        let mut bytes_packed: u64 = 0;
        // while there are files to process and the pack is not too big...
        while !self.file_chunks.is_empty() && bytes_packed < self.pack_size {
            // get a random file from the map and start putting its chunks into
            // the pack, ignoring any duplicates
            let filesum = self.file_chunks.keys().take(1).next().unwrap().to_owned();
            let mut chunks_processed = 0;
            let chunks = &self.file_chunks[&filesum];
            for chunk in chunks {
                chunks_processed += 1;
                let already_done = self.done_chunks.contains(&chunk.digest);
                let already_packed = self.packed_chunks.contains(&chunk.digest);
                if !already_done && !already_packed {
                    pack.add_chunk(chunk.clone());
                    self.packed_chunks.insert(chunk.digest.clone());
                    bytes_packed += chunk.length as u64;
                    if bytes_packed > self.pack_size {
                        break;
                    }
                }
            }
            // if we successfully visited all of the chunks in this file,
            // including duplicates, then this file is considered "done"
            if chunks_processed == chunks.len() {
                let chunks = self.file_chunks.remove(&filesum).unwrap();
                pack.add_file(filesum, chunks);
            }
        }
        if bytes_packed > 0 {
            pack.build_pack(outfile, passphrase)?;
        }
        Ok(pack)
    }
}

/// Contains the results of building a pack, and provides functions for saving
/// the results to the database.
pub struct Pack {
    /// Checksum of this pack file once it has been written.
    digest: Option<core::Checksum>,
    /// Those files that have been completed with this pack.
    files: HashMap<core::Checksum, Vec<core::Chunk>>,
    /// Those chunks that are contained in this pack.
    chunks: Vec<core::Chunk>,
}

impl Pack {
    /// Add a completed file to this pack.
    pub fn add_file(&mut self, digest: core::Checksum, chunks: Vec<core::Chunk>) {
        self.files.insert(digest, chunks);
    }

    /// Add a chunk to this pack.
    pub fn add_chunk(&mut self, chunk: core::Chunk) {
        self.chunks.push(chunk);
    }

    /// Return a reference to this pack's hash digest.
    pub fn get_digest(&self) -> Option<&core::Checksum> {
        self.digest.as_ref()
    }

    /// Write the chunks in this pack to the specified path, encrypting using
    /// OpenPGP with the given passphrase.
    pub fn build_pack(&mut self, outfile: &Path, passphrase: &str) -> Result<(), Error> {
        let digest = core::pack_chunks(&self.chunks, outfile)?;
        let mut owtfile = outfile.to_path_buf();
        owtfile.set_extension(".pgp");
        core::encrypt_file(passphrase, outfile, &owtfile)?;
        fs::rename(owtfile, outfile)?;
        self.digest = Some(digest);
        Ok(())
    }

    /// Record the results of building this pack to the database. This includes
    /// the completed files, all chunks, and the pack itself.
    pub fn record_completed(
        &mut self,
        dbase: &Database,
        bucket: &str,
        object: &str,
    ) -> Result<(), Error> {
        // massage the file/chunk data into database records and save
        for (filesum, parts) in &self.files {
            let mut length: u64 = 0;
            let mut chunks: Vec<(u64, core::Checksum)> = Vec::new();
            for chunk in parts {
                length += chunk.length as u64;
                chunks.push((chunk.offset as u64, chunk.digest.clone()));
            }
            let file = core::SavedFile::new(filesum.clone(), length, chunks);
            dbase.insert_file(&file)?;
        }
        // save all of the chunks to the database; would have done this while
        // processing the files, but cannot borrow the files map immutably and
        // mutably at the same time
        for chunks in self.files.values_mut() {
            for chunk in chunks.iter_mut() {
                // set the pack digest for each chunk record
                chunk.packfile = Some(self.digest.as_ref().unwrap().clone());
                dbase.insert_chunk(chunk)?;
            }
        }
        // record the pack in the database
        let pack = core::SavedPack::new(self.digest.as_ref().unwrap().clone(), bucket, object);
        dbase.insert_pack(&pack)?;
        Ok(())
    }
}

impl Default for Pack {
    fn default() -> Self {
        Self {
            digest: None,
            files: HashMap::new(),
            chunks: Vec::new(),
        }
    }
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