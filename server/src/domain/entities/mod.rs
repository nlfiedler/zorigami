//
// Copyright (c) 2020 Nathan Fiedler
//
use anyhow::{anyhow, Error};
use base64::{engine::general_purpose, Engine as _};
use chrono::prelude::*;
use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::SystemTime;
use store_core::Coordinates;
use uuid::Uuid;

pub mod schedule;

///
/// The `Checksum` represents a hash digest for an object, such as a tree,
/// snapshot, file, chunk, or pack file.
///
#[derive(Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub enum Checksum {
    SHA1(String),
    BLAKE3(String),
}

impl Checksum {
    ///
    /// Compute the SHA1 hash digest of the given data.
    ///
    pub fn sha1_from_bytes(data: &[u8]) -> Checksum {
        use sha1::{Digest, Sha1};
        let mut hasher = Sha1::new();
        hasher.update(data);
        let digest = hasher.finalize();
        Checksum::SHA1(format!("{:x}", digest))
    }

    ///
    /// Compute the BLAKE3 hash digest of the given data.
    ///
    pub fn blake3_from_bytes(data: &[u8]) -> Checksum {
        let mut hasher = blake3::Hasher::new();
        hasher.update(data);
        let digest = hasher.finalize();
        Checksum::BLAKE3(format!("{}", digest))
    }

    ///
    /// Compute the BLAKE3 hash digest of the given file.
    ///
    pub fn blake3_from_file(infile: &Path) -> io::Result<Checksum> {
        let mut file = fs::File::open(infile)?;
        let mut hasher = blake3::Hasher::new();
        io::copy(&mut file, &mut hasher)?;
        let digest = hasher.finalize();
        Ok(Checksum::BLAKE3(format!("{}", digest)))
    }

    /// Return `true` if this checksum is a SHA1.
    pub fn is_sha1(&self) -> bool {
        matches!(*self, Checksum::SHA1(_))
    }

    /// Return `true` if this checksum is a BLAKE3.
    pub fn is_blake3(&self) -> bool {
        matches!(*self, Checksum::BLAKE3(_))
    }
}

impl Clone for Checksum {
    fn clone(&self) -> Self {
        match self {
            Checksum::SHA1(sum) => Checksum::SHA1(sum.to_owned()),
            Checksum::BLAKE3(sum) => Checksum::BLAKE3(sum.to_owned()),
        }
    }
}

/// Useful for constructing a meaningless SHA1 value.
pub static FORTY_ZEROS: &str = "0000000000000000000000000000000000000000";

impl Default for Checksum {
    fn default() -> Self {
        Checksum::SHA1(String::from(FORTY_ZEROS))
    }
}

impl fmt::Display for Checksum {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Checksum::SHA1(hash) => write!(f, "sha1-{}", hash),
            Checksum::BLAKE3(hash) => write!(f, "blake3-{}", hash),
        }
    }
}

impl FromStr for Checksum {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(hash) = s.strip_prefix("sha1-") {
            Ok(Checksum::SHA1(hash.to_owned()))
        } else if let Some(hash) = s.strip_prefix("blake3-") {
            Ok(Checksum::BLAKE3(hash.to_owned()))
        } else {
            Err(anyhow!(format!("not a recognized algorithm: {}", s)))
        }
    }
}

/// Represents a piece of a file, and possibly an entire file.
#[derive(Clone, Debug)]
pub struct Chunk {
    /// The hash digest of the chunk, with algorithm prefix.
    pub digest: Checksum,
    /// The byte offset of this chunk within the file.
    pub offset: usize,
    /// The byte length of this chunk.
    pub length: usize,
    /// Path of the file from which the chunk is taken.
    pub filepath: Option<PathBuf>,
    /// Digest of packfile this chunk is stored within.
    pub packfile: Option<Checksum>,
}

impl Chunk {
    /// Construct a `Chunk` from the given values.
    pub fn new(digest: Checksum, offset: usize, length: usize) -> Self {
        Self {
            digest,
            offset,
            length,
            filepath: None,
            packfile: None,
        }
    }

    /// Add the filepath property.
    pub fn filepath(mut self, filepath: &Path) -> Self {
        self.filepath = Some(filepath.to_owned());
        self
    }

    /// Add the packfile property.
    pub fn packfile(mut self, packfile: Checksum) -> Self {
        self.packfile = Some(packfile);
        self
    }
}

/// StoreType identifies a kind of store.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum StoreType {
    AMAZON,
    AZURE,
    GOOGLE,
    LOCAL,
    MINIO,
    SFTP,
}

impl ToString for StoreType {
    fn to_string(&self) -> String {
        match self {
            StoreType::AMAZON => String::from("amazon"),
            StoreType::AZURE => String::from("azure"),
            StoreType::GOOGLE => String::from("google"),
            StoreType::LOCAL => String::from("local"),
            StoreType::MINIO => String::from("minio"),
            StoreType::SFTP => String::from("sftp"),
        }
    }
}

impl FromStr for StoreType {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "amazon" => Ok(StoreType::AMAZON),
            "azure" => Ok(StoreType::AZURE),
            "google" => Ok(StoreType::GOOGLE),
            "local" => Ok(StoreType::LOCAL),
            "minio" => Ok(StoreType::MINIO),
            "sftp" => Ok(StoreType::SFTP),
            _ => Err(anyhow!(format!("not a recognized store type: {}", s))),
        }
    }
}

/// Store defines a location where packs will be saved.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Store {
    /// Unique identifier for this store.
    pub id: String,
    /// Type of this store.
    pub store_type: StoreType,
    /// User-defined label for this store.
    pub label: String,
    /// Name/value pairs that make up this store configuration.
    pub properties: HashMap<String, String>,
}

impl std::hash::Hash for Store {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

///
/// Policy dictating how many snapshots to retain.
///
#[derive(Clone, Debug, PartialEq)]
pub enum RetentionPolicy {
    /// All snapshots will be retained indefinitely.
    ALL,
    /// Retain this many snapshots.
    COUNT(u16),
    /// Retain snapshots for this many days.
    DAYS(u16),
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        RetentionPolicy::ALL
    }
}

/// Represents a directory tree that will be backed up according to a schedule,
/// with pack files saved to a particular local or remote store.
#[derive(Clone, Debug)]
pub struct Dataset {
    /// Unique identifier of this dataset.
    pub id: String,
    /// Local base path of dataset to be saved.
    pub basepath: PathBuf,
    /// Set of schedules for when to run the backup.
    pub schedules: Vec<schedule::Schedule>,
    /// Path for temporary pack building.
    pub workspace: PathBuf,
    /// Target size in bytes for pack files.
    pub pack_size: u64,
    /// Identifiers of the stores to contain pack files.
    pub stores: Vec<String>,
    /// List of file/directory exclusion patterns.
    pub excludes: Vec<String>,
    /// Policy for retaining snapshots over time.
    pub retention: RetentionPolicy,
}

// Default pack size is 64mb just because. With a typical ADSL home broadband
// connection a 64mb pack file should take about 5 minutes to upload.
const DEFAULT_PACK_SIZE: u64 = 67_108_864;

impl Dataset {
    /// Construct a Dataset with the path of the directory tree to be saved.
    pub fn new(basepath: &Path) -> Dataset {
        Dataset::with_pack_size(basepath, DEFAULT_PACK_SIZE)
    }

    /// Construct a `Dataset` with the given path and pack size.
    pub fn with_pack_size(basepath: &Path, pack_size: u64) -> Self {
        let id = xid::new().to_string();
        let mut workspace = basepath.to_owned();
        workspace.push(".tmp");
        Self {
            id,
            basepath: basepath.to_owned(),
            schedules: vec![],
            workspace,
            pack_size,
            stores: vec![],
            excludes: vec![],
            retention: Default::default(),
        }
    }

    /// Add the given store identifier to the dataset.
    pub fn add_store(&mut self, store: &str) {
        self.stores.push(store.to_owned());
    }

    /// Add the given schedule to the dataset.
    pub fn add_schedule(&mut self, schedule: schedule::Schedule) {
        self.schedules.push(schedule);
    }
}

impl Default for Dataset {
    fn default() -> Self {
        Self {
            id: String::new(),
            basepath: PathBuf::new(),
            schedules: vec![],
            workspace: PathBuf::new(),
            pack_size: 0,
            stores: vec![],
            excludes: vec![],
            retention: Default::default(),
        }
    }
}

impl fmt::Display for Dataset {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "dataset-{}:{:?}", self.id, self.basepath)
    }
}

///
/// A `TreeReference` represents the "value" for a tree entry, which can be one
/// of the following: the checksum of a tree, the checksum of a file, the
/// contents of a symbolic link, or the contents of a very small file.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TreeReference {
    /// Raw value of a symbolic link.
    LINK(Vec<u8>),
    /// Hash digest of the formatted tree.
    TREE(Checksum),
    /// Hash digest of the file contents.
    FILE(Checksum),
    /// Raw contents of a very small file.
    SMALL(Vec<u8>),
}

impl TreeReference {
    /// Return `true` if this reference is for a symbolic link.
    pub fn is_link(&self) -> bool {
        matches!(*self, TreeReference::LINK(_))
    }

    /// Return `true` if this reference is for a tree.
    pub fn is_tree(&self) -> bool {
        matches!(*self, TreeReference::TREE(_))
    }

    /// Return `true` if this reference is for a file.
    pub fn is_file(&self) -> bool {
        matches!(*self, TreeReference::FILE(_))
    }

    /// Return `true` if this reference is for a very small file.
    pub fn is_small(&self) -> bool {
        matches!(*self, TreeReference::SMALL(_))
    }

    /// Return the checksum for this reference, if possible.
    pub fn checksum(&self) -> Option<Checksum> {
        match self {
            TreeReference::TREE(sum) => Some(sum.clone()),
            TreeReference::FILE(sum) => Some(sum.clone()),
            _ => None,
        }
    }

    /// Return the base64 encoded value for this symlink, if possible.
    pub fn symlink(&self) -> Option<Vec<u8>> {
        match self {
            TreeReference::LINK(link) => Some(link.clone()),
            _ => None,
        }
    }
}

impl fmt::Display for TreeReference {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TreeReference::LINK(value) => {
                let encoded = general_purpose::STANDARD.encode(value);
                write!(f, "link-{}", encoded)
            }
            TreeReference::TREE(digest) => write!(f, "tree-{}", digest),
            TreeReference::FILE(digest) => write!(f, "file-{}", digest),
            TreeReference::SMALL(contents) => {
                let encoded = general_purpose::STANDARD.encode(contents);
                write!(f, "small-{}", encoded)
            }
        }
    }
}

impl FromStr for TreeReference {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(value) = s.strip_prefix("link-") {
            let decoded = general_purpose::STANDARD.decode(value)?;
            Ok(TreeReference::LINK(decoded))
        } else if let Some(value) = s.strip_prefix("tree-") {
            let digest: Result<Checksum, Error> = FromStr::from_str(value);
            Ok(TreeReference::TREE(digest.expect("invalid tree SHA1")))
        } else if let Some(value) = s.strip_prefix("file-") {
            let digest: Result<Checksum, Error> = FromStr::from_str(value);
            Ok(TreeReference::FILE(digest.expect("invalid file BLAKE3")))
        } else if let Some(value) = s.strip_prefix("small-") {
            let decoded = general_purpose::STANDARD.decode(value)?;
            Ok(TreeReference::SMALL(decoded))
        } else {
            Err(anyhow!(format!("not a recognized reference: {}", s)))
        }
    }
}

///
/// Return the last part of the path, converting to a String.
///
fn get_file_name(path: &Path) -> String {
    // ignore any paths that end in '..'
    if let Some(p) = path.file_name() {
        // ignore any paths that failed UTF-8 translation
        if let Some(pp) = p.to_str() {
            return pp.to_owned();
        }
    }
    // normal conversion failed, return whatever garbage is there
    path.to_string_lossy().into_owned()
}

/// A file, directory, or symbolic link within a tree.
#[derive(Clone, Debug)]
pub struct TreeEntry {
    /// Name of the file, directory, or symbolic link.
    pub name: String,
    /// Unix file mode of the entry.
    pub mode: Option<u32>,
    /// Unix user identifier
    pub uid: Option<u32>,
    /// Name of the owning user.
    pub user: Option<String>,
    /// Unix group identifier
    pub gid: Option<u32>,
    /// Name of the owning group.
    pub group: Option<String>,
    /// Created time of the entry.
    pub ctime: DateTime<Utc>,
    /// Modification time of the entry.
    pub mtime: DateTime<Utc>,
    /// Reference to the entry itself.
    pub reference: TreeReference,
    /// Set of extended file attributes, if any. The key is the name of the
    /// extended attribute, and the value is the SHA1 digest for the value
    /// already recorded. Each unique value is meant to be stored once.
    pub xattrs: HashMap<String, Checksum>,
}

impl TreeEntry {
    ///
    /// Create an instance of `TreeEntry` based on the given path.
    ///
    pub fn new(path: &Path, reference: TreeReference) -> Self {
        let name = get_file_name(path);
        // Lot of error handling built-in so we can safely process any path
        // entry and not blow up the backup process.
        let metadata = fs::symlink_metadata(path);
        let mtime = match metadata.as_ref() {
            Ok(attr) => attr.modified().unwrap_or(SystemTime::UNIX_EPOCH),
            Err(_) => SystemTime::UNIX_EPOCH,
        };
        // creation time is not available on all platforms, and we are only
        // using it to record a value in the database
        let ctime = match metadata.as_ref() {
            Ok(attr) => attr.created().unwrap_or(SystemTime::UNIX_EPOCH),
            Err(_) => SystemTime::UNIX_EPOCH,
        };
        Self {
            name,
            mode: None,
            uid: None,
            gid: None,
            user: None,
            group: None,
            ctime: DateTime::<Utc>::from(ctime),
            mtime: DateTime::<Utc>::from(mtime),
            reference,
            xattrs: HashMap::new(),
        }
    }

    ///
    /// Set the `mode` property to either the Unix file mode or the
    /// Windows attributes value, both of which are u32 values.
    ///
    #[cfg(target_family = "unix")]
    pub fn mode(mut self, path: &Path) -> Self {
        // Either mode or file attributes will be sufficient to cover all
        // supported systems; the "permissions" field only has one bit,
        // read-only, and that is already in mode and file attributes.
        use std::os::unix::fs::MetadataExt;
        if let Ok(meta) = fs::symlink_metadata(path) {
            self.mode = Some(meta.mode());
        }
        self
    }

    #[cfg(target_family = "windows")]
    pub fn mode(self, _path: &Path) -> Self {
        //
        // The Windows attributes value is a number but it is not anything like
        // a Unix file mode, which is what we really want so that we get a value
        // similar to a Git tree entry.
        //
        // use std::os::windows::prelude::*; if let Ok(meta) =
        //     fs::symlink_metadata(path) {self.mode =
        //     Some(meta.file_attributes());
        //     }
        self
    }

    ///
    /// Set the user and group ownership of the given path. At present, only
    /// Unix systems have this information.
    ///
    #[cfg(target_family = "unix")]
    pub fn owners(mut self, path: &Path) -> Self {
        use std::ffi::CStr;
        use std::os::unix::fs::MetadataExt;
        if let Ok(meta) = fs::symlink_metadata(path) {
            self.uid = Some(meta.uid());
            self.gid = Some(meta.gid());
            // get the user name
            let username: String = unsafe {
                let passwd = libc::getpwuid(meta.uid());
                if passwd.is_null() {
                    String::new()
                } else {
                    let c_buf = (*passwd).pw_name;
                    if c_buf.is_null() {
                        String::new()
                    } else {
                        CStr::from_ptr(c_buf).to_string_lossy().into_owned()
                    }
                }
            };
            self.user = Some(username);
            // get the group name
            let groupname = unsafe {
                let group = libc::getgrgid(meta.gid());
                if group.is_null() {
                    String::new()
                } else {
                    let c_buf = (*group).gr_name;
                    if c_buf.is_null() {
                        String::new()
                    } else {
                        CStr::from_ptr(c_buf).to_string_lossy().into_owned()
                    }
                }
            };
            self.group = Some(groupname);
        }
        self
    }

    #[cfg(target_family = "windows")]
    pub fn owners(self, _path: &Path) -> Self {
        self
    }
}

impl fmt::Display for TreeEntry {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let form_mode = if let Some(mode) = self.mode {
            mode
        } else if self.reference.is_tree() {
            0o040_000
        } else {
            0o100_644
        };
        let ctime = self.ctime.timestamp();
        let mtime = self.mtime.timestamp();
        // Format in a manner similar to git tree entries; this forms part of
        // the digest value for the overall tree, so it should remain relatively
        // stable over time.
        write!(
            f,
            "{:o} {}:{} {} {} {} {}",
            form_mode,
            self.uid.unwrap_or(0),
            self.gid.unwrap_or(0),
            ctime,
            mtime,
            self.reference,
            self.name
        )
    }
}

///
/// A set of file system entries, such as files, directories, symbolic links.
///
#[derive(Clone, Debug)]
pub struct Tree {
    /// Digest of tree at time of snapshot.
    pub digest: Checksum,
    /// Set of entries making up this tree.
    pub entries: Vec<TreeEntry>,
    /// The number of files contained within this tree and its subtrees.
    pub file_count: u32,
}

impl Tree {
    /// Create an instance of Tree that takes ownership of the given entries.
    ///
    /// The entries will be sorted by name, hence must be mutable.
    pub fn new(mut entries: Vec<TreeEntry>, file_count: u32) -> Self {
        entries.sort_unstable_by(|a, b| a.name.cmp(&b.name));
        let mut tree = Self {
            digest: Checksum::SHA1(FORTY_ZEROS.to_owned()),
            entries,
            file_count,
        };
        let as_text = tree.to_string();
        tree.digest = Checksum::sha1_from_bytes(as_text.as_bytes());
        tree
    }
}

impl fmt::Display for Tree {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for entry in &self.entries {
            writeln!(f, "{}", entry)?;
        }
        Ok(())
    }
}

///
/// `File` records the chunks associated with a saved file.
///
#[derive(Clone, Debug)]
pub struct File {
    /// Digest of file at time of snapshot.
    pub digest: Checksum,
    /// Length of the file in bytes.
    pub length: u64,
    /// The set of the chunks contained in this file. There may be many of these
    /// for large files, so they are represented compactly. The first field is
    /// the byte offset of the chunk within the saved file.
    ///
    /// If the list contains only a single entry, then the checksum is that of
    /// the pack record, avoiding the need for a chunk record.
    pub chunks: Vec<(u64, Checksum)>,
}

impl File {
    /// Create a new File to represent the given file and its chunks.
    pub fn new(digest: Checksum, length: u64, chunks: Vec<(u64, Checksum)>) -> Self {
        Self {
            digest,
            length,
            chunks,
        }
    }
}

///
/// Holds statistics regarding a specific snapshot.
///
#[derive(Clone, Debug, Default, PartialEq)]
pub struct FileCounts {
    pub directories: u32,
    pub symlinks: u32,
    /// very small files are <= 80 bytes
    pub very_small_files: u32,
    /// very large files are >= 4 gb
    pub very_large_files: u32,
    /// Mapping of file counts by log2 of their file size.
    pub file_sizes: HashMap<u8, u32>,
}

// Size of a file that is smaller than the corresponding FileDef record. As
// such, storing it directly in the tree would be appropriate.
pub const FILE_SIZE_SMALL: u64 = 80;

impl FileCounts {
    /// Update the the file counts to track the given file size.
    pub fn register_file(&mut self, size: u64) {
        if size <= FILE_SIZE_SMALL {
            self.very_small_files += 1;
        } else if size > 4_294_967_295 {
            self.very_large_files += 1;
        } else {
            // we determined above that the size fits in a u32
            let power = f64::from(size as u32).log2().round() as u32;
            // Convert the power-of-2 value to the smallest integer for storage
            // efficiency (2^255 is already much larger than very_large_files).
            // Use 64 as the maximum value for the preso layer to convert the
            // value back to a number-string.
            let bits: u8 = power.try_into().map_or(64_u8, |v: u32| v as u8);
            if let Some(value) = self.file_sizes.get_mut(&bits) {
                *value += 1;
            } else {
                self.file_sizes.insert(bits, 1);
            }
        }
    }

    /// Return the total number of files tracked by this record.
    pub fn total_files(&self) -> u64 {
        let mut count: u64 = self.very_small_files.into();
        count += self.very_large_files as u64;
        for value in self.file_sizes.values() {
            count += *value as u64;
        }
        count
    }
}

///
/// A `Snapshot` represents a single backup, either in progress or completed.
/// It references a possible parent snapshot, and a tree representing the files
/// contained in the snapshot.
///
#[derive(Clone, Debug)]
pub struct Snapshot {
    /// Unique identifier of this snapshot.
    pub digest: Checksum,
    /// Digest of the parent snapshot, if any.
    pub parent: Option<Checksum>,
    /// Time when the snapshot was first created.
    pub start_time: DateTime<Utc>,
    /// Time when the snapshot completed its upload. Will be `None` until
    /// the backup has completed.
    pub end_time: Option<DateTime<Utc>>,
    /// Number of files and directories contained in this snapshot.
    pub file_counts: FileCounts,
    /// Digest of the root tree for this snapshot.
    pub tree: Checksum,
}

impl Snapshot {
    /// Construct a new `Snapshot` for the given tree, and optional parent.
    pub fn new(parent: Option<Checksum>, tree: Checksum, file_counts: FileCounts) -> Self {
        let start_time = Utc::now();
        let mut snapshot = Self {
            digest: Checksum::SHA1(FORTY_ZEROS.to_owned()),
            parent,
            start_time,
            end_time: None,
            file_counts,
            tree,
        };
        // Need to compute a checksum and save that as the "key" for this
        // snapshot, cannot compute the checksum later because the object is
        // mutable (e.g. end time).
        let formed = snapshot.to_string();
        snapshot.digest = Checksum::sha1_from_bytes(formed.as_bytes());
        snapshot
    }

    /// Set the `end_time` property.
    pub fn set_end_time(&mut self, end_time: DateTime<Utc>) {
        self.end_time = Some(end_time);
    }

    /// Set the `start_time` property.
    pub fn set_start_time(&mut self, start_time: DateTime<Utc>) {
        self.start_time = start_time;
    }
}

/// A SHA1 of all zeroes.
pub static NULL_SHA1: &str = "sha1-0000000000000000000000000000000000000000";

impl fmt::Display for Snapshot {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let start_time = self.start_time.timestamp();
        // Format in a manner similar to git commit entries; this forms part of
        // the digest value for the snapshot, so it should remain relatively
        // stable over time.
        let parent = match self.parent {
            None => NULL_SHA1.to_string(),
            Some(ref value) => value.to_string(),
        };
        let file_count = self.file_counts.total_files();
        write!(
            f,
            "tree {}\nparent {}\nnumFiles {}\nstartTime {}",
            self.tree, parent, file_count, start_time
        )
    }
}

///
/// Location for a pack file, naming the store, bucket, and object by which the
/// pack file can be retrieved.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PackLocation {
    /// Unique identifier of the pack store.
    pub store: String,
    /// Remote bucket name.
    pub bucket: String,
    /// Remote object name.
    pub object: String,
}

impl PackLocation {
    /// Create a new PackLocation record using the given information.
    pub fn new(store: &str, bucket: &str, object: &str) -> Self {
        Self {
            store: store.to_owned(),
            bucket: bucket.to_owned(),
            object: object.to_owned(),
        }
    }
}

impl From<Coordinates> for PackLocation {
    fn from(coords: Coordinates) -> Self {
        PackLocation {
            store: coords.store,
            bucket: coords.bucket,
            object: coords.object,
        }
    }
}

impl From<PackLocation> for Coordinates {
    fn from(val: PackLocation) -> Self {
        Coordinates {
            store: val.store,
            bucket: val.bucket,
            object: val.object,
        }
    }
}

/// Type for database record of saved packs.
#[derive(Clone, Debug)]
pub struct Pack {
    /// Digest of pack file.
    pub digest: Checksum,
    /// List of pack locations.
    pub locations: Vec<PackLocation>,
}

impl Pack {
    /// Create a new Pack record using the given information. Assumes the
    /// upload time is the current time.
    pub fn new(digest: Checksum, coords: Vec<PackLocation>) -> Self {
        Self {
            digest,
            locations: coords,
        }
    }
}

/// Information about an entry in a pack file.
#[derive(Clone, Debug)]
pub struct PackEntry {
    /// File name of the entry in the pack file.
    pub name: String,
    /// Length of the content of the entry.
    pub size: u64,
}

impl PackEntry {
    /// Create a new PackEntry using the given information.
    pub fn new(name: String, size: u64) -> Self {
        Self { name, size }
    }
}

/// Details about a pack file and its contents.
#[derive(Clone, Debug)]
pub struct PackFile {
    /// Length of the pack file.
    pub length: u64,
    /// All entries in the pack file.
    pub entries: Vec<PackEntry>,
    /// Total size of all pack entries.
    pub content_length: u64,
    /// Size of the smallest pack entry.
    pub smallest: u64,
    /// Size of the largest pack entry.
    pub largest: u64,
    /// Average size of the pack entries.
    pub average: u64,
}

impl PackFile {
    /// Create a new PackFile.
    pub fn new(length: u64, entries: Vec<PackEntry>) -> Self {
        let mut content_length: u64 = 0;
        let mut smallest: u64 = u64::MAX;
        let mut largest: u64 = 0;
        for entry in entries.iter() {
            content_length += entry.size;
            if entry.size < smallest {
                smallest = entry.size;
            }
            if entry.size > largest {
                largest = entry.size;
            }
        }
        let count: u64 = entries.len() as u64;
        let average: u64 = if count > 0 { content_length / count } else { 0 };
        if content_length == 0 {
            smallest = 0;
        }
        Self {
            length,
            entries,
            content_length,
            smallest,
            largest,
            average,
        }
    }
}

/// Contains the configuration of the application, pertaining to all datasets.
#[derive(Clone, Debug)]
pub struct Configuration {
    /// Name of the computer on which this application is running.
    pub hostname: String,
    /// Name of the user running this application.
    pub username: String,
    /// Computer UUID for generating bucket names.
    pub computer_id: String,
}

impl Configuration {
    /// Generate a type 5 UUID based on the given values.
    ///
    /// Returns a shortened version of the UUID to minimize storage and reduce
    /// the display width on screen. It can be converted back to a UUID using
    /// `blob_uuid::to_uuid()` if necessary.
    pub fn generate_unique_id(username: &str, hostname: &str) -> String {
        let mut name = String::from(username);
        name.push(':');
        name.push_str(hostname);
        let bytes = name.into_bytes();
        let uuid = Uuid::new_v5(&Uuid::NAMESPACE_URL, &bytes);
        blob_uuid::to_blob(&uuid)
    }
}

impl Default for Configuration {
    fn default() -> Self {
        let username = whoami::username();
        let hostname = whoami::fallible::hostname().unwrap_or("none".into());
        let computer_id = Configuration::generate_unique_id(&username, &hostname);
        Self {
            hostname,
            username,
            computer_id,
        }
    }
}

/// Record counts for the various entities stored in the record repository.
#[derive(Clone, Debug, Default)]
pub struct RecordCounts {
    /// Number of chunks stored in the repository.
    pub chunk: usize,
    /// Number of datasets stored in the repository.
    pub dataset: usize,
    /// Number of files stored in the repository.
    pub file: usize,
    /// Number of packs stored in the repository.
    pub pack: usize,
    /// Number of snapshots stored in the repository.
    pub snapshot: usize,
    /// Number of stores stored in the repository.
    pub store: usize,
    /// Number of trees stored in the repository.
    pub tree: usize,
    /// Number of extended attributes stored in the repository.
    pub xattr: usize,
}

impl fmt::Display for RecordCounts {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "counts(chunks: {}, datasets: {}, files: {}, packs: {}, snapshots: {}, stores: {}, trees: {}, xattrs: {})",
            self.chunk, self.dataset, self.file, self.pack, self.snapshot, self.store, self.tree, self.xattr
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checksum_sort() {
        use std::cmp::Ordering;

        // sha1
        let s1a = Checksum::SHA1(String::from("65ace06cc7f835c497811ea7199968a119eeba4b"));
        let s1b = Checksum::SHA1(String::from("ee76ee57ba2fbc7690a38e125ec6af322288f750"));
        assert_eq!(Ordering::Less, s1a.partial_cmp(&s1b).unwrap());
        assert_eq!(Ordering::Greater, s1b.partial_cmp(&s1a).unwrap());

        // blake3
        let b3a = Checksum::BLAKE3(String::from(
            "a58dd8680234c1f8cc2ef2b325a43733605a7f16f288e072de8eae81fd8d6433",
        ));
        let b3b = Checksum::BLAKE3(String::from(
            "e03c4de56410b680ef69d8f8cfe140c54bb33f295015b40462d260deb9a60b82",
        ));
        assert_eq!(Ordering::Less, b3a.partial_cmp(&b3b).unwrap());
        assert_eq!(Ordering::Greater, b3b.partial_cmp(&b3a).unwrap());
    }

    #[test]
    fn test_checksum_fromstr() {
        let result: Result<Checksum, Error> =
            FromStr::from_str("sha1-e7505beb754bed863e3885f73e3bb6866bdd7f8c");
        assert!(result.is_ok());
        let checksum = result.unwrap();
        assert_eq!(
            checksum,
            Checksum::SHA1(String::from("e7505beb754bed863e3885f73e3bb6866bdd7f8c"))
        );

        let result: Result<Checksum, Error> = FromStr::from_str(
            "blake3-a58dd8680234c1f8cc2ef2b325a43733605a7f16f288e072de8eae81fd8d6433",
        );
        assert!(result.is_ok());
        let checksum = result.unwrap();
        assert_eq!(
            checksum,
            Checksum::BLAKE3(String::from(
                "a58dd8680234c1f8cc2ef2b325a43733605a7f16f288e072de8eae81fd8d6433"
            ))
        );

        let result: Result<Checksum, Error> = FromStr::from_str("foobar");
        assert!(result.is_err());
    }

    #[test]
    fn test_checksum_data() {
        let data = b"Lorem ipsum dolor sit amet, consectetur adipiscing elit.";
        let sha1 = Checksum::sha1_from_bytes(data);
        assert_eq!(
            sha1.to_string(),
            "sha1-e7505beb754bed863e3885f73e3bb6866bdd7f8c"
        );
        let digest = Checksum::blake3_from_bytes(data);
        assert_eq!(
            digest.to_string(),
            "blake3-7d084733ca51ea73bb3ee8f3bfa15abd117d750eb7cbcb463e2a1dadbd3a5536"
        );
    }

    #[test]
    fn test_checksum_file() -> Result<(), io::Error> {
        // use a file larger than the buffer size used for hashing
        let infile = Path::new("../test/fixtures/SekienAkashita.jpg");
        let digest = Checksum::blake3_from_file(&infile)?;
        assert_eq!(
            digest.to_string(),
            "blake3-dba425aa7292ef1209841ab3855a93d4dfa6855658a347f85c502f2c2208cf0f"
        );
        Ok(())
    }

    #[test]
    fn test_generate_unique_id() {
        let uuid = Configuration::generate_unique_id("charlie", "localhost");
        // UUIDv5 = 747267d5-6e70-5711-8a9a-a40c24c1730f
        assert_eq!(uuid, "dHJn1W5wVxGKmqQMJMFzDw");
    }

    #[test]
    fn test_storetype_fromstr() {
        // amazon
        let result = StoreType::from_str("amazon");
        assert!(result.is_ok());
        let stype = result.unwrap();
        assert_eq!(stype, StoreType::AMAZON);
        assert_eq!(stype.to_string(), "amazon");
        // azure
        let result = StoreType::from_str("azure");
        assert!(result.is_ok());
        let stype = result.unwrap();
        assert_eq!(stype, StoreType::AZURE);
        assert_eq!(stype.to_string(), "azure");
        // local
        let result = StoreType::from_str("local");
        assert!(result.is_ok());
        let stype = result.unwrap();
        assert_eq!(stype, StoreType::LOCAL);
        assert_eq!(stype.to_string(), "local");
        // google
        let result = StoreType::from_str("google");
        assert!(result.is_ok());
        let stype = result.unwrap();
        assert_eq!(stype, StoreType::GOOGLE);
        assert_eq!(stype.to_string(), "google");
        // minio
        let result = StoreType::from_str("minio");
        assert!(result.is_ok());
        let stype = result.unwrap();
        assert_eq!(stype, StoreType::MINIO);
        assert_eq!(stype.to_string(), "minio");
        // sftp
        let result = StoreType::from_str("sftp");
        assert!(result.is_ok());
        let stype = result.unwrap();
        assert_eq!(stype, StoreType::SFTP);
        assert_eq!(stype.to_string(), "sftp");
        // not a supported store type
        let result = StoreType::from_str("foobar");
        assert!(result.is_err());
    }

    #[test]
    fn test_tree_entry() {
        let path = Path::new("../test/fixtures/lorem-ipsum.txt");
        let tref = TreeReference::TREE(Checksum::SHA1("cafebabe".to_owned()));
        let mut entry = TreeEntry::new(&path, tref);
        entry = entry.mode(&path);
        entry = entry.owners(&path);
        assert_eq!(entry.reference.to_string(), "tree-sha1-cafebabe");
        assert_eq!(entry.name, "lorem-ipsum.txt");
        #[cfg(target_family = "unix")]
        {
            assert_eq!(entry.mode.unwrap(), 0o100_644);
            assert!(entry.uid.is_some());
            assert!(entry.gid.is_some());
            assert!(entry.user.is_some());
            assert!(entry.group.is_some());
        }
        let formed = entry.to_string();
        // formatted tree entry should look something like this on Unix:
        //
        // "100644 501:20 1545525436 1545525436 tree-sha1-cafebabe lorem-ipsum.txt"
        //
        // formatted tree entry should look something like this on Windows:
        //
        // "40000 0:0 1695515391 1695515391 tree-sha1-cafebabe lorem-ipsum.txt"
        #[cfg(target_family = "unix")]
        assert!(formed.starts_with("100644"));
        #[cfg(target_family = "windows")]
        assert!(formed.starts_with("40000"));
        assert!(formed.contains("sha1-cafebabe"));
        assert!(formed.contains("lorem-ipsum.txt"));
    }

    #[test]
    fn test_tree() {
        let path = Path::new("../test/fixtures/lorem-ipsum.txt");
        let sha1 = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
        let tref = TreeReference::FILE(sha1);
        let entry1 = TreeEntry::new(&path, tref);
        let path = Path::new("../test/fixtures/SekienAkashita.jpg");
        let sha1 = Checksum::SHA1("4c009e44fe5794df0b1f828f2a8c868e66644964".to_owned());
        let tref = TreeReference::FILE(sha1);
        let entry2 = TreeEntry::new(&path, tref);
        let tree = Tree::new(vec![entry1, entry2], 2);
        // with file timestamps, the digest always changes
        assert!(tree.digest.is_sha1());
        let mut entries = tree.entries.iter();
        assert_eq!(entries.next().unwrap().name, "SekienAkashita.jpg");
        assert_eq!(entries.next().unwrap().name, "lorem-ipsum.txt");
        assert!(entries.next().is_none());
        assert_eq!(tree.file_count, 2);
    }

    #[test]
    fn test_treereference_string() {
        // file
        let blake3 = Checksum::BLAKE3(
            "b4cfd55cb7a434f534993bddbb51c8fc04a4142c4bb8a04e11773a1acc26c5ac".to_owned(),
        );
        let tref = TreeReference::FILE(blake3);
        let result = tref.to_string();
        assert_eq!(
            result,
            "file-blake3-b4cfd55cb7a434f534993bddbb51c8fc04a4142c4bb8a04e11773a1acc26c5ac"
        );
        let result = FromStr::from_str(
            "file-blake3-b4cfd55cb7a434f534993bddbb51c8fc04a4142c4bb8a04e11773a1acc26c5ac",
        );
        assert_eq!(tref, result.unwrap());
        // tree
        let sha1 = Checksum::SHA1("4c009e44fe5794df0b1f828f2a8c868e66644964".to_owned());
        let tref = TreeReference::TREE(sha1);
        let result = tref.to_string();
        assert_eq!(result, "tree-sha1-4c009e44fe5794df0b1f828f2a8c868e66644964");
        let result = FromStr::from_str("tree-sha1-4c009e44fe5794df0b1f828f2a8c868e66644964");
        assert_eq!(tref, result.unwrap());
        // link
        let contents = "danger mouse".as_bytes().to_vec();
        let tref = TreeReference::LINK(contents);
        let result = tref.to_string();
        assert_eq!(result, "link-ZGFuZ2VyIG1vdXNl");
        let result = FromStr::from_str("link-ZGFuZ2VyIG1vdXNl");
        assert_eq!(tref, result.unwrap());
        // small
        let contents = "keyboard cat".as_bytes().to_vec();
        let tref = TreeReference::SMALL(contents);
        let result = tref.to_string();
        assert_eq!(result, "small-a2V5Ym9hcmQgY2F0");
        let result = FromStr::from_str("small-a2V5Ym9hcmQgY2F0");
        assert_eq!(tref, result.unwrap());
    }

    #[test]
    fn test_checksum_tree() {
        let tref1 = TreeReference::FILE(Checksum::SHA1("cafebabe".to_owned()));
        let entry1 = TreeEntry {
            name: String::from("madoka.kaname"),
            mode: Some(0o644),
            uid: Some(100),
            gid: Some(100),
            user: Some(String::from("user")),
            group: Some(String::from("group")),
            ctime: Utc.timestamp_opt(0, 0).unwrap(),
            mtime: Utc.timestamp_opt(0, 0).unwrap(),
            reference: tref1,
            xattrs: HashMap::new(),
        };
        let tref2 = TreeReference::FILE(Checksum::SHA1("babecafe".to_owned()));
        let entry2 = TreeEntry {
            name: String::from("homura.akemi"),
            mode: Some(0o644),
            uid: Some(100),
            gid: Some(100),
            user: Some(String::from("user")),
            group: Some(String::from("group")),
            ctime: Utc.timestamp_opt(0, 0).unwrap(),
            mtime: Utc.timestamp_opt(0, 0).unwrap(),
            reference: tref2,
            xattrs: HashMap::new(),
        };
        let tref3 = TreeReference::FILE(Checksum::SHA1("babebabe".to_owned()));
        let entry3 = TreeEntry {
            name: String::from("sayaka.miki"),
            mode: Some(0o644),
            uid: Some(100),
            gid: Some(100),
            user: Some(String::from("user")),
            group: Some(String::from("group")),
            ctime: Utc.timestamp_opt(0, 0).unwrap(),
            mtime: Utc.timestamp_opt(0, 0).unwrap(),
            reference: tref3,
            xattrs: HashMap::new(),
        };
        let tree = Tree::new(vec![entry1, entry2, entry3], 2);
        // would look something like this, if we used "now" instead of unix epoch
        // 644 100:100 1552877320 1552877320 sha1-babecafe homura.akemi
        // 644 100:100 1552877320 1552877320 sha1-cafebabe madoka.kaname
        // 644 100:100 1552877320 1552877320 sha1-babebabe sayaka.miki
        let result = tree.to_string();
        // results should be sorted lexicographically by filename
        assert!(result.find("homura").unwrap() < result.find("madoka").unwrap());
        assert!(result.find("madoka").unwrap() < result.find("sayaka").unwrap());
        // because the timestamps are always 0, sha1 is always the same
        assert_eq!(
            tree.digest.to_string(),
            "sha1-086f6c6ba3e51882c4fd55fc9733316c4ee1b15d"
        );
    }

    #[test]
    fn test_file_counts() {
        let mut counts = FileCounts {
            directories: 65,
            symlinks: 101,
            very_small_files: 1,
            very_large_files: 8,
            file_sizes: HashMap::new(),
        };
        counts.register_file(83864);
        counts.register_file(11273);
        counts.register_file(131072);
        counts.register_file(1048576);
        counts.register_file(8388608);
        counts.register_file(16777216);
        counts.register_file(33554432);
        let actual = counts.total_files();
        assert_eq!(actual, 16);
    }
}
