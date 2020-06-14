//
// Copyright (c) 2020 Nathan Fiedler
//
use chrono::prelude::*;
use failure::{err_msg, Error};
use log::error;
use rusty_ulid::generate_ulid_string;
use sodiumoxide::crypto::pwhash::Salt;
use std::collections::HashMap;
use std::fmt;
use std::fs::{self, FileType};
use std::io;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::SystemTime;
use uuid::Uuid;

pub mod schedule;

///
/// The `Checksum` represents a hash digest for an object, such as a tree,
/// snapshot, file, chunk, or pack file.
///
#[derive(Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub enum Checksum {
    SHA1(String),
    SHA256(String),
}

impl Checksum {
    ///
    /// Compute the SHA1 hash digest of the given data.
    ///
    pub fn sha1_from_bytes(data: &[u8]) -> Checksum {
        use sha1::{Digest, Sha1};
        let mut hasher = Sha1::new();
        hasher.input(data);
        let digest = hasher.result();
        Checksum::SHA1(format!("{:x}", digest))
    }

    ///
    /// Compute the SHA256 hash digest of the given data.
    ///
    pub fn sha256_from_bytes(data: &[u8]) -> Checksum {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.input(data);
        let digest = hasher.result();
        Checksum::SHA256(format!("{:x}", digest))
    }

    ///
    /// Compute the SHA256 hash digest of the given file.
    ///
    pub fn sha256_from_file(infile: &Path) -> io::Result<Checksum> {
        use sha2::{Digest, Sha256};
        let mut file = fs::File::open(infile)?;
        let mut hasher = Sha256::new();
        io::copy(&mut file, &mut hasher)?;
        let digest = hasher.result();
        Ok(Checksum::SHA256(format!("{:x}", digest)))
    }

    /// Return `true` if this checksum is a SHA1.
    pub fn is_sha1(&self) -> bool {
        matches!(*self, Checksum::SHA1(_))
    }

    /// Return `true` if this checksum is a SHA256.
    pub fn is_sha256(&self) -> bool {
        matches!(*self, Checksum::SHA256(_))
    }
}

impl Clone for Checksum {
    fn clone(&self) -> Self {
        match self {
            Checksum::SHA1(sum) => Checksum::SHA1(sum.to_owned()),
            Checksum::SHA256(sum) => Checksum::SHA256(sum.to_owned()),
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
            Checksum::SHA256(hash) => write!(f, "sha256-{}", hash),
        }
    }
}

impl FromStr for Checksum {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.starts_with("sha1-") {
            Ok(Checksum::SHA1(s[5..].to_owned()))
        } else if s.starts_with("sha256-") {
            Ok(Checksum::SHA256(s[7..].to_owned()))
        } else {
            Err(err_msg(format!("not a recognized algorithm: {}", s)))
        }
    }
}

/// Represents a piece of a file, and possibly an entire file.
#[derive(Clone, Debug)]
pub struct Chunk {
    /// The SHA256 checksum of the chunk, with algorithm prefix.
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
    LOCAL,
    MINIO,
    SFTP,
}

impl ToString for StoreType {
    fn to_string(&self) -> String {
        match self {
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
            "local" => Ok(StoreType::LOCAL),
            "minio" => Ok(StoreType::MINIO),
            "sftp" => Ok(StoreType::SFTP),
            _ => Err(err_msg(format!("not a recognized store type: {}", s))),
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
}

// Default pack size is 64mb just because. With a typical ADSL home broadband
// connection a 64mb pack file should take about 5 minutes to upload.
const DEFAULT_PACK_SIZE: u64 = 67_108_864;

impl Dataset {
    /// Construct a Dataset with the given unique (computer) identifier, and
    /// base path of the directory structure to be saved.
    pub fn new(basepath: &Path) -> Dataset {
        let id = generate_ulid_string().to_lowercase();
        let mut workspace = basepath.to_owned();
        workspace.push(".tmp");
        Self {
            id,
            basepath: basepath.to_owned(),
            schedules: vec![],
            workspace,
            pack_size: DEFAULT_PACK_SIZE,
            stores: vec![],
        }
    }

    /// Add the given store identifier to the dataset.
    pub fn add_store(mut self, store: &str) -> Self {
        self.stores.push(store.to_owned());
        self
    }

    /// Add the given schedule to the dataset.
    pub fn add_schedule(mut self, schedule: schedule::Schedule) -> Self {
        self.schedules.push(schedule);
        self
    }

    /// Set the pack size for the dataset.
    pub fn pack_size(mut self, pack_size: u64) -> Self {
        self.pack_size = pack_size;
        self
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
        }
    }
}

impl fmt::Display for Dataset {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "dataset-{}", self.id)
    }
}

/// Tree entry type, such as a file or directory.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum EntryType {
    /// Anything that is not a directory or symlink.
    FILE,
    /// Represents a directory.
    DIR,
    /// Represents a symbolic link.
    LINK,
    /// Error occurred while processing the entry.
    ERROR,
}

impl EntryType {
    /// Return `true` if this entry is for a file.
    pub fn is_file(self) -> bool {
        matches!(self, EntryType::FILE)
    }

    /// Return `true` if this entry is for a directory.
    pub fn is_dir(self) -> bool {
        matches!(self, EntryType::DIR)
    }

    /// Return `true` if this entry is for a symbolic link.
    pub fn is_link(self) -> bool {
        matches!(self, EntryType::LINK)
    }
}

impl From<FileType> for EntryType {
    fn from(fstype: FileType) -> Self {
        if fstype.is_dir() {
            EntryType::DIR
        } else if fstype.is_symlink() {
            EntryType::LINK
        } else {
            // default to file type for everything else
            EntryType::FILE
        }
    }
}

///
/// A `TreeReference` represents the "value" for a tree entry, either the
/// checksum of a tree object, or an individual file, or a symbolic link. The
/// symbolic link value should be base64 encoded for the purpose of character
/// encoding safety.
///
#[derive(Debug, Eq, PartialEq)]
pub enum TreeReference {
    LINK(String),
    TREE(Checksum),
    FILE(Checksum),
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

    /// Return the checksum for this reference, if possible.
    pub fn checksum(&self) -> Option<Checksum> {
        match self {
            TreeReference::TREE(sum) => Some(sum.clone()),
            TreeReference::FILE(sum) => Some(sum.clone()),
            _ => None,
        }
    }

    /// Return the base64 encoded value for this symlink, if possible.
    pub fn symlink(&self) -> Option<String> {
        match self {
            TreeReference::LINK(link) => Some(link.clone()),
            _ => None,
        }
    }
}

impl fmt::Display for TreeReference {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TreeReference::LINK(value) => write!(f, "link-{}", value),
            TreeReference::TREE(digest) => write!(f, "tree-{}", digest),
            TreeReference::FILE(digest) => write!(f, "file-{}", digest),
        }
    }
}

impl FromStr for TreeReference {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.starts_with("link-") {
            Ok(TreeReference::LINK(s[5..].to_owned()))
        } else if s.starts_with("tree-") {
            let digest: Result<Checksum, Error> = FromStr::from_str(&s[5..]);
            Ok(TreeReference::TREE(digest.expect("invalid tree SHA1")))
        } else if s.starts_with("file-") {
            let digest: Result<Checksum, Error> = FromStr::from_str(&s[7..]);
            Ok(TreeReference::FILE(digest.expect("invalid file SHA256")))
        } else {
            Err(err_msg(format!("not a recognized reference: {}", s)))
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
#[derive(Debug)]
pub struct TreeEntry {
    /// Name of the file, directory, or symbolic link.
    pub name: String,
    /// Basic type of the entry, e.g. file or directory.
    pub fstype: EntryType,
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
    /// extended attribute, and the value is the checksum for the value
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
        let fstype = match metadata.as_ref() {
            Ok(attr) => EntryType::from(attr.file_type()),
            Err(err) => {
                error!("error getting metadata for {:?}: {}", path, err);
                EntryType::ERROR
            }
        };
        let mtime = match metadata.as_ref() {
            Ok(attr) => attr.modified().unwrap_or_else(|_| SystemTime::UNIX_EPOCH),
            Err(_) => SystemTime::UNIX_EPOCH,
        };
        // creation time is not available on all platforms, and we are only
        // using it to record a value in the database
        let ctime = match metadata.as_ref() {
            Ok(attr) => attr.created().unwrap_or_else(|_| SystemTime::UNIX_EPOCH),
            Err(_) => SystemTime::UNIX_EPOCH,
        };
        Self {
            name,
            fstype,
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
        } else if self.fstype == EntryType::DIR {
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

/// A set of file system entries, such as files, directories, symbolic links.
#[derive(Debug)]
pub struct Tree {
    /// Set of entries making up this tree.
    pub entries: Vec<TreeEntry>,
    /// The number of files contained within this tree and its subtrees.
    pub file_count: u32,
}

impl Tree {
    ///
    /// Create an instance of Tree that takes ownership of the given entries.
    /// The entries will be sorted by name, hence must be mutable.
    ///
    pub fn new(mut entries: Vec<TreeEntry>, file_count: u32) -> Self {
        entries.sort_unstable_by(|a, b| a.name.cmp(&b.name));
        Self {
            entries,
            file_count,
        }
    }

    ///
    /// Calculate the SHA1 digest for the tree.
    ///
    pub fn checksum(&self) -> Checksum {
        let formed = self.to_string();
        Checksum::sha1_from_bytes(formed.as_bytes())
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
#[derive(Debug)]
pub struct File {
    /// Digest of file at time of snapshot.
    pub digest: Checksum,
    /// Length of the file in bytes.
    pub length: u64,
    /// The set of the chunks contained in this file. There may be many of these
    /// for large files, so they are represented compactly. The first field is
    /// the byte offset of the chunk within the saved file.
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
    /// Total number of files contained in this snapshot.
    pub file_count: u32,
    /// Digest of the root tree for this snapshot.
    pub tree: Checksum,
}

impl Snapshot {
    ///
    /// Construct a new `Snapshot` for the given tree, and optional parent.
    ///
    pub fn new(parent: Option<Checksum>, tree: Checksum, file_count: u32) -> Self {
        let start_time = Utc::now();
        let mut snapshot = Self {
            digest: Checksum::SHA1(String::from("sha1-cafebabe")),
            parent,
            start_time,
            end_time: None,
            file_count,
            tree,
        };
        // Need to compute a checksum and save that as the "key" for this
        // snapshot, cannot compute the checksum later because the object is
        // mutable (e.g. end time).
        let formed = snapshot.to_string();
        snapshot.digest = Checksum::sha1_from_bytes(formed.as_bytes());
        snapshot
    }

    /// Add the end_time property.
    pub fn end_time(mut self, end_time: DateTime<Utc>) -> Self {
        self.end_time = Some(end_time);
        self
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
        write!(
            f,
            "tree {}\nparent {}\nnumFiles {}\nstartTime {}",
            self.tree, parent, self.file_count, start_time
        )
    }
}

///
/// Remote coordinates for a pack file, naming the store, bucket, and object by
/// which the pack file can be retrieved.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PackLocation {
    /// ULID of the pack store.
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

/// Type for database record of saved packs.
#[derive(Clone, Debug)]
pub struct Pack {
    /// Digest of pack file.
    pub digest: Checksum,
    /// List of remote pack coordinates.
    pub locations: Vec<PackLocation>,
    /// Date/time of successful upload, for conflict resolution.
    pub upload_time: DateTime<Utc>,
    /// Salt used to encrypt this pack.
    pub crypto_salt: Option<Salt>,
}

impl Pack {
    /// Create a new Pack record using the given information. Assumes the
    /// upload time is the current time.
    pub fn new(digest: Checksum, coords: Vec<PackLocation>) -> Self {
        Self {
            digest,
            locations: coords,
            upload_time: Utc::now(),
            crypto_salt: None,
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
        let hostname = whoami::hostname();
        let computer_id = Configuration::generate_unique_id(&username, &hostname);
        Self {
            hostname,
            username,
            computer_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checksum_sort() {
        use std::cmp::Ordering;
        let c1a = Checksum::SHA1(String::from("65ace06cc7f835c497811ea7199968a119eeba4b"));
        let c1b = Checksum::SHA1(String::from("ee76ee57ba2fbc7690a38e125ec6af322288f750"));
        assert_eq!(Ordering::Less, c1a.partial_cmp(&c1b).unwrap());
        assert_eq!(Ordering::Greater, c1b.partial_cmp(&c1a).unwrap());
        let c2a = Checksum::SHA256(String::from(
            "a58dd8680234c1f8cc2ef2b325a43733605a7f16f288e072de8eae81fd8d6433",
        ));
        let c2b = Checksum::SHA256(String::from(
            "e03c4de56410b680ef69d8f8cfe140c54bb33f295015b40462d260deb9a60b82",
        ));
        assert_eq!(Ordering::Less, c2a.partial_cmp(&c2b).unwrap());
        assert_eq!(Ordering::Greater, c2b.partial_cmp(&c2a).unwrap());
        // all SHA1 values are always less than any SHA256 value
        assert_eq!(Ordering::Less, c1b.partial_cmp(&c2a).unwrap());
        assert_eq!(Ordering::Greater, c2a.partial_cmp(&c1b).unwrap());
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
            "sha256-a58dd8680234c1f8cc2ef2b325a43733605a7f16f288e072de8eae81fd8d6433",
        );
        assert!(result.is_ok());
        let checksum = result.unwrap();
        assert_eq!(
            checksum,
            Checksum::SHA256(String::from(
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
        let sha256 = Checksum::sha256_from_bytes(data);
        assert_eq!(
            sha256.to_string(),
            "sha256-a58dd8680234c1f8cc2ef2b325a43733605a7f16f288e072de8eae81fd8d6433"
        );
    }

    #[test]
    fn test_checksum_file() -> Result<(), io::Error> {
        // use a file larger than the buffer size used for hashing
        let infile = Path::new("./tests/fixtures/SekienAkashita.jpg");
        let sha256 = Checksum::sha256_from_file(&infile)?;
        assert_eq!(
            sha256.to_string(),
            "sha256-d9e749d9367fc908876749d6502eb212fee88c9a94892fb07da5ef3ba8bc39ed"
        );
        Ok(())
    }

    #[test]
    fn test_storetype_fromstr() {
        let result = StoreType::from_str("local");
        assert!(result.is_ok());
        let stype = result.unwrap();
        assert_eq!(stype, StoreType::LOCAL);
        assert_eq!(stype.to_string(), "local");
        let result = StoreType::from_str("minio");
        assert!(result.is_ok());
        let stype = result.unwrap();
        assert_eq!(stype, StoreType::MINIO);
        assert_eq!(stype.to_string(), "minio");
        let result = StoreType::from_str("sftp");
        assert!(result.is_ok());
        let stype = result.unwrap();
        assert_eq!(stype, StoreType::SFTP);
        assert_eq!(stype.to_string(), "sftp");
        let result = StoreType::from_str("foobar");
        assert!(result.is_err());
    }

    #[test]
    fn test_tree_entry() {
        let path = Path::new("./tests/fixtures/lorem-ipsum.txt");
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
        // formatted tree entry should look something like this:
        // "100644 501:20 1545525436 1545525436 sha1-cafebabe lorem-ipsum.txt"
        assert!(formed.contains("100644"));
        assert!(formed.contains("sha1-cafebabe"));
        assert!(formed.contains("lorem-ipsum.txt"));
    }

    #[test]
    fn test_tree() {
        let path = Path::new("./tests/fixtures/lorem-ipsum.txt");
        let sha1 = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
        let tref = TreeReference::FILE(sha1);
        let entry1 = TreeEntry::new(&path, tref);
        let path = Path::new("./tests/fixtures/SekienAkashita.jpg");
        let sha1 = Checksum::SHA1("4c009e44fe5794df0b1f828f2a8c868e66644964".to_owned());
        let tref = TreeReference::FILE(sha1);
        let entry2 = TreeEntry::new(&path, tref);
        let tree = Tree::new(vec![entry1, entry2], 2);
        let sha1 = tree.checksum();
        // with file timestamps, the digest always changes
        assert!(sha1.is_sha1());
        let mut entries = tree.entries.iter();
        assert_eq!(entries.next().unwrap().name, "SekienAkashita.jpg");
        assert_eq!(entries.next().unwrap().name, "lorem-ipsum.txt");
        assert!(entries.next().is_none());
        assert_eq!(tree.file_count, 2);
    }
}
