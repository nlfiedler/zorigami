//
// Copyright (c) 2019 Nathan Fiedler
//
use failure::{err_msg, Error};
use fastcdc;
use gpgme;
use memmap::MmapOptions;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::fs::{self, File, FileType};
use std::io;
use std::io::prelude::*;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::SystemTime;
use tar::{Archive, Builder, Header};
use ulid::Ulid;
use uuid::Uuid;

///
/// Generate a type 5 UUID based on the given values.
///
pub fn generate_unique_id(username: &str, hostname: &str) -> String {
    let mut name = String::from(username);
    name.push(':');
    name.push_str(hostname);
    let bytes = name.into_bytes();
    Uuid::new_v5(&Uuid::NAMESPACE_URL, &bytes)
        .to_hyphenated()
        .to_string()
}

///
/// Generate a suitable bucket name, using a ULID and the given UUID.
///
pub fn generate_bucket_name(unique_id: &str) -> String {
    let shorter = String::from(unique_id).replace("-", "");
    let mut ulid = Ulid::new().to_string();
    ulid.push_str(&shorter);
    ulid.to_lowercase()
}

///
/// The `Checksum` represents a hash digest for an object, such as a tree,
/// snapshot, file, chunk, or pack file.
///
#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, PartialOrd, Hash)]
pub enum Checksum {
    SHA1(String),
    SHA256(String),
}

impl Checksum {
    /// Return `true` if this checksum is a SHA1.
    pub fn is_sha1(&self) -> bool {
        match *self {
            Checksum::SHA1(_) => true,
            _ => false,
        }
    }

    /// Return `true` if this checksum is a SHA256.
    pub fn is_sha256(&self) -> bool {
        match *self {
            Checksum::SHA256(_) => true,
            _ => false,
        }
    }
}

impl Clone for Checksum {
    fn clone(&self) -> Self {
        match self {
            Checksum::SHA1(sum) => Checksum::SHA1(String::from(sum.as_ref())),
            Checksum::SHA256(sum) => Checksum::SHA256(String::from(sum.as_ref())),
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

///
/// Compute the SHA1 hash digest of the given data.
///
pub fn checksum_data_sha1(data: &[u8]) -> Checksum {
    use sha1::{Digest, Sha1};
    let mut hasher = Sha1::new();
    hasher.input(data);
    let digest = hasher.result();
    Checksum::SHA1(format!("{:x}", digest))
}

///
/// Compute the SHA256 hash digest of the given data.
///
pub fn checksum_data_sha256(data: &[u8]) -> Checksum {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.input(data);
    let digest = hasher.result();
    Checksum::SHA256(format!("{:x}", digest))
}

///
/// Compute the SHA256 hash digest of the given file.
///
pub fn checksum_file(infile: &Path) -> io::Result<Checksum> {
    use sha2::{Digest, Sha256};
    let mut file = File::open(infile)?;
    let mut hasher = Sha256::new();
    io::copy(&mut file, &mut hasher)?;
    let digest = hasher.result();
    Ok(Checksum::SHA256(format!("{:x}", digest)))
}

/// Some chunk of a file.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Chunk {
    /// The SHA256 checksum of the chunk, with algo prefix.
    #[serde(skip)]
    pub digest: Checksum,
    /// The byte offset of this chunk within the file. This is _not_ saved to
    /// the database since an identical chunk may appear in different files at
    /// different offsets.
    #[serde(skip)]
    pub offset: usize,
    /// The byte length of this chunk.
    #[serde(rename = "le")]
    pub length: usize,
    /// Path of the file from which the chunk is taken.
    #[serde(skip)]
    pub filepath: Option<PathBuf>,
    /// Digest of packfile this chunk is stored within.
    #[serde(rename = "pf")]
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

///
/// Find the chunk boundaries within the given file, using the FastCDC
/// algorithm. The given `size` is the desired average size in bytes for the
/// chunks, but they may be between half and twice that size.
///
pub fn find_file_chunks(infile: &Path, size: u64) -> io::Result<Vec<Chunk>> {
    let file = File::open(infile)?;
    let mmap = unsafe { MmapOptions::new().map(&file).expect("cannot create mmap?") };
    let avg_size = size as usize;
    let min_size = avg_size / 2;
    let max_size = avg_size * 2;
    let chunker = fastcdc::FastCDC::new(&mmap[..], min_size, avg_size, max_size);
    let mut results = Vec::new();
    for entry in chunker {
        let end = entry.offset + entry.length;
        let chksum = checksum_data_sha256(&mmap[entry.offset..end]);
        let mut chunk = Chunk::new(chksum, entry.offset, entry.length);
        chunk = chunk.filepath(infile);
        results.push(chunk);
    }
    Ok(results)
}

///
/// Write a sequence of chunks into a pack file, returning the SHA256 of the
/// pack file. The chunks will be written in the order they appear in the array.
///
pub fn pack_chunks(chunks: &[Chunk], outfile: &Path) -> io::Result<Checksum> {
    let file = File::create(outfile)?;
    let mut builder = Builder::new(file);
    for chunk in chunks {
        let fp = chunk.filepath.as_ref().expect("chunk requires a filepath");
        let mut infile = File::open(fp)?;
        infile.seek(io::SeekFrom::Start(chunk.offset as u64))?;
        let handle = infile.take(chunk.length as u64);
        let mut header = Header::new_gnu();
        header.set_size(chunk.length as u64);
        // set the date so the tar file produces the same results for the same
        // inputs every time; the date for chunks is completely irrelevant
        header.set_mtime(0);
        header.set_cksum();
        let filename = chunk.digest.to_string();
        builder.append_data(&mut header, filename, handle)?;
    }
    let _output = builder.into_inner()?;
    checksum_file(outfile)
}

///
/// Extract the chunks from the given pack file, writing them to the output
/// directory, with the names being the original SHA256 of the chunk (with a
/// "sha256-" prefix).
///
pub fn unpack_chunks(infile: &Path, outdir: &Path) -> io::Result<Vec<String>> {
    fs::create_dir_all(outdir)?;
    let mut results = Vec::new();
    let file = File::open(infile)?;
    let mut ar = Archive::new(file);
    for entry in ar.entries()? {
        let mut file = entry?;
        let fp = file.path()?;
        // we know the names are valid UTF-8, we created them
        results.push(String::from(fp.to_str().unwrap()));
        file.unpack_in(outdir)?;
    }
    Ok(results)
}

///
/// Copy the chunk files to the given output location, deleting the chunks as
/// each one is copied.
///
pub fn assemble_chunks(chunks: &[&Path], outfile: &Path) -> io::Result<()> {
    let mut file = File::create(outfile)?;
    for infile in chunks {
        let mut cfile = File::open(infile)?;
        io::copy(&mut cfile, &mut file)?;
        fs::remove_file(infile)?;
    }
    Ok(())
}

///
/// Encrypt the given file using the OpenPGP (RFC 4880) format, with the
/// provided passphrase as the seed for the encryption key.
///
pub fn encrypt_file(passphrase: &str, infile: &Path, outfile: &Path) -> Result<(), gpgme::Error> {
    let mut ctx = gpgme::Context::from_protocol(gpgme::Protocol::OpenPgp)?;
    // need to set pinentry mode to avoid user interaction
    // n.b. this setting is cached in memory somehow
    ctx.set_pinentry_mode(gpgme::PinentryMode::Loopback)?;
    let recipients = Vec::new();
    let mut input = File::open(infile)?;
    let mut cipher = File::create(outfile)?;
    // need a passphrase provider otherwise nothing is output;
    // n.b. this and/or the passphrase is cached in memory somehow
    ctx.with_passphrase_provider(
        |_: gpgme::PassphraseRequest, out: &mut Write| {
            out.write_all(passphrase.as_bytes())?;
            Ok(())
        },
        |ctx| match ctx.encrypt(&recipients, &mut input, &mut cipher) {
            Ok(v) => v,
            Err(err) => panic!("operation failed {}", err),
        },
    );
    Ok(())
}

///
/// Decrypt the OpenPGP-encrypted file using the given passphrase.
///
pub fn decrypt_file(passphrase: &str, infile: &Path, outfile: &Path) -> Result<(), gpgme::Error> {
    let mut ctx = gpgme::Context::from_protocol(gpgme::Protocol::OpenPgp)?;
    // need to set pinentry mode to avoid user interaction
    // n.b. this setting is cached in memory somehow
    ctx.set_pinentry_mode(gpgme::PinentryMode::Loopback)?;
    let mut input = File::open(infile)?;
    let mut plain = File::create(outfile)?;
    // need a passphrase provider otherwise nothing is output;
    // n.b. this and/or the passphrase is cached in memory somehow
    ctx.with_passphrase_provider(
        |_: gpgme::PassphraseRequest, out: &mut Write| {
            out.write_all(passphrase.as_bytes())?;
            Ok(())
        },
        |ctx| match ctx.decrypt(&mut input, &mut plain) {
            Ok(v) => v,
            Err(err) => panic!("operation failed {}", err),
        },
    );
    Ok(())
}

#[derive(Serialize, Deserialize, Debug)]
pub enum EntryType {
    /// Anything that is not a directory or symlink.
    FILE,
    /// Definitely a directory.
    DIR,
    /// Definitely a symbolic link.
    SYMLINK,
}

impl EntryType {
    /// Return `true` if this entry is for a file.
    pub fn is_file(&self) -> bool {
        match *self {
            EntryType::FILE => true,
            _ => false,
        }
    }

    /// Return `true` if this entry is for a directory.
    pub fn is_dir(&self) -> bool {
        match *self {
            EntryType::DIR => true,
            _ => false,
        }
    }

    /// Return `true` if this entry is for a symbolic link.
    pub fn is_link(&self) -> bool {
        match *self {
            EntryType::SYMLINK => true,
            _ => false,
        }
    }
}

impl From<FileType> for EntryType {
    fn from(fstype: FileType) -> Self {
        if fstype.is_dir() {
            EntryType::DIR
        } else if fstype.is_symlink() {
            EntryType::SYMLINK
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
#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
pub enum TreeReference {
    LINK(String),
    TREE(Checksum),
    FILE(Checksum),
}

impl TreeReference {
    /// Return `true` if this reference is for a symbolic link.
    pub fn is_link(&self) -> bool {
        match *self {
            TreeReference::LINK(_) => true,
            _ => false,
        }
    }

    /// Return `true` if this reference is for a tree.
    pub fn is_tree(&self) -> bool {
        match *self {
            TreeReference::TREE(_) => true,
            _ => false,
        }
    }

    /// Return `true` if this reference is for a file.
    pub fn is_file(&self) -> bool {
        match *self {
            TreeReference::FILE(_) => true,
            _ => false,
        }
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

///
/// A `TreeEntry` represents a file, directory, or symbolic link within a tree.
///
#[derive(Serialize, Deserialize, Debug)]
pub struct TreeEntry {
    /// Name of the file, directory, or symbolic link.
    #[serde(rename = "nm")]
    pub name: String,
    /// Basic type of the entry, e.g. file or directory.
    #[serde(rename = "ty")]
    pub fstype: EntryType,
    /// Unix file mode of the entry.
    #[serde(rename = "mo")]
    pub mode: Option<u32>,
    /// Unix user identifier
    #[serde(rename = "ui")]
    pub uid: Option<u32>,
    /// Name of the owning user.
    #[serde(rename = "us")]
    pub user: Option<String>,
    /// Unix group identifier
    #[serde(rename = "gi")]
    pub gid: Option<u32>,
    /// Name of the owning group.
    #[serde(rename = "gr")]
    pub group: Option<String>,
    /// Created time.
    #[serde(rename = "ct")]
    pub ctime: SystemTime,
    /// Modified time.
    #[serde(rename = "mt")]
    pub mtime: SystemTime,
    /// Reference to the entry itself.
    #[serde(rename = "tr")]
    pub reference: TreeReference,
    /// Set of extended file attributes, if any. The key is the name of the
    /// extended attribute, and the value is the checksum for the value
    /// already recorded. Each unique value is meant to be stored once.
    #[serde(rename = "xa")]
    pub xattrs: HashMap<String, Checksum>,
}

impl TreeEntry {
    ///
    /// Create an instance of `TreeEntry` based on the given path.
    ///
    pub fn new(path: &Path, reference: TreeReference) -> Result<Self, Error> {
        let attr = fs::symlink_metadata(path)?;
        let name = path
            .file_name()
            .ok_or_else(|| err_msg("invalid file path"))?;
        Ok(Self {
            name: name.to_str().unwrap().to_owned(),
            fstype: EntryType::from(attr.file_type()),
            mode: None,
            uid: None,
            gid: None,
            user: None,
            group: None,
            ctime: attr.created()?,
            mtime: attr.modified()?,
            reference,
            xattrs: HashMap::new(),
        })
    }

    ///
    /// Set the `mode` property to either the Unix file mode or the
    /// Windows attributes value, both of which are u32 values.
    ///
    pub fn mode(mut self, path: &Path) -> Self {
        // Either mode or file attributes will be sufficient to cover all
        // supported systems; the "permissions" field only has one bit,
        // read-only, and that is already in mode and file attributes.
        #[cfg(target_family = "unix")]
        {
            use std::os::unix::fs::MetadataExt;
            let result = fs::symlink_metadata(path);
            if let Ok(meta) = result {
                self.mode = Some(meta.mode());
            }
        }
        #[cfg(target_family = "windows")]
        {
            use std::os::windows::prelude::*;
            let result = fs::symlink_metadata(path);
            if let Ok(meta) = result {
                self.mode = Some(metadata.file_attributes());
            }
        }
        self
    }

    ///
    /// Set the user and group ownership of the given path. At present, only
    /// Unix systems have this information.
    ///
    pub fn owners(mut self, path: &Path) -> Self {
        #[cfg(target_family = "unix")]
        {
            use libc;
            use std::ffi::CStr;
            use std::os::unix::fs::MetadataExt;
            let result = fs::symlink_metadata(path);
            if let Ok(meta) = result {
                self.uid = Some(meta.uid());
                self.gid = Some(meta.gid());
                // get the user name
                let c_str = unsafe {
                    let passwd = libc::getpwuid(meta.uid());
                    let c_buf = (*passwd).pw_name;
                    CStr::from_ptr(c_buf)
                };
                let str_slice: &str = c_str.to_str().unwrap();
                self.user = Some(str_slice.to_owned());
                // get the group name
                let c_str = unsafe {
                    let group = libc::getgrgid(meta.gid());
                    let c_buf = (*group).gr_name;
                    CStr::from_ptr(c_buf)
                };
                let str_slice: &str = c_str.to_str().unwrap();
                self.group = Some(str_slice.to_owned());
            }
        }
        self
    }
}

impl fmt::Display for TreeEntry {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let ctime = self
            .ctime
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let mtime = self
            .mtime
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        // Format in a manner similar to git tree entries; this forms part of
        // the digest value for the overall tree, so it should remain relatively
        // stable over time.
        write!(
            f,
            "{:o} {}:{} {} {} {} {}",
            self.mode.unwrap_or(0),
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
/// A Tree represents a set of file system entries, including files,
/// directories, symbolic links, etc.
///
#[derive(Serialize, Deserialize, Debug)]
pub struct Tree {
    /// Set of entries making up this tree.
    #[serde(rename = "en")]
    pub entries: Vec<TreeEntry>,
    /// The number of files contained within this tree and its subtrees.
    #[serde(skip)]
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
        checksum_data_sha1(formed.as_bytes())
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
/// A `Snapshot` represents a single backup, either in progress or completed.
/// It references a possible parent snapshot, and a tree representing the files
/// contained in the snapshot.
///
#[derive(Serialize, Deserialize, Debug)]
pub struct Snapshot {
    /// Digest of the parent snapshot, if any.
    #[serde(rename = "pa")]
    pub parent: Option<Checksum>,
    /// Time when the snapshot was first created.
    #[serde(rename = "st")]
    pub start_time: SystemTime,
    /// Time when the snapshot completed its upload. Will be `None` until
    /// the backup has completed.
    #[serde(rename = "et")]
    pub end_time: Option<SystemTime>,
    /// Total number of files contained in this snapshot.
    #[serde(rename = "fc")]
    pub file_count: u32,
    /// Digest of the root tree for this snapshot.
    #[serde(rename = "tr")]
    pub tree: Checksum,
}

impl Snapshot {
    ///
    /// Construct a new `Snapshot` for the given tree, and optional parent.
    /// Use the builder-style functions to set the other fields.
    ///
    pub fn new(parent: Option<Checksum>, tree: Checksum) -> Self {
        Self {
            parent,
            start_time: SystemTime::UNIX_EPOCH,
            end_time: None,
            file_count: 0,
            tree,
        }
    }

    /// Add the start_time property.
    pub fn start_time(mut self, start_time: SystemTime) -> Self {
        self.start_time = start_time;
        self
    }

    /// Add the end_time property.
    pub fn end_time(mut self, end_time: SystemTime) -> Self {
        self.end_time = Some(end_time);
        self
    }

    /// Add the file_count property.
    pub fn file_count(mut self, file_count: u32) -> Self {
        self.file_count = file_count;
        self
    }

    ///
    /// Calculate the SHA1 digest for the snapshot.
    ///
    pub fn checksum(&self) -> Checksum {
        let formed = self.to_string();
        checksum_data_sha1(formed.as_bytes())
    }
}

/// A SHA1 of all zeroes.
pub static NULL_SHA1: &str = "sha1-0000000000000000000000000000000000000000";

impl fmt::Display for Snapshot {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let stime = self
            .start_time
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let etime = self
            .end_time
            .unwrap_or(std::time::UNIX_EPOCH)
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        // Format in a manner similar to git commit entries; this forms part of
        // the digest value for the snapshot, so it should remain relatively
        // stable over time.
        let parent = if self.parent.is_none() {
            NULL_SHA1.to_string()
        } else {
            self.parent.as_ref().unwrap().to_string()
        };
        write!(
            f,
            "tree {}\nparent {}\nnumFiles {}\nstartTime {}\nendTime {}",
            self.tree, parent, self.file_count, stime, etime
        )
    }
}

/// Type for database record of saved files.
#[derive(Serialize, Deserialize, Debug)]
pub struct SavedFile {
    /// Digest of file at time of snapshot.
    #[serde(skip)]
    pub digest: Checksum,
    /// Length of the file in bytes, or zero if `changed`.
    #[serde(rename = "len")]
    pub length: u64,
    /// The set of the chunks contained in this file; will be empty if the
    /// `changed` property is some value. There may be many of these for large
    /// files, so they are represented compactly.
    #[serde(rename = "cnx")]
    pub chunks: Vec<(u64, Checksum)>,
    /// Digest of file at time of backup, if different from `digest`.
    #[serde(rename = "new")]
    pub changed: Option<Checksum>,
}

impl SavedFile {
    /// Create a new SavedFile to represent the given file and its chunks.
    pub fn new(digest: Checksum, length: u64, chunks: Vec<(u64, Checksum)>) -> Self {
        Self {
            digest,
            length,
            chunks,
            changed: None,
        }
    }

    /// Set the changed property and reset certain other fields.
    pub fn set_changed(mut self, digest: Checksum) -> Self {
        self.changed = Some(digest);
        self.length = 0;
        self.chunks.clear();
        self
    }
}

///
/// Remote coordinates for a pack file, naming the store, bucket, and object by
/// which the pack file can be retrieved.
///
#[derive(Serialize, Deserialize, Debug)]
pub struct PackLocation {
    /// ULID of the pack store.
    #[serde(rename = "st")]
    pub store: String,
    /// Remote bucket name.
    #[serde(rename = "bu")]
    pub bucket: String,
    /// Remote object name.
    #[serde(rename = "ob")]
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
#[derive(Serialize, Deserialize, Debug)]
pub struct SavedPack {
    /// Digest of pack file.
    #[serde(skip)]
    pub digest: Checksum,
    /// List of remote pack coordinates.
    #[serde(rename = "pc")]
    pub locations: Vec<PackLocation>,
    /// Date/time of successful upload, for conflict resolution.
    #[serde(rename = "tm")]
    pub upload_time: SystemTime,
}

impl SavedPack {
    /// Create a new SavedPack record using the given information. Assumes the
    /// upload time is the current time.
    pub fn new(digest: Checksum, coords: Vec<PackLocation>) -> Self {
        Self {
            digest,
            locations: coords,
            upload_time: SystemTime::now(),
        }
    }
}

/// Represents a directory tree that will be backed up according to a schedule,
/// with pack files saved to a particular local or remote store.
#[derive(Serialize, Deserialize, Debug)]
pub struct Dataset {
    /// Unique identifier of this dataset for persisting to database.
    #[serde(skip)]
    pub key: String,
    /// computer UUID for generating bucket names
    #[serde(rename = "id")]
    pub computer_id: String,
    /// local base path of dataset to be saved
    #[serde(rename = "bp")]
    pub basepath: PathBuf,
    /// latest snapshot reference, if any
    #[serde(rename = "ls")]
    pub latest_snapshot: Option<Checksum>,
    /// path for temporary pack building
    #[serde(rename = "ws")]
    pub workspace: PathBuf,
    /// target size in bytes for pack files
    #[serde(rename = "ps")]
    pub pack_size: u64,
    /// names of the stores to contain pack files
    #[serde(rename = "st")]
    pub stores: Vec<String>,
}

// Default pack size is 64mb just because. With a typical ADSL home broadband
// connection a 64mb pack file should take about 5 minutes to upload.
const DEFAULT_PACK_SIZE: u64 = 67_108_864;

impl Dataset {
    ///
    /// Construct a Dataset with the given unique (computer) identifier, base
    /// path of the directory structure to be saved, and the identifier for the
    /// store that will receive the pack files.
    ///
    pub fn new(computer_id: &str, basepath: &Path, store: &str) -> Dataset {
        let key = Ulid::new().to_string().to_lowercase();
        let mut workspace = basepath.to_owned();
        workspace.push(".tmp");
        Self {
            key,
            computer_id: computer_id.to_owned(),
            basepath: basepath.to_owned(),
            latest_snapshot: None,
            workspace,
            pack_size: DEFAULT_PACK_SIZE,
            stores: vec![store.to_owned()],
        }
    }

    /// Add the named store to the dataset.
    pub fn add_store(mut self, store: &str) -> Self {
        self.stores.push(store.to_owned());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_cbor;
    use tempfile::tempdir;

    #[test]
    fn test_generate_unique_id() {
        let uuid = generate_unique_id("charlie", "localhost");
        assert_eq!(uuid, "747267d5-6e70-5711-8a9a-a40c24c1730f");
    }

    #[test]
    fn test_generate_bucket_name() {
        let uuid = generate_unique_id("charlie", "localhost");
        let bucket = generate_bucket_name(&uuid);
        // Ensure the generated name is safe for the "cloud", which so far means
        // Google Cloud Storage and Amazon Glacier. It needs to be reasonably
        // short, must consist only of lowercase letters or digits.
        assert_eq!(bucket.len(), 58, "bucket name is 58 characters");
        for c in bucket.chars() {
            assert!(c.is_ascii_alphanumeric());
            if c.is_ascii_alphabetic() {
                assert!(c.is_ascii_lowercase());
            }
        }
    }

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
        let result = Checksum::from_str("sha1-e7505beb754bed863e3885f73e3bb6866bdd7f8c");
        assert!(result.is_ok());
        let checksum = result.unwrap();
        assert_eq!(
            checksum,
            Checksum::SHA1(String::from("e7505beb754bed863e3885f73e3bb6866bdd7f8c"))
        );
        let result = Checksum::from_str(
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
        let result = Checksum::from_str("foobar");
        assert!(result.is_err());
    }

    #[test]
    fn test_checksum_data() {
        let data = b"Lorem ipsum dolor sit amet, consectetur adipiscing elit.";
        let sha1 = checksum_data_sha1(data);
        assert_eq!(
            sha1.to_string(),
            "sha1-e7505beb754bed863e3885f73e3bb6866bdd7f8c"
        );
        let sha256 = checksum_data_sha256(data);
        assert_eq!(
            sha256.to_string(),
            "sha256-a58dd8680234c1f8cc2ef2b325a43733605a7f16f288e072de8eae81fd8d6433"
        );
    }

    #[test]
    fn test_checksum_file() -> Result<(), io::Error> {
        // use a file larger than the buffer size used for hashing
        let infile = Path::new("./tests/fixtures/SekienAkashita.jpg");
        let sha256 = checksum_file(&infile)?;
        assert_eq!(
            sha256.to_string(),
            "sha256-d9e749d9367fc908876749d6502eb212fee88c9a94892fb07da5ef3ba8bc39ed"
        );
        Ok(())
    }

    #[test]
    fn test_file_chunking_16k() -> io::Result<()> {
        let infile = Path::new("./tests/fixtures/SekienAkashita.jpg");
        let results = find_file_chunks(&infile, 16384)?;
        assert_eq!(results.len(), 6);
        assert_eq!(results[0].offset, 0);
        assert_eq!(results[0].length, 22366);
        assert_eq!(
            results[0].digest.to_string(),
            "sha256-103159aa68bb1ea98f64248c647b8fe9a303365d80cb63974a73bba8bc3167d7"
        );
        assert_eq!(results[1].offset, 22366);
        assert_eq!(results[1].length, 8282);
        assert_eq!(
            results[1].digest.to_string(),
            "sha256-c95e0d6a53f61dc7b6039cfb8618f6e587fc6395780cf28169f4013463c89db3"
        );
        assert_eq!(results[2].offset, 30648);
        assert_eq!(results[2].length, 16303);
        assert_eq!(
            results[2].digest.to_string(),
            "sha256-e03c4de56410b680ef69d8f8cfe140c54bb33f295015b40462d260deb9a60b82"
        );
        assert_eq!(results[3].offset, 46951);
        assert_eq!(results[3].length, 18696);
        assert_eq!(
            results[3].digest.to_string(),
            "sha256-bd1198535cdb87c5571378db08b6e886daf810873f5d77000a54795409464138"
        );
        assert_eq!(results[4].offset, 65647);
        assert_eq!(results[4].length, 32768);
        assert_eq!(
            results[4].digest.to_string(),
            "sha256-5c8251cce144b5291be3d4b161461f3e5ed441a7a24a1a65fdcc3d7b21bfc29d"
        );
        assert_eq!(results[5].offset, 98415);
        assert_eq!(results[5].length, 11051);
        assert_eq!(
            results[5].digest.to_string(),
            "sha256-a566243537738371133ecff524501290f0621f786f010b45d20a9d5cf82365f8"
        );
        Ok(())
    }

    #[test]
    fn test_file_chunking_32k() -> io::Result<()> {
        let infile = Path::new("./tests/fixtures/SekienAkashita.jpg");
        let results = find_file_chunks(&infile, 32768)?;
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].offset, 0);
        assert_eq!(results[0].length, 32857);
        assert_eq!(
            results[0].digest.to_string(),
            "sha256-5a80871bad4588c7278d39707fe68b8b174b1aa54c59169d3c2c72f1e16ef46d"
        );
        assert_eq!(results[1].offset, 32857);
        assert_eq!(results[1].length, 16408);
        assert_eq!(
            results[1].digest.to_string(),
            "sha256-13f6a4c6d42df2b76c138c13e86e1379c203445055c2b5f043a5f6c291fa520d"
        );
        assert_eq!(results[2].offset, 49265);
        assert_eq!(results[2].length, 60201);
        assert_eq!(
            results[2].digest.to_string(),
            "sha256-0fe7305ba21a5a5ca9f89962c5a6f3e29cd3e2b36f00e565858e0012e5f8df36"
        );
        Ok(())
    }

    #[test]
    fn test_file_chunking_64k() -> io::Result<()> {
        let infile = Path::new("./tests/fixtures/SekienAkashita.jpg");
        let results = find_file_chunks(&infile, 65536)?;
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].offset, 0);
        assert_eq!(results[0].length, 32857);
        assert_eq!(
            results[0].digest.to_string(),
            "sha256-5a80871bad4588c7278d39707fe68b8b174b1aa54c59169d3c2c72f1e16ef46d"
        );
        assert_eq!(results[1].offset, 32857);
        assert_eq!(results[1].length, 76609);
        assert_eq!(
            results[1].digest.to_string(),
            "sha256-5420a3bcc7d57eaf5ca9bb0ab08a1bd3e4d89ae019b1ffcec39b1a5905641115"
        );
        Ok(())
    }

    #[test]
    fn test_pack_file_one_chunk() -> io::Result<()> {
        let chunks = [Chunk::new(
            Checksum::SHA256(
                "095964d07f3e821659d4eb27ed9e20cd5160c53385562df727e98eb815bb371f".to_owned(),
            ),
            0,
            3129,
        )
        .filepath(Path::new("./tests/fixtures/lorem-ipsum.txt"))];
        let outdir = tempdir()?;
        let packfile = outdir.path().join("pack.tar");
        let digest = pack_chunks(&chunks[..], &packfile)?;
        assert_eq!(
            digest.to_string(),
            "sha256-9fd73dfe8b3815ebbf9b0932816306526104336017d9ba308e37e48bce5ab150"
        );
        // verify by unpacking
        let entries: Vec<String> = unpack_chunks(&packfile, outdir.path())?;
        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries[0],
            "sha256-095964d07f3e821659d4eb27ed9e20cd5160c53385562df727e98eb815bb371f"
        );
        let sha256 = checksum_file(&outdir.path().join(&entries[0]))?;
        assert_eq!(
            sha256.to_string(),
            "sha256-095964d07f3e821659d4eb27ed9e20cd5160c53385562df727e98eb815bb371f"
        );
        Ok(())
    }

    #[test]
    fn test_pack_file_multiple_chunks() -> io::Result<()> {
        let chunks = [
            Chunk::new(
                Checksum::SHA256(
                    "ca8a04949bc4f604eb6fc4f2aeb27a0167e959565964b4bb3f3b780da62f6cb1".to_owned(),
                ),
                0,
                40000,
            )
            .filepath(Path::new("./tests/fixtures/SekienAkashita.jpg")),
            Chunk::new(
                Checksum::SHA256(
                    "cff5c0c15c6eef98784e8733d21dec87aae170a67e07ab0823024b26cab07b6f".to_owned(),
                ),
                40000,
                40000,
            )
            .filepath(Path::new("./tests/fixtures/SekienAkashita.jpg")),
            Chunk::new(
                Checksum::SHA256(
                    "e02dd839859aed2783f7aae9b68e1a568d68139bd9d907c1cd5beca056f06464".to_owned(),
                ),
                80000,
                29466,
            )
            .filepath(Path::new("./tests/fixtures/SekienAkashita.jpg")),
        ];
        let outdir = tempdir()?;
        let packfile = outdir.path().join("pack.tar");
        let digest = pack_chunks(&chunks, &packfile)?;
        assert_eq!(
            digest.to_string(),
            "sha256-0715334707315e0b16e1786d0a76ff70929b5671a2081da78970a652431b4a74"
        );
        // verify by unpacking
        let entries: Vec<String> = unpack_chunks(&packfile, outdir.path())?;
        assert_eq!(entries.len(), 3);
        assert_eq!(
            entries[0],
            "sha256-ca8a04949bc4f604eb6fc4f2aeb27a0167e959565964b4bb3f3b780da62f6cb1"
        );
        assert_eq!(
            entries[1],
            "sha256-cff5c0c15c6eef98784e8733d21dec87aae170a67e07ab0823024b26cab07b6f"
        );
        assert_eq!(
            entries[2],
            "sha256-e02dd839859aed2783f7aae9b68e1a568d68139bd9d907c1cd5beca056f06464"
        );
        let part1sum = checksum_file(&outdir.path().join(&entries[0]))?;
        assert_eq!(
            part1sum.to_string(),
            "sha256-ca8a04949bc4f604eb6fc4f2aeb27a0167e959565964b4bb3f3b780da62f6cb1"
        );
        let part2sum = checksum_file(&outdir.path().join(&entries[1]))?;
        assert_eq!(
            part2sum.to_string(),
            "sha256-cff5c0c15c6eef98784e8733d21dec87aae170a67e07ab0823024b26cab07b6f"
        );
        let part3sum = checksum_file(&outdir.path().join(&entries[2]))?;
        assert_eq!(
            part3sum.to_string(),
            "sha256-e02dd839859aed2783f7aae9b68e1a568d68139bd9d907c1cd5beca056f06464"
        );
        // test reassembling the file again
        let outfile = outdir.path().join("SekienAkashita.jpg");
        let part1 = outdir.path().join(&entries[0]);
        let part2 = outdir.path().join(&entries[1]);
        let part3 = outdir.path().join(&entries[2]);
        let parts = [part1.as_path(), part2.as_path(), part3.as_path()];
        assemble_chunks(&parts[..], &outfile)?;
        let allsum = checksum_file(&outfile)?;
        assert_eq!(
            allsum.to_string(),
            "sha256-d9e749d9367fc908876749d6502eb212fee88c9a94892fb07da5ef3ba8bc39ed"
        );
        Ok(())
    }

    #[test]
    fn test_encryption() -> Result<(), gpgme::Error> {
        let passphrase = "some passphrase";
        let infile = Path::new("./tests/fixtures/SekienAkashita.jpg");
        let outdir = tempdir()?;
        let ciphertext = outdir.path().join("SekienAkashita.jpg.enc");
        encrypt_file(passphrase, infile, &ciphertext)?;
        // cannot do much validation of the encrypted file, it is always
        // going to be different because of random keys and init vectors
        let plaintext = outdir.path().join("SekienAkashita.jpg");
        decrypt_file(passphrase, &ciphertext, &plaintext)?;
        let plainsum = checksum_file(&plaintext)?;
        assert_eq!(
            plainsum.to_string(),
            "sha256-d9e749d9367fc908876749d6502eb212fee88c9a94892fb07da5ef3ba8bc39ed"
        );
        Ok(())
    }

    #[test]
    fn test_serde_chunk() {
        // also good for tracking the rough size of a chunk record
        let chunk = Chunk::new(
            Checksum::SHA256(
                "ca8a04949bc4f604eb6fc4f2aeb27a0167e959565964b4bb3f3b780da62f6cb1".to_owned(),
            ),
            0,
            40000,
        )
        .packfile(Checksum::SHA256(
            "d9e749d9367fc908876749d6502eb212fee88c9a94892fb07da5ef3ba8bc39ed".to_owned(),
        ));
        let encoded: Vec<u8> = serde_cbor::to_vec(&chunk).unwrap();
        // serde_json produces a string that is about 10% larger than CBOR,
        // and CBOR is a (pending) internet standard, making it a good choice
        assert_eq!(encoded.len(), 84);
        let result: Chunk = serde_cbor::from_slice(&encoded).unwrap();
        // checksum for skipped field is all zeros
        assert_eq!(result.digest.to_string(), NULL_SHA1);
        // offset for skipped field is always zero
        assert_eq!(result.offset, 0);
        assert_eq!(result.length, 40000);
        assert!(result.packfile.is_some());
        assert_eq!(
            result.packfile.unwrap().to_string(),
            "sha256-d9e749d9367fc908876749d6502eb212fee88c9a94892fb07da5ef3ba8bc39ed"
        );
    }

    #[test]
    fn test_serde_pack() {
        // also good for tracking the rough size of a pack record
        let digest = Checksum::SHA256(
            "ca8a04949bc4f604eb6fc4f2aeb27a0167e959565964b4bb3f3b780da62f6cb1".to_owned(),
        );
        let uuid = generate_unique_id("charlie", "localhost");
        let bucket = generate_bucket_name(&uuid);
        let object = "sha256-ca8a04949bc4f604eb6fc4f2aeb27a0167e959565964b4bb3f3b780da62f6cb1";
        let pacloc = PackLocation::new("01arz3ndektsv4rrffq69g5fav", &bucket, &object);
        let pack = SavedPack::new(digest, vec![pacloc]);
        let encoded: Vec<u8> = serde_cbor::to_vec(&pack).unwrap();
        assert_eq!(encoded.len(), 225);
    }

    #[test]
    fn test_tree_entry() {
        let path = Path::new("./tests/fixtures/lorem-ipsum.txt");
        let tref = TreeReference::TREE(Checksum::SHA1("cafebabe".to_owned()));
        let result = TreeEntry::new(&path, tref);
        assert!(result.is_ok());
        let mut entry = result.unwrap();
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
        let result = TreeEntry::new(&path, tref);
        let entry1 = result.unwrap();
        let path = Path::new("./tests/fixtures/SekienAkashita.jpg");
        let sha1 = Checksum::SHA1("4c009e44fe5794df0b1f828f2a8c868e66644964".to_owned());
        let tref = TreeReference::FILE(sha1);
        let result = TreeEntry::new(&path, tref);
        let entry2 = result.unwrap();
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
