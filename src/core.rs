//
// Copyright (c) 2019 Nathan Fiedler
//

//! The `core` module defines the most basic of functions and the core data
//! types used throughout the application.

use chrono::prelude::*;
use failure::{err_msg, Error};
use fastcdc;
use flate2::read::ZlibDecoder;
use flate2::write::ZlibEncoder;
use flate2::Compression;
use log::error;
use memmap::MmapOptions;
use serde::{Deserialize, Serialize};
use sodiumoxide::crypto::pwhash::{self, Salt};
use sodiumoxide::crypto::secretstream::{self, Stream, Tag};
use std::collections::HashMap;
use std::fmt;
use std::fs::{self, File, FileType};
use std::io;
use std::io::prelude::*;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Once;
use std::time::SystemTime;
use tar::{Archive, Builder, Header};
use ulid::Ulid;
use uuid::Uuid;

///
/// Generate a type 5 UUID based on the given values.
///
/// Returns a shortened version of the UUID to minimize storage and reduce the
/// number of pixels used to display the value. It can be converted back to a
/// UUID using `blob_uuid::to_uuid()` if necessary.
///
pub fn generate_unique_id(username: &str, hostname: &str) -> String {
    let mut name = String::from(username);
    name.push(':');
    name.push_str(hostname);
    let bytes = name.into_bytes();
    let uuid = Uuid::new_v5(&Uuid::NAMESPACE_URL, &bytes);
    blob_uuid::to_blob(&uuid)
}

///
/// Generate a suitable bucket name, using a ULID and the given unique ID.
///
/// The unique ID is assumed to be a shorted version of the UUID returned from
/// `generate_unique_id()`, and will be converted back to a full UUID for the
/// purposes of generating a bucket name consisting only of lowercase letters.
///
pub fn generate_bucket_name(unique_id: &str) -> String {
    match blob_uuid::to_uuid(unique_id) {
        Ok(uuid) => {
            let shorter = uuid.to_simple().to_string();
            let mut ulid = Ulid::new().to_string();
            ulid.push_str(&shorter);
            ulid.to_lowercase()
        }
        Err(err) => {
            error!("failed to convert unique ID: {:?}", err);
            Ulid::new().to_string().to_lowercase()
        }
    }
}

///
/// Return the unique bucket name for this computer and user.
///
pub fn computer_bucket_name(unique_id: &str) -> String {
    match blob_uuid::to_uuid(unique_id) {
        Ok(uuid) => uuid.to_simple().to_string(),
        Err(err) => {
            error!("failed to convert unique ID: {:?}", err);
            Ulid::new().to_string().to_lowercase()
        }
    }
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
/// Create a compressed tar file for the given directory structure.
///
pub fn create_tar(basepath: &Path, outfile: &Path) -> Result<(), Error> {
    let file = File::create(outfile)?;
    let encoder = ZlibEncoder::new(file, Compression::default());
    let mut builder = Builder::new(encoder);
    builder.append_dir_all(".", basepath)?;
    let _output = builder.into_inner()?;
    Ok(())
}

///
/// Extract the contents of the compressed tar file to the given directory.
///
pub fn extract_tar(infile: &Path, outdir: &Path) -> Result<(), Error> {
    let file = File::open(infile)?;
    let decoder = ZlibDecoder::new(file);
    let mut ar = Archive::new(decoder);
    ar.unpack(outdir)?;
    Ok(())
}

// Used to avoid initializing the crypto library more than once. Not a
// requirement, but seems sensible and it is easy.
static CRYPTO_INIT: Once = Once::new();

// Initialize the crypto library to improve performance and ensure all of its
// operations are thread-safe.
fn init_crypto() {
    CRYPTO_INIT.call_once(|| {
        let _ = sodiumoxide::init();
    });
}

/// Retrieve the user-defined passphrase.
///
/// Returns a default if one has not been defined.
pub fn get_passphrase() -> String {
    std::env::var("PASSPHRASE").unwrap_or_else(|_| "keyboard cat".to_owned())
}

// Hash the given user password using a computationally expensive algorithm.
fn hash_password(passphrase: &str, salt: &Salt) -> Result<secretstream::Key, Error> {
    init_crypto();
    let mut k = secretstream::Key([0; secretstream::KEYBYTES]);
    let secretstream::Key(ref mut kb) = k;
    match pwhash::derive_key(
        kb,
        passphrase.as_bytes(),
        salt,
        pwhash::OPSLIMIT_INTERACTIVE,
        pwhash::MEMLIMIT_INTERACTIVE,
    ) {
        Ok(_) => Ok(k),
        Err(()) => Err(err_msg("pwhash::derive_key() failed mysteriously")),
    }
}

// Size of the "messages" encrypted with libsodium. We need to read the stream
// back in chunks this size to successfully decrypt.
static CRYPTO_BUFLEN: usize = 8192;

///
/// Encrypt the given file using libsodium stream encryption.
///
/// The passphrase is used with a newly generated salt to produce a secret key,
/// which is then used to encrypt the file. The salt is returned to the caller.
///
pub fn encrypt_file(passphrase: &str, infile: &Path, outfile: &Path) -> Result<Salt, Error> {
    init_crypto();
    let salt = pwhash::gen_salt();
    let key = hash_password(passphrase, &salt)?;
    let attr = fs::symlink_metadata(infile)?;
    let infile_len = attr.len();
    let mut total_bytes_read: u64 = 0;
    let mut buffer = vec![0; CRYPTO_BUFLEN];
    let (mut enc_stream, header) =
        Stream::init_push(&key).map_err(|_| err_msg("stream init failed"))?;
    let mut input = File::open(infile)?;
    let mut cipher = File::create(outfile)?;
    // Write out a magic/version number for backward compatibility. The magic
    // number is meant to be unique for this file type. The version accounts for
    // any change in the size of the secret stream header, which may change,
    // even if that may be unlikely.
    let version = [b'Z', b'R', b'G', b'M', 0, 0, 0, 1];
    cipher.write_all(&version)?;
    cipher.write_all(header.as_ref())?;
    while total_bytes_read < infile_len {
        let bytes_read = input.read(&mut buffer)?;
        total_bytes_read += bytes_read as u64;
        let tag = if total_bytes_read < infile_len {
            Tag::Message
        } else {
            Tag::Final
        };
        let cipher_text = enc_stream
            .push(&buffer[0..bytes_read], None, tag)
            .map_err(|_| err_msg("stream push failed"))?;
        cipher.write_all(cipher_text.as_ref())?;
    }
    Ok(salt)
}

///
/// Decrypt the encrypted file using the given passphrase and salt.
///
pub fn decrypt_file(
    passphrase: &str,
    salt: &Salt,
    infile: &Path,
    outfile: &Path,
) -> Result<(), Error> {
    init_crypto();
    let key = hash_password(passphrase, salt)?;
    let mut input = File::open(infile)?;
    // read the magic/version and ensure they match expectations
    let mut version_bytes = [0; 8];
    input.read_exact(&mut version_bytes)?;
    if version_bytes[0..4] != [b'Z', b'R', b'G', b'M'] {
        return Err(err_msg("pack file missing magic number"));
    }
    if version_bytes[4..8] != [0, 0, 0, 1] {
        return Err(err_msg("pack file unsupported version"));
    }
    // create a vector with sufficient space to read the header
    let mut header_vec = vec![0; secretstream::HEADERBYTES];
    input.read_exact(&mut header_vec)?;
    let header = secretstream::Header::from_slice(&header_vec)
        .ok_or_else(|| err_msg("invalid secretstream header"))?;
    // initialize the pull stream
    let mut dec_stream =
        Stream::init_pull(&header, &key).map_err(|_| err_msg("stream init failed"))?;
    let mut plain = File::create(outfile)?;
    // buffer must be large enough for reading an entire message
    let mut buffer = vec![0; CRYPTO_BUFLEN + secretstream::ABYTES];
    // read the encrypted text until the stream is finalized
    while !dec_stream.is_finalized() {
        let bytes_read = input.read(&mut buffer)?;
        // n.b. this will fail if the read does not get the entire message, but
        // that is unlikely when reading local files
        let (decrypted, _tag) = dec_stream
            .pull(&buffer[0..bytes_read], None)
            .map_err(|_| err_msg("stream pull failed"))?;
        plain.write_all(decrypted.as_ref())?;
    }
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
    /// Error occurred while processing the entry.
    ERROR,
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
    pub ctime: DateTime<Utc>,
    /// Modified time.
    #[serde(rename = "mt")]
    pub mtime: DateTime<Utc>,
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
    pub fn mode(mut self, path: &Path) -> Self {
        // Either mode or file attributes will be sufficient to cover all
        // supported systems; the "permissions" field only has one bit,
        // read-only, and that is already in mode and file attributes.
        #[cfg(target_family = "unix")]
        {
            use std::os::unix::fs::MetadataExt;
            if let Ok(meta) = fs::symlink_metadata(path) {
                self.mode = Some(meta.mode());
            }
        }
        #[cfg(target_family = "windows")]
        {
            use std::os::windows::prelude::*;
            if let Ok(meta) = fs::symlink_metadata(path) {
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
        }
        self
    }
}

impl fmt::Display for TreeEntry {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let ctime = self.ctime.timestamp();
        let mtime = self.mtime.timestamp();
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
    pub start_time: DateTime<Utc>,
    /// Time when the snapshot completed its upload. Will be `None` until
    /// the backup has completed.
    #[serde(rename = "et")]
    pub end_time: Option<DateTime<Utc>>,
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
        let start_time = Utc::now();
        Self {
            parent,
            start_time,
            end_time: None,
            file_count: 0,
            tree,
        }
    }

    /// Add the end_time property.
    pub fn end_time(mut self, end_time: DateTime<Utc>) -> Self {
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
        let stime = self.start_time.timestamp();
        let etime = self.end_time.unwrap_or_else(Utc::now).timestamp();
        // Format in a manner similar to git commit entries; this forms part of
        // the digest value for the snapshot, so it should remain relatively
        // stable over time.
        let parent = match self.parent {
            None => NULL_SHA1.to_string(),
            Some(ref value) => value.to_string(),
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
    /// Length of the file in bytes.
    #[serde(rename = "len")]
    pub length: u64,
    /// The set of the chunks contained in this file. There may be many of these
    /// for large files, so they are represented compactly.
    #[serde(rename = "cnx")]
    pub chunks: Vec<(u64, Checksum)>,
}

impl SavedFile {
    /// Create a new SavedFile to represent the given file and its chunks.
    pub fn new(digest: Checksum, length: u64, chunks: Vec<(u64, Checksum)>) -> Self {
        Self {
            digest,
            length,
            chunks,
        }
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
    pub upload_time: DateTime<Utc>,
    /// Salt used to encrypt this pack.
    #[serde(rename = "sa")]
    pub crypto_salt: Option<Salt>,
}

impl SavedPack {
    /// Create a new SavedPack record using the given information. Assumes the
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
    /// cron-like expression for the backup schedule
    #[serde(rename = "sc")]
    pub schedule: Option<String>,
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
            schedule: None,
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

impl fmt::Display for Dataset {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "dataset-{}", self.key)
    }
}

/// Contains the configuration of the application, pertaining to all datasets.
#[derive(Serialize, Deserialize, Debug)]
pub struct Configuration {
    /// name of the computer on which this application is running
    #[serde(rename = "hn")]
    pub hostname: String,
    /// name of the user running this application
    #[serde(rename = "un")]
    pub username: String,
    /// computer UUID for generating bucket names
    #[serde(rename = "id")]
    pub computer_id: String,
}

impl Default for Configuration {
    fn default() -> Self {
        let username = whoami::username();
        let hostname = whoami::hostname();
        let computer_id = generate_unique_id(&username, &hostname);
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
    use serde_cbor;
    use tempfile::tempdir;

    #[test]
    fn test_generate_unique_id() {
        let uuid = generate_unique_id("charlie", "localhost");
        // UUIDv5 = 747267d5-6e70-5711-8a9a-a40c24c1730f
        assert_eq!(uuid, "dHJn1W5wVxGKmqQMJMFzDw");
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
    fn test_encryption() -> Result<(), Error> {
        let passphrase = "some passphrase";
        let infile = Path::new("./tests/fixtures/SekienAkashita.jpg");
        let outdir = tempdir()?;
        let ciphertext = outdir.path().join("SekienAkashita.jpg.enc");
        let salt = encrypt_file(passphrase, infile, &ciphertext)?;
        // cannot do much validation of the encrypted file, it is always
        // going to be different because of random keys and init vectors
        let plaintext = outdir.path().join("SekienAkashita.jpg");
        decrypt_file(passphrase, &salt, &ciphertext, &plaintext)?;
        let plainsum = checksum_file(&plaintext)?;
        assert_eq!(
            plainsum.to_string(),
            "sha256-d9e749d9367fc908876749d6502eb212fee88c9a94892fb07da5ef3ba8bc39ed"
        );
        Ok(())
    }

    #[test]
    fn test_hash_password() -> Result<(), Error> {
        let passwd = "Correct Horse Battery Staple";
        let salt = pwhash::gen_salt();
        let result = hash_password(passwd, &salt)?;
        assert_eq!(result.as_ref().len(), secretstream::KEYBYTES);
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
        let mut pack = SavedPack::new(digest, vec![pacloc]);
        pack.crypto_salt = Some(pwhash::gen_salt());
        let encoded: Vec<u8> = serde_cbor::to_vec(&pack).unwrap();
        assert_eq!(encoded.len(), 245);
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

    #[test]
    fn test_tar_file() -> Result<(), Error> {
        let outdir = tempdir()?;
        let packfile = outdir.path().join("filename.tz");
        create_tar(Path::new("./tests/fixtures"), &packfile)?;
        extract_tar(&packfile, outdir.path())?;

        let file = outdir.path().join("SekienAkashita.jpg");
        let chksum = checksum_file(&file)?;
        assert_eq!(
            chksum.to_string(),
            "sha256-d9e749d9367fc908876749d6502eb212fee88c9a94892fb07da5ef3ba8bc39ed"
        );
        let file = outdir.path().join("lorem-ipsum.txt");
        let chksum = checksum_file(&file)?;
        assert_eq!(
            chksum.to_string(),
            "sha256-095964d07f3e821659d4eb27ed9e20cd5160c53385562df727e98eb815bb371f"
        );
        let file = outdir.path().join("washington-journal.txt");
        let chksum = checksum_file(&file)?;
        assert_eq!(
            chksum.to_string(),
            "sha256-314d5e0f0016f0d437829541f935bd1ebf303f162fdd253d5a47f65f40425f05"
        );

        Ok(())
    }
}
