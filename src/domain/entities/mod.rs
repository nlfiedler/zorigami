//
// Copyright (c) 2020 Nathan Fiedler
//
use failure::{err_msg, Error};
use std::collections::HashMap;
use std::fmt;
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::str::FromStr;

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
        let mut file = File::open(infile)?;
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
#[derive(Clone, Debug)]
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
}
