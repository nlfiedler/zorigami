//
// Copyright (c) 2023 Nathan Fiedler
//

//! Defines the traits and types for all pack stores.

use anyhow::Error;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Write as _;
use std::fs::File;
use std::io::Read;
use std::path::Path;

/// Store property key selecting the object-lock (WORM) window, in days.
///
/// Absent, empty, `0`, or an unparseable value all mean "no lock", preserving
/// the pre-immutability behavior. See `lock_days_from_props`.
pub const LOCK_DAYS_PROPERTY: &str = "lock_days";

/// Parse the object-lock window (in days) from a store's `properties` map.
///
/// This is the lenient reader used on the hot paths (upload and pruning): any
/// absent, empty, or malformed value yields `0` (no lock). Values are validated
/// strictly at store create/update time, so a malformed value should not reach
/// here in practice; treating it as "no lock" is the safe fallback.
pub fn lock_days_from_props(props: &HashMap<String, String>) -> u16 {
    props
        .get(LOCK_DAYS_PROPERTY)
        .and_then(|s| s.trim().parse::<u16>().ok())
        .unwrap_or(0)
}

/// Compute the object-lock retain-until instant: `lock_days` from now.
///
/// Returned as a `SystemTime` so this stays free of any storage SDK; callers
/// convert to their backend's timestamp type. The store fixes this absolute
/// value at upload time, so later changes to `lock_days` do not alter objects
/// already written.
pub fn lock_retain_until(lock_days: u16) -> std::time::SystemTime {
    std::time::SystemTime::now() + std::time::Duration::from_secs(lock_days as u64 * 24 * 60 * 60)
}

///
/// Return the last part of the path, converting to a String.
///
/// This is useful in cases where we want a sensible value for the final
/// component of the path, but if that is not possible, then just give up and
/// ignore this path. For listings of local or SFTP directories, this is
/// probably okay, since if the file name cannot be converted to UTF-8
/// correctly, then we did not create it and we don't care about it.
///
pub fn get_file_name(path: &Path) -> Option<String> {
    // ignore any paths that end in '..'
    if let Some(p) = path.file_name() {
        // ignore any paths that failed UTF-8 translation
        if let Some(pp) = p.to_str() {
            return Some(pp.to_owned());
        }
    }
    // This is like core::get_file_name(), but we would likely have errors later
    // on if we tried to use lossy values for CRUD operations.
    None
}

/// Compute the MD5 digest of the given file.
pub fn md5sum_file(infile: &Path) -> Result<String, Error> {
    use md5::{Digest, Md5};
    let mut file = File::open(infile)?;
    let mut hasher = Md5::new();
    let mut buf = [0u8; 8192];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hex_lower(&hasher.finalize()))
}

/// Compute the MD5 digest of the given blob of data.
pub fn md5sum_blob<T: AsRef<[u8]>>(data: T) -> Result<String, Error> {
    use md5::{Digest, Md5};
    let mut hasher = Md5::new();
    hasher.update(data);
    Ok(hex_lower(&hasher.finalize()))
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(out, "{:02x}", b);
    }
    out
}

///
/// Remote coordinates for a pack file, naming the store, bucket, and object by
/// which the pack file can be retrieved.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Coordinates {
    /// ULID of the pack store.
    pub store: String,
    /// Remote bucket name.
    pub bucket: String,
    /// Remote object name.
    pub object: String,
}

impl Coordinates {
    /// Create a new Coordinates record using the given information.
    pub fn new(store: &str, bucket: &str, object: &str) -> Self {
        Self {
            store: store.to_owned(),
            bucket: bucket.to_owned(),
            object: object.to_owned(),
        }
    }
}

///
/// Raised when the cloud service indicates that a bucket with the same name
/// already exists but belongs to another project.
///
#[derive(thiserror::Error, Debug)]
pub struct CollisionError;

impl fmt::Display for CollisionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "bucket collision")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_md5sum_file() {
        let infile = Path::new("../../test/fixtures/lorem-ipsum.txt");
        let md5sum = md5sum_file(infile).unwrap();
        #[cfg(target_family = "unix")]
        assert_eq!(md5sum, "40756e6058736e2485119410c2014380");
        #[cfg(target_family = "windows")]
        assert_eq!(md5sum, "8aed508af644bc58db20c9b73c5b67ad");
    }

    #[test]
    fn test_md5sum_blob() {
        let md5sum = md5sum_blob(b"hello world").unwrap();
        #[cfg(target_family = "unix")]
        assert_eq!(md5sum, "5eb63bbbe01eeed093cb22bb8f5acdc3");
        #[cfg(target_family = "windows")]
        assert_eq!(md5sum, "5eb63bbbe01eeed093cb22bb8f5acdc3");
    }

    #[test]
    fn test_lock_days_from_props() {
        let mut props: HashMap<String, String> = HashMap::new();
        // absent -> 0
        assert_eq!(lock_days_from_props(&props), 0);
        // explicit zero -> 0
        props.insert(LOCK_DAYS_PROPERTY.to_owned(), "0".to_owned());
        assert_eq!(lock_days_from_props(&props), 0);
        // normal value, tolerating surrounding whitespace
        props.insert(LOCK_DAYS_PROPERTY.to_owned(), " 30 ".to_owned());
        assert_eq!(lock_days_from_props(&props), 30);
        // empty -> 0
        props.insert(LOCK_DAYS_PROPERTY.to_owned(), "".to_owned());
        assert_eq!(lock_days_from_props(&props), 0);
        // malformed -> 0 (validation rejects these at write time)
        props.insert(LOCK_DAYS_PROPERTY.to_owned(), "notanumber".to_owned());
        assert_eq!(lock_days_from_props(&props), 0);
    }

    #[test]
    fn test_lock_retain_until() {
        use std::time::{Duration, SystemTime};
        let before = SystemTime::now();
        let retain = lock_retain_until(2);
        // the retain-until must be at least ~2 days out, and no more than a
        // hair over 2 days from the moment we sampled `before`
        assert!(retain >= before + Duration::from_secs(2 * 24 * 60 * 60));
        assert!(retain <= SystemTime::now() + Duration::from_secs(2 * 24 * 60 * 60 + 5));
    }
}
