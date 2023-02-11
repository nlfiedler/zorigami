//
// Copyright (c) 2023 Nathan Fiedler
//

//! Defines the traits and types for all pack stores.

use anyhow::Error;
use std::fmt;
use std::fs::File;
use std::io;
use std::path::Path;

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
    io::copy(&mut file, &mut hasher)?;
    let digest = hasher.finalize();
    let result = format!("{:x}", digest);
    Ok(result)
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
        let md5sum = md5sum_file(&infile).unwrap();
        #[cfg(target_family = "unix")]
        assert_eq!(md5sum, "40756e6058736e2485119410c2014380");
        #[cfg(target_family = "windows")]
        assert_eq!(
            // this checksum is wrong and will need to be fixed
            md5sum,
            "40756e6058736e2485119410c2014380"
        );
    }
}
