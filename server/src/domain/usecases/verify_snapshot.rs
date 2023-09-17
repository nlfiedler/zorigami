//
// Copyright (c) 2023 Nathan Fiedler
//
use crate::domain::entities::{Checksum, TreeReference};
use crate::domain::repositories::RecordRepository;
use anyhow::{anyhow, Error};
use bloomfilter::Bloom;
use std::cmp;
use std::collections::VecDeque;
use std::fmt;

pub struct VerifySnapshot {
    repo: Box<dyn RecordRepository>,
}

impl VerifySnapshot {
    pub fn new(repo: Box<dyn RecordRepository>) -> Self {
        Self { repo }
    }
}

impl super::UseCase<Vec<DataError>, Params> for VerifySnapshot {
    fn call(&self, params: Params) -> Result<Vec<DataError>, Error> {
        let mut issues: Vec<DataError> = Vec::new();
        let snapshot = self
            .repo
            .get_snapshot(&params.digest)?
            .ok_or_else(|| anyhow!(format!("missing snapshot: {:?}", params.digest)))?;
        let mut pending_trees: VecDeque<Checksum> = VecDeque::new();
        pending_trees.push_back(snapshot.tree);
        // roughly 1mb of space for visited tree filter
        let mut visited_trees: Bloom<Checksum> = Bloom::new_for_fp_rate(250000, 0.0000001);
        // roughly 2mb of space for visited file filter
        let mut visited_files: Bloom<Checksum> = Bloom::new_for_fp_rate(500000, 0.0000001);
        while let Some(tree_digest) = pending_trees.pop_front() {
            if !visited_trees.check(&tree_digest) {
                if let Some(tree) = self.repo.get_tree(&tree_digest)? {
                    for entry in tree.entries.iter() {
                        match &entry.reference {
                            TreeReference::LINK(_) => (),
                            TreeReference::SMALL(_) => (),
                            TreeReference::TREE(checksum) => {
                                pending_trees.push_back(checksum.to_owned())
                            }
                            TreeReference::FILE(file_digest) => {
                                if !visited_files.check(file_digest) {
                                    if let Some(code) = check_file(&self.repo, file_digest)? {
                                        issues.push(code);
                                    }
                                    visited_files.set(file_digest);
                                }
                            }
                        }
                        for (_, xattr_digest) in entry.xattrs.iter() {
                            if self.repo.get_xattr(xattr_digest)?.is_none() {
                                issues.push(DataError::MissingXattrs(xattr_digest.to_owned()));
                            }
                        }
                    }
                    visited_trees.set(&tree_digest);
                } else {
                    issues.push(DataError::MissingTree(tree_digest.to_owned()));
                }
            }
        }
        // TODO: devise an efficient means of verifying all pack entries, or
        //       consider putting this into a separate use case due to cost
        Ok(issues)
    }
}

fn check_file(
    repo: &Box<dyn RecordRepository>,
    file_digest: &Checksum,
) -> Result<Option<DataError>, Error> {
    if let Some(file) = repo.get_file(file_digest)? {
        for (_, chunk_digest) in file.chunks.iter() {
            if let Some(chunk) = repo.get_chunk(chunk_digest)? {
                if let Some(pack_digest) = chunk.packfile {
                    if repo.get_pack(&pack_digest)?.is_none() {
                        return Ok(Some(DataError::MissingPack(pack_digest.to_owned())));
                    }
                } else {
                    return Ok(Some(DataError::UnpackedChunk(chunk_digest.to_owned())));
                }
            } else {
                return Ok(Some(DataError::MissingChunk(chunk_digest.to_owned())));
            }
        }
    } else {
        return Ok(Some(DataError::MissingFile(file_digest.to_owned())));
    }
    Ok(None)
}

pub struct Params {
    /// Hash digest of the snapshot to verify.
    digest: Checksum,
}

impl Params {
    pub fn new(digest: Checksum) -> Self {
        Self { digest }
    }
}

impl fmt::Display for Params {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Params({})", self.digest)
    }
}

impl cmp::PartialEq for Params {
    fn eq(&self, other: &Self) -> bool {
        self.digest == other.digest
    }
}

impl cmp::Eq for Params {}

/// A single issue found regarding a database record.
#[derive(Clone, Debug, PartialEq)]
pub enum DataError {
    /// Missing tree record.
    MissingTree(Checksum),
    /// Missing file record.
    MissingFile(Checksum),
    /// Missing chunk record.
    MissingChunk(Checksum),
    /// Chunk has no associated pack record.
    UnpackedChunk(Checksum),
    /// Missing pack record.
    MissingPack(Checksum),
    /// Missing extended attributes record.
    MissingXattrs(Checksum),
}

#[cfg(test)]
mod tests {
    use super::super::UseCase;
    use super::*;
    use crate::domain::entities::Snapshot;
    use crate::domain::repositories::MockRecordRepository;

    #[test]
    fn test_verify_snapshot_missing_snapshot() {
        // arrange
        let snapshot_sha1 = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
        let snapshot_cleon = snapshot_sha1.clone();
        let mut mock = MockRecordRepository::new();
        mock.expect_get_snapshot()
            .withf(move |d| d == &snapshot_sha1)
            .returning(move |_| Ok(None));
        // act
        let usecase = VerifySnapshot::new(Box::new(mock));
        let params = Params::new(snapshot_cleon);
        let result = usecase.call(params);
        // assert
        assert!(result.is_err());
        let err_string = result.unwrap_err().to_string();
        assert!(err_string.contains("missing snapshot"));
    }

    #[test]
    fn test_verify_snapshot_missing_tree() {
        // arrange
        let tree_sha = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
        let tree_sha2 = tree_sha.clone();
        let snapshot = Snapshot::new(None, tree_sha, Default::default());
        let snapshot_sha1 = snapshot.digest.clone();
        let snapshot_sha2 = snapshot.digest.clone();
        let mut mock = MockRecordRepository::new();
        mock.expect_get_snapshot()
            .withf(move |d| d == &snapshot_sha1)
            .returning(move |_| Ok(Some(snapshot.clone())));
        mock.expect_get_tree()
            .withf(move |d| d == &tree_sha2)
            .returning(|_| Ok(None));
        // act
        let usecase = VerifySnapshot::new(Box::new(mock));
        let params = Params::new(snapshot_sha2);
        let result = usecase.call(params);
        // assert
        assert!(result.is_ok());
        let errors = result.unwrap();
        assert_eq!(errors.len(), 1);
        matches!(&errors[0], DataError::MissingTree(_));
    }

    // TODO: missing file
    // TODO: missing pack
    // TODO: missing chunk
    // TODO: missing xattr
    // TODO: unpacked chunk
}
