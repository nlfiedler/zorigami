//
// Copyright (c) 2020 Nathan Fiedler
//
use crate::domain::entities::{Checksum, Snapshot};
use crate::domain::repositories::RecordRepository;
use anyhow::Error;
use std::cmp;
use std::fmt;

pub struct GetSnapshot {
    repo: Box<dyn RecordRepository>,
}

impl GetSnapshot {
    pub fn new(repo: Box<dyn RecordRepository>) -> Self {
        Self { repo }
    }
}

impl super::UseCase<Option<Snapshot>, Params> for GetSnapshot {
    fn call(&self, params: Params) -> Result<Option<Snapshot>, Error> {
        self.repo.get_snapshot(&params.digest)
    }
}

pub struct Params {
    /// Hash digest of the snapshot to retrieve.
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

#[cfg(test)]
mod tests {
    use super::super::UseCase;
    use super::*;
    use crate::domain::repositories::MockRecordRepository;
    use anyhow::anyhow;

    #[test]
    fn test_get_snapshot_some() {
        // arrange
        let tree_sha = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
        let snapshot = Snapshot::new(None, tree_sha, Default::default());
        let snapshot_sha1 = snapshot.digest.clone();
        let snapshot_sha2 = snapshot.digest.clone();
        let snapshot_sha3 = snapshot.digest.clone();
        let mut mock = MockRecordRepository::new();
        mock.expect_get_snapshot()
            .withf(move |d| d == &snapshot_sha1)
            .returning(move |_| Ok(Some(snapshot.clone())));
        // act
        let usecase = GetSnapshot::new(Box::new(mock));
        let params = Params::new(snapshot_sha2);
        let result = usecase.call(params);
        // assert
        assert!(result.is_ok());
        let option = result.unwrap();
        assert!(option.is_some());
        let actual = option.unwrap();
        assert_eq!(actual.digest, snapshot_sha3);
    }

    #[test]
    fn test_get_snapshot_none() {
        // arrange
        let tree_sha = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
        let snapshot = Snapshot::new(None, tree_sha, Default::default());
        let snapshot_sha1 = snapshot.digest.clone();
        let snapshot_sha2 = snapshot.digest.clone();
        let mut mock = MockRecordRepository::new();
        mock.expect_get_snapshot()
            .withf(move |d| d == &snapshot_sha1)
            .returning(move |_| Ok(None));
        // act
        let usecase = GetSnapshot::new(Box::new(mock));
        let params = Params::new(snapshot_sha2);
        let result = usecase.call(params);
        // assert
        assert!(result.is_ok());
        let option = result.unwrap();
        assert!(option.is_none());
    }

    #[test]
    fn test_get_snapshot_err() {
        // arrange
        let tree_sha = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
        let snapshot = Snapshot::new(None, tree_sha, Default::default());
        let snapshot_sha1 = snapshot.digest.clone();
        let snapshot_sha2 = snapshot.digest.clone();
        let mut mock = MockRecordRepository::new();
        mock.expect_get_snapshot()
            .withf(move |d| d == &snapshot_sha1)
            .returning(|_| Err(anyhow!("oh no")));
        // act
        let usecase = GetSnapshot::new(Box::new(mock));
        let params = Params::new(snapshot_sha2);
        let result = usecase.call(params);
        // assert
        assert!(result.is_err());
    }
}
