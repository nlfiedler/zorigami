//
// Copyright (c) 2026 Nathan Fiedler
//
use crate::domain::entities::Snapshot;
use crate::domain::repositories::RecordRepository;
use anyhow::{Error, anyhow};
use std::cmp;
use std::fmt;

///
/// Retrieve all snapshots for a given dataset, in chronological order.
///
pub struct GetSnapshots {
    repo: Box<dyn RecordRepository>,
}

impl GetSnapshots {
    pub fn new(repo: Box<dyn RecordRepository>) -> Self {
        Self { repo }
    }
}

impl super::UseCase<Vec<Snapshot>, Params> for GetSnapshots {
    fn call(&self, params: Params) -> Result<Vec<Snapshot>, Error> {
        let dataset = self
            .repo
            .get_dataset(&params.dataset)?
            .ok_or_else(|| anyhow!(format!("missing dataset: {:?}", &params.dataset)))?;
        if let Some(mut digest) = dataset.snapshot {
            let mut snaps: Vec<Snapshot> = vec![];
            loop {
                let snapshot = self
                    .repo
                    .get_snapshot(&digest)?
                    .ok_or_else(|| anyhow!(format!("missing snapshot: {:?}", &digest)))?;
                snaps.push(snapshot.clone());
                if let Some(parent) = snapshot.parent {
                    digest = parent;
                } else {
                    break;
                }
            }
            Ok(snaps)
        } else {
            Ok(vec![])
        }
    }
}

pub struct Params {
    /// Identifier of the dataset for which to retrieve snapshots.
    dataset: String,
}

impl Params {
    pub fn new(id: String) -> Self {
        Self { dataset: id }
    }
}

impl fmt::Display for Params {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Params({})", self.dataset)
    }
}

impl cmp::PartialEq for Params {
    fn eq(&self, other: &Self) -> bool {
        self.dataset == other.dataset
    }
}

impl cmp::Eq for Params {}

#[cfg(test)]
mod tests {
    use super::super::UseCase;
    use super::*;
    use crate::domain::entities::{Checksum, Dataset};
    use crate::domain::repositories::MockRecordRepository;
    use anyhow::anyhow;
    use std::path::Path;

    #[test]
    fn test_get_snapshots_one() {
        // arrange
        let mut dataset = Dataset::new(Path::new("/home/planet"));
        let dataset_id = dataset.id.clone();
        let tree_sha = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
        let snapshot = Snapshot::new(None, tree_sha, Default::default());
        let snapshot_sha1 = snapshot.digest.clone();
        dataset.snapshot = Some(snapshot_sha1.clone());
        let mut mock = MockRecordRepository::new();
        mock.expect_get_dataset()
            .returning(move |_| Ok(Some(dataset.clone())));
        mock.expect_get_snapshot()
            .withf(move |d| d == &snapshot_sha1)
            .returning(move |_| Ok(Some(snapshot.clone())));
        // act
        let usecase = GetSnapshots::new(Box::new(mock));
        let params = Params::new(dataset_id);
        let result = usecase.call(params);
        let actual = result.unwrap();
        // assert
        assert_eq!(actual.len(), 1);
    }

    #[test]
    fn test_get_snapshots_three() {
        // arrange
        let tree_c = Checksum::SHA1("9f61917".to_owned());
        let snapshot_c = Snapshot::new(None, tree_c, Default::default());
        let snapshot_c1 = snapshot_c.digest.clone();
        let snapshot_c2 = snapshot_c.digest.clone();
        let tree_b = Checksum::SHA1("014f04f".to_owned());
        let snapshot_b = Snapshot::new(Some(snapshot_c1), tree_b, Default::default());
        let snapshot_b1 = snapshot_b.digest.clone();
        let snapshot_b2 = snapshot_b.digest.clone();
        let tree_a = Checksum::SHA1("e794e51".to_owned());
        let snapshot_a = Snapshot::new(Some(snapshot_b1), tree_a, Default::default());
        let snapshot_a1 = snapshot_a.digest.clone();
        let snapshot_a2 = snapshot_a.digest.clone();
        let mut mock = MockRecordRepository::new();
        mock.expect_get_snapshot()
            .withf(move |d| d == &snapshot_a1)
            .returning(move |_| Ok(Some(snapshot_a.clone())));
        mock.expect_get_snapshot()
            .withf(move |d| d == &snapshot_b2)
            .returning(move |_| Ok(Some(snapshot_b.clone())));
        mock.expect_get_snapshot()
            .withf(move |d| d == &snapshot_c2)
            .returning(move |_| Ok(Some(snapshot_c.clone())));

        let mut dataset1 = Dataset::new(std::path::Path::new("/home/planet"));
        dataset1.snapshot = Some(snapshot_a2.clone());
        let dataset1_1 = dataset1.clone();
        let dataset1_id = dataset1.id.clone();
        let dataset1_id2 = dataset1.id.clone();
        mock.expect_get_dataset()
            .once()
            .withf(move |id| id == dataset1_id)
            .returning(move |_| Ok(Some(dataset1_1.clone())));

        // act
        let usecase = GetSnapshots::new(Box::new(mock));
        let params = Params::new(dataset1_id2);
        let result = usecase.call(params);
        // assert
        assert!(result.is_ok());
        let actual = result.unwrap();
        assert_eq!(actual.len(), 3);
    }

    #[test]
    fn test_get_snapshots_none() {
        // arrange
        let dataset = Dataset::new(Path::new("/home/planet"));
        let dataset_id = dataset.id.clone();
        let mut mock = MockRecordRepository::new();
        mock.expect_get_dataset()
            .returning(move |_| Ok(Some(dataset.clone())));
        // act
        let usecase = GetSnapshots::new(Box::new(mock));
        let params = Params::new(dataset_id);
        let result = usecase.call(params);
        let actual = result.unwrap();
        // assert
        assert!(actual.is_empty());
    }

    #[test]
    fn test_get_snapshots_dataset_err() {
        // arrange
        let dataset = Dataset::new(Path::new("/home/planet"));
        let dataset_id = dataset.id.clone();
        let mut mock = MockRecordRepository::new();
        mock.expect_get_dataset()
            .returning(|_| Err(anyhow!("oh no")));
        // act
        let usecase = GetSnapshots::new(Box::new(mock));
        let params = Params::new(dataset_id);
        let result = usecase.call(params);
        // assert
        assert!(result.is_err());
    }

    #[test]
    fn test_get_snapshots_snapshot_err() {
        // arrange
        let mut dataset = Dataset::new(Path::new("/home/planet"));
        let dataset_id = dataset.id.clone();
        let tree_sha = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".to_owned());
        let snapshot = Snapshot::new(None, tree_sha, Default::default());
        let snapshot_sha1 = snapshot.digest.clone();
        dataset.snapshot = Some(snapshot_sha1.clone());
        let mut mock = MockRecordRepository::new();
        mock.expect_get_dataset()
            .returning(move |_| Ok(Some(dataset.clone())));
        mock.expect_get_snapshot()
            .withf(move |d| d == &snapshot_sha1)
            .returning(|_| Err(anyhow!("oh no")));
        // act
        let usecase = GetSnapshots::new(Box::new(mock));
        let params = Params::new(dataset_id);
        let result = usecase.call(params);
        // assert
        assert!(result.is_err());
    }
}
