//
// Copyright (c) 2024 Nathan Fiedler
//
use crate::domain::entities::SnapshotCount;
use crate::domain::repositories::RecordRepository;
use anyhow::{Error, anyhow};
use chrono::{DateTime, Utc};
use std::cmp;
use std::fmt;

///
/// Count the number of existing snapshots for a given dataset, and return the
/// date/time of the oldest snapshot.
///
pub struct CountSnapshots {
    repo: Box<dyn RecordRepository>,
}

impl CountSnapshots {
    pub fn new(repo: Box<dyn RecordRepository>) -> Self {
        Self { repo }
    }
}

impl super::UseCase<SnapshotCount, Params> for CountSnapshots {
    fn call(&self, params: Params) -> Result<SnapshotCount, Error> {
        let dataset = self
            .repo
            .get_dataset(&params.dataset)?
            .ok_or_else(|| anyhow!(format!("missing dataset: {:?}", &params.dataset)))?;
        if let Some(latest_hash) = dataset.snapshot {
            let mut visited = 1;
            let mut digest = latest_hash;
            let mut newest: Option<DateTime<Utc>> = None;
            let mut oldest: Option<DateTime<Utc>>;
            loop {
                let snapshot = self
                    .repo
                    .get_snapshot(&digest)?
                    .ok_or_else(|| anyhow!(format!("missing snapshot: {:?}", &digest)))?;
                if newest.is_none() {
                    newest = Some(snapshot.start_time);
                }
                oldest = snapshot.end_time;
                if let Some(parent) = snapshot.parent {
                    digest = parent;
                } else {
                    break;
                }
                visited += 1;
            }
            Ok(SnapshotCount {
                count: visited,
                newest,
                oldest,
            })
        } else {
            Ok(SnapshotCount::default())
        }
    }
}

pub struct Params {
    /// Identifier of the dataset containing the snapshot.
    dataset: String,
}

impl Params {
    pub fn new(dataset: String) -> Self {
        Self { dataset }
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
    use chrono::TimeDelta;

    use super::*;
    use crate::domain::entities::{Checksum, Dataset, Snapshot};
    use crate::domain::repositories::MockRecordRepository;
    use crate::domain::usecases::UseCase;

    #[test]
    fn test_count_snapshots_zero() {
        // arrange
        let mut mock = MockRecordRepository::new();
        let dataset1 = Dataset::new(std::path::Path::new("/home/planet"));
        let dataset1_1 = dataset1.clone();
        let dataset1_id = dataset1.id.clone();
        let dataset1_id2 = dataset1.id.clone();
        mock.expect_get_dataset()
            .once()
            .withf(move |id| id == dataset1_id)
            .returning(move |_| Ok(Some(dataset1_1.clone())));

        // act
        let usecase = CountSnapshots::new(Box::new(mock));
        let params = Params::new(dataset1_id2);
        let result = usecase.call(params);
        // assert
        assert!(result.is_ok());
        let counts = result.unwrap();
        assert_eq!(counts.count, 0);
        assert!(counts.oldest.is_none());
        assert!(counts.newest.is_none());
    }

    #[test]
    fn test_count_snapshots_one() {
        // arrange
        let tree_a = Checksum::SHA1("e794e51".to_owned());
        let snapshot_a = Snapshot::new(None, tree_a, Default::default());
        let snapshot_a1 = snapshot_a.digest.clone();
        let snapshot_a2 = snapshot_a.digest.clone();
        let mut mock = MockRecordRepository::new();
        mock.expect_get_snapshot()
            .withf(move |d| d == &snapshot_a1)
            .returning(move |_| Ok(Some(snapshot_a.clone())));

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
        let usecase = CountSnapshots::new(Box::new(mock));
        let params = Params::new(dataset1_id2);
        let result = usecase.call(params);
        // assert
        assert!(result.is_ok());
        let counts = result.unwrap();
        assert_eq!(counts.count, 1);
        // the one snapshot does not have an end time yet
        assert!(counts.oldest.is_none());
        assert!(counts.newest.is_some());
    }

    #[test]
    fn test_count_snapshots_three() {
        // arrange
        let tree_c = Checksum::SHA1("9f61917".to_owned());
        let mut snapshot_c = Snapshot::new(None, tree_c, Default::default());
        snapshot_c.set_start_time(Utc::now() - TimeDelta::days(4));
        snapshot_c.set_end_time(Utc::now() - TimeDelta::days(3));
        let snapshot_c1 = snapshot_c.digest.clone();
        let snapshot_c2 = snapshot_c.digest.clone();
        let tree_b = Checksum::SHA1("014f04f".to_owned());
        let mut snapshot_b = Snapshot::new(Some(snapshot_c1), tree_b, Default::default());
        snapshot_b.set_start_time(Utc::now() - TimeDelta::days(2));
        snapshot_b.set_end_time(Utc::now() - TimeDelta::days(1));
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
        let usecase = CountSnapshots::new(Box::new(mock));
        let params = Params::new(dataset1_id2);
        let result = usecase.call(params);
        // assert
        assert!(result.is_ok());
        let counts = result.unwrap();
        assert_eq!(counts.count, 3);
        assert!(counts.oldest.is_some());
        assert!(counts.newest.is_some());
    }
}
