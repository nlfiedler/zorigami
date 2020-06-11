//
// Copyright (c) 2020 Nathan Fiedler
//
use crate::domain::entities::schedule::Schedule;
use crate::domain::entities::Dataset;
use crate::domain::repositories::RecordRepository;
use failure::Error;
use std::cmp;
use std::fmt;
use std::path::PathBuf;

pub struct UpdateDataset {
    repo: Box<dyn RecordRepository>,
}

impl UpdateDataset {
    pub fn new(repo: Box<dyn RecordRepository>) -> Self {
        Self { repo }
    }
}

impl super::UseCase<Dataset, Params> for UpdateDataset {
    fn call(&self, params: Params) -> Result<Dataset, Error> {
        // use the constructor to leverage some of the default behavior in case
        // not everything has been defined in the params
        let mut dataset = Dataset::new(&params.basepath);
        dataset.key = params.key;
        for schedule in params.schedules {
            dataset = dataset.add_schedule(schedule);
        }
        for store in params.stores.iter() {
            dataset = dataset.add_store(store);
        }
        dataset = dataset.pack_size(params.pack_size);
        if let Some(workspace) = params.workspace {
            dataset.workspace = workspace;
        }
        self.repo.put_dataset(&dataset)?;
        Ok(dataset)
    }
}

pub struct Params {
    /// Unique identifier of this dataset.
    key: String,
    /// Local base path of dataset to be saved.
    basepath: PathBuf,
    /// Set of schedules for when to run the backup.
    schedules: Vec<Schedule>,
    /// Path for temporary pack building.
    workspace: Option<PathBuf>,
    /// Target size in bytes for pack files.
    pack_size: u64,
    /// Identifiers of the stores to contain pack files.
    stores: Vec<String>,
}

impl Params {
    pub fn new(
        key: String,
        basepath: PathBuf,
        schedules: Vec<Schedule>,
        workspace: Option<PathBuf>,
        pack_size: u64,
        stores: Vec<String>,
    ) -> Self {
        Self {
            key,
            basepath,
            schedules,
            workspace,
            pack_size,
            stores,
        }
    }
}

impl fmt::Display for Params {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Params({})", self.key)
    }
}

impl cmp::PartialEq for Params {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl cmp::Eq for Params {}

#[cfg(test)]
mod tests {
    use super::super::UseCase;
    use super::*;
    use crate::domain::repositories::MockRecordRepository;
    use failure::err_msg;
    use mockall::predicate::*;

    #[test]
    fn test_update_dataset_ok() {
        // arrange
        let mut mock = MockRecordRepository::new();
        mock.expect_put_dataset()
            .with(always())
            .returning(|_| Ok(()));
        // act
        let usecase = UpdateDataset::new(Box::new(mock));
        let params = Params {
            key: "cafebabe".to_owned(),
            basepath: PathBuf::from("/home/planet"),
            schedules: vec![],
            workspace: None,
            pack_size: 33_554_432,
            stores: vec!["cafebabe".to_owned()],
        };
        let result = usecase.call(params);
        // assert
        assert!(result.is_ok());
        let actual = result.unwrap();
        assert_eq!(actual.basepath.to_string_lossy(), "/home/planet");
    }

    #[test]
    fn test_update_dataset_err() {
        // arrange
        let mut mock = MockRecordRepository::new();
        mock.expect_put_dataset()
            .with(always())
            .returning(|_| Err(err_msg("oh no")));
        // act
        let usecase = UpdateDataset::new(Box::new(mock));
        let params = Params {
            key: "cafebabe".to_owned(),
            basepath: PathBuf::from("/home/planet"),
            schedules: vec![],
            workspace: None,
            pack_size: 33_554_432,
            stores: vec!["cafebabe".to_owned()],
        };
        let result = usecase.call(params);
        // assert
        assert!(result.is_err());
    }
}
