//
// Copyright (c) 2020 Nathan Fiedler
//
use crate::domain::entities::schedule::Schedule;
use crate::domain::entities::Dataset;
use crate::domain::repositories::RecordRepository;
use anyhow::Error;
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
        dataset.id = params.id;
        dataset.excludes = params.excludes;
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
    id: String,
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
    /// List of file/directory exclusion patterns.
    excludes: Vec<String>,
}

impl Params {
    pub fn new(
        id: String,
        basepath: PathBuf,
        schedules: Vec<Schedule>,
        workspace: Option<PathBuf>,
        pack_size: u64,
        stores: Vec<String>,
        excludes: Vec<String>,
    ) -> Self {
        Self {
            id,
            basepath,
            schedules,
            workspace,
            pack_size,
            stores,
            excludes,
        }
    }
}

impl fmt::Display for Params {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Params({})", self.id)
    }
}

impl cmp::PartialEq for Params {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
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
    fn test_update_dataset_ok() {
        // arrange
        let mut mock = MockRecordRepository::new();
        mock.expect_put_dataset().returning(|_| Ok(()));
        // act
        let usecase = UpdateDataset::new(Box::new(mock));
        let params = Params {
            id: "cafebabe".to_owned(),
            basepath: PathBuf::from("/home/planet"),
            schedules: vec![],
            workspace: None,
            pack_size: 33_554_432,
            stores: vec!["cafebabe".to_owned()],
            excludes: vec![],
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
            .returning(|_| Err(anyhow!("oh no")));
        // act
        let usecase = UpdateDataset::new(Box::new(mock));
        let params = Params {
            id: "cafebabe".to_owned(),
            basepath: PathBuf::from("/home/planet"),
            schedules: vec![],
            workspace: None,
            pack_size: 33_554_432,
            stores: vec!["cafebabe".to_owned()],
            excludes: vec![],
        };
        let result = usecase.call(params);
        // assert
        assert!(result.is_err());
    }
}
