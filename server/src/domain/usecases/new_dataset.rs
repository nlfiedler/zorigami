//
// Copyright (c) 2024 Nathan Fiedler
//
use crate::domain::entities::schedule::Schedule;
use crate::domain::entities::Dataset;
use crate::domain::repositories::RecordRepository;
use anyhow::Error;
use std::cmp;
use std::fmt;
use std::path::PathBuf;

pub struct NewDataset {
    repo: Box<dyn RecordRepository>,
}

impl NewDataset {
    pub fn new(repo: Box<dyn RecordRepository>) -> Self {
        Self { repo }
    }
}

impl super::UseCase<Dataset, Params> for NewDataset {
    fn call(&self, params: Params) -> Result<Dataset, Error> {
        // use the constructor to generate a new identifier and then copy
        // everything over
        let mut dataset = Dataset::with_pack_size(&params.basepath, params.pack_size);
        dataset.excludes = params
            .excludes
            .into_iter()
            .map(|e| e.trim().to_owned())
            .filter(|e| !e.is_empty())
            .collect();
        for schedule in params.schedules {
            dataset.add_schedule(schedule);
        }
        for store in params.stores.iter() {
            dataset.add_store(store);
        }
        self.repo.put_dataset(&dataset)?;
        // for new datasets we need to save the computer id
        let config = self.repo.get_configuration()?;
        self.repo
            .put_computer_id(&dataset.id, &config.computer_id)?;
        Ok(dataset)
    }
}

pub struct Params {
    /// Local base path of dataset to be saved.
    basepath: PathBuf,
    /// Set of schedules for when to run the backup.
    schedules: Vec<Schedule>,
    /// Target size in bytes for pack files.
    pack_size: u64,
    /// Identifiers of the stores to contain pack files.
    stores: Vec<String>,
    /// List of file/directory exclusion patterns.
    excludes: Vec<String>,
}

impl Params {
    pub fn new(
        basepath: PathBuf,
        schedules: Vec<Schedule>,
        pack_size: u64,
        stores: Vec<String>,
        excludes: Vec<String>,
    ) -> Self {
        Self {
            basepath,
            schedules,
            pack_size,
            stores,
            excludes,
        }
    }
}

impl fmt::Display for Params {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Params({})", self.basepath.to_string_lossy())
    }
}

impl cmp::PartialEq for Params {
    fn eq(&self, other: &Self) -> bool {
        self.basepath == other.basepath
    }
}

impl cmp::Eq for Params {}

#[cfg(test)]
mod tests {
    use super::super::UseCase;
    use super::*;
    use crate::domain::entities::Configuration;
    use crate::domain::repositories::MockRecordRepository;
    use anyhow::anyhow;
    use mockall::predicate::*;

    #[test]
    fn test_new_dataset_ok() {
        // arrange
        let config: Configuration = Default::default();
        let mut mock = MockRecordRepository::new();
        mock.expect_get_configuration()
            .returning(move || Ok(config.clone()));
        mock.expect_put_dataset().returning(|_| Ok(()));
        mock.expect_put_computer_id()
            .with(always(), always())
            .returning(|_, _| Ok(()));
        // act
        #[cfg(target_family="unix")]
        let basepath = "/home/planet";
        #[cfg(target_family="windows")]
        let basepath = "\\home\\planet";
        let usecase = NewDataset::new(Box::new(mock));
        let params = Params {
            basepath: PathBuf::from(basepath),
            schedules: vec![],
            pack_size: 33_554_432,
            stores: vec!["cafebabe".to_owned()],
            excludes: vec![],
        };
        let result = usecase.call(params);
        // assert
        assert!(result.is_ok());
        let actual = result.unwrap();
        assert_eq!(actual.basepath.to_string_lossy(), basepath);
        #[cfg(target_family="unix")]
        let expected_workspace = "/home/planet/.tmp";
        #[cfg(target_family="windows")]
        let expected_workspace = "\\home\\planet\\.tmp";
        assert_eq!(actual.workspace.to_string_lossy(), expected_workspace);
        assert_eq!(actual.pack_size, 33_554_432);
        assert_eq!(actual.stores.len(), 1);
        assert_eq!(actual.stores[0], "cafebabe");
        assert_eq!(actual.excludes.len(), 0);
    }

    #[test]
    fn test_new_dataset_empty_excludes() {
        // arrange
        let config: Configuration = Default::default();
        let mut mock = MockRecordRepository::new();
        mock.expect_get_configuration()
            .returning(move || Ok(config.clone()));
        mock.expect_put_dataset().returning(|_| Ok(()));
        mock.expect_put_computer_id()
            .with(always(), always())
            .returning(|_, _| Ok(()));
        // act
        let usecase = NewDataset::new(Box::new(mock));
        #[cfg(target_family="unix")]
        let basepath = "/home/planet";
        #[cfg(target_family="windows")]
        let basepath = "\\home\\planet";
        let params = Params {
            basepath: PathBuf::from(basepath),
            schedules: vec![],
            pack_size: 33_554_432,
            stores: vec!["cafebabe".to_owned()],
            excludes: vec!["".to_owned()],
        };
        let result = usecase.call(params);
        // assert
        assert!(result.is_ok());
        let actual = result.unwrap();
        assert_eq!(actual.basepath.to_string_lossy(), basepath);
        #[cfg(target_family="unix")]
        let expected_workspace = "/home/planet/.tmp";
        #[cfg(target_family="windows")]
        let expected_workspace = "\\home\\planet\\.tmp";
        assert_eq!(actual.workspace.to_string_lossy(), expected_workspace);
        assert_eq!(actual.pack_size, 33_554_432);
        assert_eq!(actual.stores.len(), 1);
        assert_eq!(actual.stores[0], "cafebabe");
        assert_eq!(actual.excludes.len(), 0);
    }

    #[test]
    fn test_new_dataset_err() {
        // arrange
        let config: Configuration = Default::default();
        let mut mock = MockRecordRepository::new();
        mock.expect_get_configuration()
            .returning(move || Ok(config.clone()));
        mock.expect_put_dataset()
            .returning(|_| Err(anyhow!("oh no")));
        mock.expect_put_computer_id()
            .with(always(), always())
            .returning(|_, _| Ok(()));
        // act
        let usecase = NewDataset::new(Box::new(mock));
        let params = Params {
            basepath: PathBuf::from("/home/planet"),
            schedules: vec![],
            pack_size: 33_554_432,
            stores: vec!["cafebabe".to_owned()],
            excludes: vec![],
        };
        let result = usecase.call(params);
        // assert
        assert!(result.is_err());
    }
}
