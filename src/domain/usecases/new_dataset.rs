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
        let config = self.repo.get_configuration()?;
        let mut dataset = Dataset::new(&config.computer_id, &params.basepath);
        for schedule in params.schedules {
            dataset = dataset.add_schedule(schedule);
        }
        for store in params.stores.iter() {
            dataset = dataset.add_store(store);
        }
        dataset = dataset.pack_size(params.pack_size);
        self.repo.put_dataset(&dataset)?;
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
}

impl Params {
    pub fn new(
        basepath: PathBuf,
        schedules: Vec<Schedule>,
        pack_size: u64,
        stores: Vec<String>,
    ) -> Self {
        Self {
            basepath,
            schedules,
            pack_size,
            stores,
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
    use failure::err_msg;
    use mockall::predicate::*;

    #[test]
    fn test_new_dataset_ok() {
        // arrange
        let config: Configuration = Default::default();
        let mut mock = MockRecordRepository::new();
        mock.expect_get_configuration()
            .returning(move || Ok(config.clone()));
        mock.expect_put_dataset()
            .with(always())
            .returning(|_| Ok(()));
        // act
        let usecase = NewDataset::new(Box::new(mock));
        let params = Params {
            basepath: PathBuf::from("/home/planet"),
            schedules: vec![],
            pack_size: 33_554_432,
            stores: vec!["cafebabe".to_owned()],
        };
        let result = usecase.call(params);
        // assert
        assert!(result.is_ok());
        let actual = result.unwrap();
        assert_eq!(actual.basepath.to_string_lossy(), "/home/planet");
        assert_eq!(actual.pack_size, 33_554_432);
        assert_eq!(actual.stores.len(), 1);
        assert_eq!(actual.stores[0], "cafebabe");
    }

    #[test]
    fn test_new_dataset_err() {
        // arrange
        let config: Configuration = Default::default();
        let mut mock = MockRecordRepository::new();
        mock.expect_get_configuration()
            .returning(move || Ok(config.clone()));
        mock.expect_put_dataset()
            .with(always())
            .returning(|_| Err(err_msg("oh no")));
        // act
        let usecase = NewDataset::new(Box::new(mock));
        let params = Params {
            basepath: PathBuf::from("/home/planet"),
            schedules: vec![],
            pack_size: 33_554_432,
            stores: vec!["cafebabe".to_owned()],
        };
        let result = usecase.call(params);
        // assert
        assert!(result.is_err());
    }
}
