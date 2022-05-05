//
// Copyright (c) 2022 Nathan Fiedler
//
use crate::domain::managers::process::Processor;
use crate::domain::repositories::RecordRepository;
use anyhow::Error;
use std::cmp;
use std::fmt;
use std::sync::Arc;

pub struct StartBackup {
    repo: Box<dyn RecordRepository>,
    processor: Arc<dyn Processor>,
}

impl StartBackup {
    pub fn new(repo: Box<dyn RecordRepository>, processor: Arc<dyn Processor>) -> Self {
        Self { repo, processor }
    }
}

impl super::UseCase<(), Params> for StartBackup {
    fn call(&self, params: Params) -> Result<(), Error> {
        for dataset in self.repo.get_datasets()? {
            if dataset.id == params.dataset_id {
                self.processor.start_backup(dataset)?;
            }
        }
        Ok(())
    }
}

pub struct Params {
    /// Unique identifier of the dataset.
    dataset_id: String,
}

impl Params {
    pub fn new(dataset_id: String) -> Self {
        Self { dataset_id }
    }
}

impl fmt::Display for Params {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Params({})", self.dataset_id)
    }
}

impl cmp::PartialEq for Params {
    fn eq(&self, other: &Self) -> bool {
        self.dataset_id == other.dataset_id
    }
}

impl cmp::Eq for Params {}

#[cfg(test)]
mod tests {
    use super::super::UseCase;
    use super::*;
    use crate::domain::entities::Dataset;
    use crate::domain::managers::process::MockProcessor;
    use crate::domain::repositories::MockRecordRepository;
    use anyhow::anyhow;
    use std::path::Path;

    #[test]
    fn test_start_backup_ok() {
        // arrange
        let datasets = vec![Dataset::new(Path::new("/home/planet"))];
        let dataset_id = datasets[0].id.clone();
        let mut repo = MockRecordRepository::new();
        repo.expect_get_datasets()
            .returning(move || Ok(datasets.clone()));
        let mut processor = MockProcessor::new();
        processor.expect_start_backup().returning(|_| Ok(()));
        // act
        let usecase = StartBackup::new(Box::new(repo), Arc::new(processor));
        let params = Params { dataset_id };
        let result = usecase.call(params);
        // assert
        assert!(result.is_ok());
    }

    #[test]
    fn test_start_backup_not_found() {
        // arrange
        let datasets = vec![Dataset::new(Path::new("/home/planet"))];
        let mut repo = MockRecordRepository::new();
        repo.expect_get_datasets()
            .returning(move || Ok(datasets.clone()));
        let processor = MockProcessor::new();
        // act
        let usecase = StartBackup::new(Box::new(repo), Arc::new(processor));
        let params = Params {
            dataset_id: "nonesuch".to_owned(),
        };
        let result = usecase.call(params);
        // assert
        assert!(result.is_ok());
    }

    #[test]
    fn test_start_backup_err() {
        // arrange
        let mut repo = MockRecordRepository::new();
        repo.expect_get_datasets()
            .returning(|| Err(anyhow!("oh no")));
        let processor = MockProcessor::new();
        // act
        let usecase = StartBackup::new(Box::new(repo), Arc::new(processor));
        let params = Params {
            dataset_id: "cafebabe".to_owned(),
        };
        let result = usecase.call(params);
        // assert
        assert!(result.is_err());
    }
}
