//
// Copyright (c) 2020 Nathan Fiedler
//
use crate::domain::repositories::RecordRepository;
use anyhow::Error;
use std::cmp;
use std::fmt;

pub struct DeleteDataset {
    repo: Box<dyn RecordRepository>,
}

impl DeleteDataset {
    pub fn new(repo: Box<dyn RecordRepository>) -> Self {
        Self { repo }
    }
}

impl super::UseCase<(), Params> for DeleteDataset {
    fn call(&self, params: Params) -> Result<(), Error> {
        self.repo.delete_dataset(&params.dataset_id)?;
        // ignore any errors when deleting records that may or may not be
        // present in the data source
        let _ = self.repo.delete_computer_id(&params.dataset_id);
        let _ = self.repo.delete_latest_snapshot(&params.dataset_id);
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
    use crate::domain::repositories::MockRecordRepository;
    use anyhow::anyhow;

    #[test]
    fn test_delete_dataset_ok() {
        // arrange
        let mut mock = MockRecordRepository::new();
        mock.expect_delete_dataset().returning(|_| Ok(()));
        mock.expect_delete_computer_id().returning(|_| Ok(()));
        mock.expect_delete_latest_snapshot().returning(|_| Ok(()));
        // act
        let usecase = DeleteDataset::new(Box::new(mock));
        let params = Params {
            dataset_id: "cafebabe".to_owned(),
        };
        let result = usecase.call(params);
        // assert
        assert!(result.is_ok());
    }

    #[test]
    fn test_delete_dataset_err() {
        // arrange
        let mut mock = MockRecordRepository::new();
        mock.expect_delete_dataset()
            .returning(|_| Err(anyhow!("oh no")));
        // act
        let usecase = DeleteDataset::new(Box::new(mock));
        let params = Params {
            dataset_id: "cafebabe".to_owned(),
        };
        let result = usecase.call(params);
        // assert
        assert!(result.is_err());
    }
}
