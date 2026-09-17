//
// Copyright (c) 2020 Nathan Fiedler
//
use crate::domain::repositories::RecordRepository;
use anyhow::Error;
use log::warn;
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
        let result = self.repo.delete_dataset(&params.dataset_id);
        match &result {
            Ok(()) => warn!(
                "audit: deleteDataset id={} caller={} outcome=ok",
                params.dataset_id, params.caller
            ),
            Err(e) => warn!(
                "audit: deleteDataset id={} caller={} outcome=error error={}",
                params.dataset_id, params.caller, e
            ),
        }
        result
    }
}

pub struct Params {
    /// Unique identifier of the dataset.
    dataset_id: String,
    /// Caller identity for audit logging (e.g. remote address); "unknown"
    /// when not available.
    caller: String,
}

impl Params {
    pub fn new(dataset_id: String) -> Self {
        Self {
            dataset_id,
            caller: "unknown".to_owned(),
        }
    }

    /// Attach the caller identity (e.g. remote address) used in the audit
    /// log line for this deletion.
    pub fn with_caller(mut self, caller: String) -> Self {
        self.caller = caller;
        self
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
        // act
        let usecase = DeleteDataset::new(Box::new(mock));
        let params = Params {
            dataset_id: "cafebabe".to_owned(),
            caller: "unknown".to_owned(),
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
            caller: "unknown".to_owned(),
        };
        let result = usecase.call(params);
        // assert
        assert!(result.is_err());
    }
}
