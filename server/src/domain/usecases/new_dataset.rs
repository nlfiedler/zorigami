//
// Copyright (c) 2024 Nathan Fiedler
//
use crate::domain::entities::Dataset;
use crate::domain::repositories::RecordRepository;
use crate::domain::usecases::NoParams;
use anyhow::Error;
use std::path::Path;

pub struct NewDataset {
    repo: Box<dyn RecordRepository>,
}

impl NewDataset {
    pub fn new(repo: Box<dyn RecordRepository>) -> Self {
        Self { repo }
    }
}

impl super::UseCase<Dataset, NoParams> for NewDataset {
    fn call(&self, _params: NoParams) -> Result<Dataset, Error> {
        let dataset = Dataset::new(Path::new("."));
        self.repo.put_dataset(&dataset)?;
        Ok(dataset)
    }
}

#[cfg(test)]
mod tests {
    use super::super::UseCase;
    use super::*;
    use crate::domain::repositories::MockRecordRepository;
    use anyhow::anyhow;

    #[test]
    fn test_new_dataset_ok() {
        // arrange
        let mut mock = MockRecordRepository::new();
        mock.expect_put_dataset().returning(|_| Ok(()));
        // act
        let usecase = NewDataset::new(Box::new(mock));
        let params = NoParams {};
        let result = usecase.call(params);
        // assert
        assert!(result.is_ok());
    }

    #[test]
    fn test_new_dataset_err() {
        // arrange
        let mut mock = MockRecordRepository::new();
        mock.expect_put_dataset()
            .returning(|_| Err(anyhow!("oh no")));
        // act
        let usecase = NewDataset::new(Box::new(mock));
        let params = NoParams {};
        let result = usecase.call(params);
        // assert
        assert!(result.is_err());
    }
}
