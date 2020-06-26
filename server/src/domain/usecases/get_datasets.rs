//
// Copyright (c) 2020 Nathan Fiedler
//
use crate::domain::entities::Dataset;
use crate::domain::repositories::RecordRepository;
use crate::domain::usecases::NoParams;
use failure::Error;

pub struct GetDatasets {
    repo: Box<dyn RecordRepository>,
}

impl GetDatasets {
    pub fn new(repo: Box<dyn RecordRepository>) -> Self {
        Self { repo }
    }
}

impl super::UseCase<Vec<Dataset>, NoParams> for GetDatasets {
    fn call(&self, _params: NoParams) -> Result<Vec<Dataset>, Error> {
        self.repo.get_datasets()
    }
}

#[cfg(test)]
mod tests {
    use super::super::UseCase;
    use super::*;
    use crate::domain::repositories::MockRecordRepository;
    use failure::err_msg;
    use std::path::Path;

    #[test]
    fn test_get_datasets_ok() {
        // arrange
        let datasets = vec![Dataset::new(Path::new("/home/planet"))];
        let mut mock = MockRecordRepository::new();
        mock.expect_get_datasets()
            .returning(move || Ok(datasets.clone()));
        // act
        let usecase = GetDatasets::new(Box::new(mock));
        let params = NoParams {};
        let result = usecase.call(params);
        // assert
        assert!(result.is_ok());
        let actual = result.unwrap();
        assert_eq!(actual.len(), 1);
        assert_eq!(actual[0].basepath.to_string_lossy(), "/home/planet");
    }

    #[test]
    fn test_get_datasets_err() {
        // arrange
        let mut mock = MockRecordRepository::new();
        mock.expect_get_datasets()
            .returning(|| Err(err_msg("oh no")));
        // act
        let usecase = GetDatasets::new(Box::new(mock));
        let params = NoParams {};
        let result = usecase.call(params);
        // assert
        assert!(result.is_err());
    }
}
