//
// Copyright (c) 2022 Nathan Fiedler
//
use crate::domain::entities::RecordCounts;
use crate::domain::repositories::RecordRepository;
use crate::domain::usecases::NoParams;
use anyhow::Error;

pub struct GetCounts {
    repo: Box<dyn RecordRepository>,
}

impl GetCounts {
    pub fn new(repo: Box<dyn RecordRepository>) -> Self {
        Self { repo }
    }
}

impl super::UseCase<RecordCounts, NoParams> for GetCounts {
    fn call(&self, _params: NoParams) -> Result<RecordCounts, Error> {
        self.repo.get_entity_counts()
    }
}

#[cfg(test)]
mod tests {
    use super::super::UseCase;
    use super::*;
    use crate::domain::repositories::MockRecordRepository;
    use anyhow::anyhow;

    #[test]
    fn test_get_stores_ok() {
        // arrange
        let mut mock = MockRecordRepository::new();
        mock.expect_get_entity_counts().returning(move || {
            Ok(RecordCounts {
                chunk: 5,
                dataset: 1,
                file: 25,
                pack: 1,
                snapshot: 1,
                store: 1,
                tree: 3,
                xattr: 4,
            })
        });
        // act
        let usecase = GetCounts::new(Box::new(mock));
        let params = NoParams {};
        let result = usecase.call(params);
        // assert
        assert!(result.is_ok());
        let actual = result.unwrap();
        assert_eq!(actual.chunk, 5);
        assert_eq!(actual.dataset, 1);
        assert_eq!(actual.file, 25);
        assert_eq!(actual.pack, 1);
        assert_eq!(actual.snapshot, 1);
        assert_eq!(actual.store, 1);
        assert_eq!(actual.tree, 3);
        assert_eq!(actual.xattr, 4);
    }

    #[test]
    fn test_get_stores_err() {
        // arrange
        let mut mock = MockRecordRepository::new();
        mock.expect_get_entity_counts()
            .returning(|| Err(anyhow!("oh no")));
        // act
        let usecase = GetCounts::new(Box::new(mock));
        let params = NoParams {};
        let result = usecase.call(params);
        // assert
        assert!(result.is_err());
    }
}
