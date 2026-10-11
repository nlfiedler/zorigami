//
// Copyright (c) 2026 Nathan Fiedler
//
use crate::domain::entities::RecordSizes;
use crate::domain::repositories::RecordRepository;
use crate::domain::usecases::NoParams;
use anyhow::Error;

pub struct GetSizes {
    repo: Box<dyn RecordRepository>,
}

impl GetSizes {
    pub fn new(repo: Box<dyn RecordRepository>) -> Self {
        Self { repo }
    }
}

impl super::UseCase<Option<RecordSizes>, NoParams> for GetSizes {
    fn call(&self, _params: NoParams) -> Result<Option<RecordSizes>, Error> {
        self.repo.get_entity_sizes()
    }
}

#[cfg(test)]
mod tests {
    use super::super::UseCase;
    use super::*;
    use crate::domain::repositories::MockRecordRepository;
    use anyhow::anyhow;

    #[test]
    fn test_get_sizes_ok() {
        // arrange
        let mut mock = MockRecordRepository::new();
        mock.expect_get_entity_sizes().returning(move || {
            Ok(Some(RecordSizes {
                chunk: 500,
                dataset: 100,
                file: 2500,
                pack: 150,
                snapshot: 120,
                store: 110,
                tree: 300,
                xattr: 400,
            }))
        });
        // act
        let usecase = GetSizes::new(Box::new(mock));
        let params = NoParams {};
        let result = usecase.call(params);
        // assert
        assert!(result.is_ok());
        let actual = result.unwrap().unwrap();
        assert_eq!(actual.chunk, 500);
        assert_eq!(actual.dataset, 100);
        assert_eq!(actual.file, 2500);
        assert_eq!(actual.pack, 150);
        assert_eq!(actual.snapshot, 120);
        assert_eq!(actual.store, 110);
        assert_eq!(actual.tree, 300);
        assert_eq!(actual.xattr, 400);
    }

    #[test]
    fn test_get_sizes_unsupported() {
        // arrange
        let mut mock = MockRecordRepository::new();
        mock.expect_get_entity_sizes().returning(|| Ok(None));
        // act
        let usecase = GetSizes::new(Box::new(mock));
        let params = NoParams {};
        let result = usecase.call(params);
        // assert
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_get_sizes_err() {
        // arrange
        let mut mock = MockRecordRepository::new();
        mock.expect_get_entity_sizes()
            .returning(|| Err(anyhow!("oh no")));
        // act
        let usecase = GetSizes::new(Box::new(mock));
        let params = NoParams {};
        let result = usecase.call(params);
        // assert
        assert!(result.is_err());
    }
}
