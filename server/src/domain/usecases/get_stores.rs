//
// Copyright (c) 2020 Nathan Fiedler
//
use crate::domain::entities::Store;
use crate::domain::repositories::RecordRepository;
use crate::domain::usecases::NoParams;
use anyhow::Error;

pub struct GetStores {
    repo: Box<dyn RecordRepository>,
}

impl GetStores {
    pub fn new(repo: Box<dyn RecordRepository>) -> Self {
        Self { repo }
    }
}

impl super::UseCase<Vec<Store>, NoParams> for GetStores {
    fn call(&self, _params: NoParams) -> Result<Vec<Store>, Error> {
        self.repo.get_stores()
    }
}

#[cfg(test)]
mod tests {
    use super::super::UseCase;
    use super::*;
    use crate::domain::entities::{PackRetention, StoreType};
    use crate::domain::repositories::MockRecordRepository;
    use anyhow::anyhow;
    use std::collections::HashMap;

    #[test]
    fn test_get_stores_ok() {
        // arrange
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("basepath".to_owned(), "/home/planet".to_owned());
        let stores = vec![Store {
            id: "cafebabe".to_owned(),
            store_type: StoreType::LOCAL,
            label: "mylocalstore".to_owned(),
            properties,
            retention: PackRetention::ALL,
        }];
        let mut mock = MockRecordRepository::new();
        mock.expect_get_stores()
            .returning(move || Ok(stores.clone()));
        // act
        let usecase = GetStores::new(Box::new(mock));
        let params = NoParams {};
        let result = usecase.call(params);
        // assert
        assert!(result.is_ok());
        let actual = result.unwrap();
        assert_eq!(actual.len(), 1);
        assert_eq!(actual[0].id, "cafebabe");
    }

    #[test]
    fn test_get_stores_err() {
        // arrange
        let mut mock = MockRecordRepository::new();
        mock.expect_get_stores().returning(|| Err(anyhow!("oh no")));
        // act
        let usecase = GetStores::new(Box::new(mock));
        let params = NoParams {};
        let result = usecase.call(params);
        // assert
        assert!(result.is_err());
    }
}
