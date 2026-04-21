//
// Copyright (c) 2026 Nathan Fiedler
//
use crate::domain::entities::Configuration;
use crate::domain::repositories::RecordRepository;
use crate::domain::services::buckets::BucketNamingPolicy;
use anyhow::Error;
use std::cmp;
use std::fmt;

///
/// Update the application configuration. Currently only affects the bucket
/// naming policy as the other properties are read-only.
///
pub struct UpdateConfiguration {
    repo: Box<dyn RecordRepository>,
}

impl UpdateConfiguration {
    pub fn new(repo: Box<dyn RecordRepository>) -> Self {
        Self { repo }
    }
}

impl super::UseCase<Configuration, Params> for UpdateConfiguration {
    fn call(&self, params: Params) -> Result<Configuration, Error> {
        let mut config = self.repo.get_configuration()?;
        config.bucket_naming = params.bucket_naming;
        self.repo.put_configuration(&config)?;
        Ok(config)
    }
}

pub struct Params {
    /// Bucket naming policy to set, or `None` to clear.
    bucket_naming: Option<BucketNamingPolicy>,
}

impl Params {
    pub fn new(bucket_naming: Option<BucketNamingPolicy>) -> Self {
        Self { bucket_naming }
    }
}

impl fmt::Display for Params {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Params({:?})", self.bucket_naming)
    }
}

impl cmp::PartialEq for Params {
    fn eq(&self, other: &Self) -> bool {
        self.bucket_naming == other.bucket_naming
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
    fn test_update_configuration_set_policy() {
        // arrange
        let mut mock = MockRecordRepository::new();
        mock.expect_get_configuration()
            .returning(|| Ok(Configuration::default()));
        mock.expect_put_configuration().returning(|config| {
            assert_eq!(config.bucket_naming, Some(BucketNamingPolicy::Scheduled(7)));
            Ok(())
        });
        // act
        let usecase = UpdateConfiguration::new(Box::new(mock));
        let params = Params::new(Some(BucketNamingPolicy::Scheduled(7)));
        let result = usecase.call(params);
        // assert
        assert!(result.is_ok());
        let actual = result.unwrap();
        assert_eq!(actual.bucket_naming, Some(BucketNamingPolicy::Scheduled(7)));
    }

    #[test]
    fn test_update_configuration_clear_policy() {
        // arrange
        let mut mock = MockRecordRepository::new();
        mock.expect_get_configuration().returning(|| {
            Ok(Configuration {
                bucket_naming: Some(BucketNamingPolicy::RandomPool(5)),
                ..Default::default()
            })
        });
        mock.expect_put_configuration().returning(|config| {
            assert!(config.bucket_naming.is_none());
            Ok(())
        });
        // act
        let usecase = UpdateConfiguration::new(Box::new(mock));
        let params = Params::new(None);
        let result = usecase.call(params);
        // assert
        assert!(result.is_ok());
        assert!(result.unwrap().bucket_naming.is_none());
    }

    #[test]
    fn test_update_configuration_err() {
        // arrange
        let mut mock = MockRecordRepository::new();
        mock.expect_get_configuration()
            .returning(|| Ok(Configuration::default()));
        mock.expect_put_configuration()
            .returning(|_| Err(anyhow!("oh no")));
        // act
        let usecase = UpdateConfiguration::new(Box::new(mock));
        let params = Params::new(Some(BucketNamingPolicy::RandomPool(10)));
        let result = usecase.call(params);
        // assert
        assert!(result.is_err());
    }
}
