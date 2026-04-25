//
// Copyright (c) 2026 Nathan Fiedler
//
use crate::domain::entities::Configuration;
use crate::domain::repositories::RecordRepository;
use crate::domain::services::buckets::BucketNamingPolicy;
use anyhow::{Error, anyhow};
use std::cmp;
use std::fmt;

///
/// Update the application configuration. Read-only properties (hostname,
/// username, computer_id) cannot be changed via this use case.
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
        if let Some(bucket_naming) = params.bucket_naming {
            config.bucket_naming = bucket_naming;
        }
        if let Some(timezone) = params.timezone {
            if let Some(ref name) = timezone {
                name.parse::<chrono_tz::Tz>()
                    .map_err(|_| anyhow!("not a recognized IANA timezone: {}", name))?;
            }
            config.timezone = timezone;
        }
        self.repo.put_configuration(&config)?;
        Ok(config)
    }
}

/// Each field uses `Option<Option<T>>` to distinguish between "don't touch"
/// (outer `None`) and "set this value, possibly to None to clear" (outer
/// `Some`). This lets each setter mutation update its field independently.
pub struct Params {
    bucket_naming: Option<Option<BucketNamingPolicy>>,
    timezone: Option<Option<String>>,
}

impl Params {
    /// Build params that update the bucket naming policy.
    pub fn new(bucket_naming: Option<BucketNamingPolicy>) -> Self {
        Self {
            bucket_naming: Some(bucket_naming),
            timezone: None,
        }
    }

    /// Build params that update the timezone.
    pub fn with_timezone(timezone: Option<String>) -> Self {
        Self {
            bucket_naming: None,
            timezone: Some(timezone),
        }
    }
}

impl fmt::Display for Params {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Params(bucket_naming={:?}, timezone={:?})",
            self.bucket_naming, self.timezone
        )
    }
}

impl cmp::PartialEq for Params {
    fn eq(&self, other: &Self) -> bool {
        self.bucket_naming == other.bucket_naming && self.timezone == other.timezone
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
    fn test_update_configuration_set_timezone() {
        let mut mock = MockRecordRepository::new();
        mock.expect_get_configuration()
            .returning(|| Ok(Configuration::default()));
        mock.expect_put_configuration().returning(|config| {
            assert_eq!(config.timezone, Some("America/Los_Angeles".to_string()));
            Ok(())
        });
        let usecase = UpdateConfiguration::new(Box::new(mock));
        let params = Params::with_timezone(Some("America/Los_Angeles".to_string()));
        let result = usecase.call(params);
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap().timezone,
            Some("America/Los_Angeles".to_string())
        );
    }

    #[test]
    fn test_update_configuration_clear_timezone() {
        let mut mock = MockRecordRepository::new();
        mock.expect_get_configuration().returning(|| {
            Ok(Configuration {
                timezone: Some("Europe/Paris".to_string()),
                ..Default::default()
            })
        });
        mock.expect_put_configuration().returning(|config| {
            assert!(config.timezone.is_none());
            Ok(())
        });
        let usecase = UpdateConfiguration::new(Box::new(mock));
        let params = Params::with_timezone(None);
        let result = usecase.call(params);
        assert!(result.is_ok());
        assert!(result.unwrap().timezone.is_none());
    }

    #[test]
    fn test_update_configuration_invalid_timezone() {
        let mut mock = MockRecordRepository::new();
        mock.expect_get_configuration()
            .returning(|| Ok(Configuration::default()));
        let usecase = UpdateConfiguration::new(Box::new(mock));
        let params = Params::with_timezone(Some("Not/A_Real_Zone".to_string()));
        let result = usecase.call(params);
        assert!(result.is_err());
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
