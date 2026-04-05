//
// Copyright (c) 2024 Nathan Fiedler
//
use crate::tasks::backup::Request;
use crate::tasks::leader::RingLeader;
use anyhow::Error;
use std::cmp;
use std::fmt;
use std::sync::Arc;

pub struct StartBackup {
    leader: Arc<dyn RingLeader>,
}

impl StartBackup {
    pub fn new(leader: Arc<dyn RingLeader>) -> Self {
        Self { leader }
    }
}

impl super::UseCase<(), Params> for StartBackup {
    fn call(&self, params: Params) -> Result<(), Error> {
        self.leader
            .backup(Request::new(params.dataset_id, params.passphrase, None))?;
        Ok(())
    }
}

pub struct Params {
    /// Unique identifier of the dataset.
    dataset_id: String,
    /// Pass phrase for encrypting pack files.
    passphrase: String,
}

impl Params {
    pub fn new(dataset_id: impl Into<String>, passphrase: impl Into<String>) -> Self {
        Self {
            dataset_id: dataset_id.into(),
            passphrase: passphrase.into(),
        }
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
    use crate::tasks::leader::MockRingLeader;
    use anyhow::anyhow;

    #[test]
    fn test_start_backup_ok() {
        // arrange
        let dataset_id = xid::new().to_string();
        let mut leader = MockRingLeader::new();
        leader.expect_backup().returning(move |_| Ok(()));
        // act
        let usecase = StartBackup::new(Arc::new(leader));
        let params = Params::new(dataset_id, "tiger");
        let result = usecase.call(params);
        // assert
        assert!(result.is_ok());
    }

    #[test]
    fn test_start_backup_err() {
        // arrange
        let dataset_id = xid::new().to_string();
        let mut leader = MockRingLeader::new();
        leader
            .expect_backup()
            .returning(move |_| Err(anyhow!("oh no")));
        // act
        let usecase = StartBackup::new(Arc::new(leader));
        let params = Params::new(dataset_id, "tiger");
        let result = usecase.call(params);
        // assert
        assert!(result.is_err());
    }
}
