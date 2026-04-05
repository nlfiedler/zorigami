//
// Copyright (c) 2024 Nathan Fiedler
//
use crate::tasks::leader::RingLeader;
use anyhow::Error;
use std::cmp;
use std::fmt;
use std::sync::Arc;

pub struct RestoreDatabase {
    leader: Arc<dyn RingLeader>,
}

impl RestoreDatabase {
    pub fn new(leader: Arc<dyn RingLeader>) -> Self {
        Self { leader }
    }
}

impl super::UseCase<String, Params> for RestoreDatabase {
    fn call(&self, params: Params) -> Result<String, Error> {
        let result = self
            .leader
            .restore_database(params.store_id, params.passphrase);
        result.map(|_| String::from("ok"))
    }
}

pub struct Params {
    /// Identifier of the pack store from which to retrieve the database.
    store_id: String,
    /// Pass phrase for decrypting the pack.
    passphrase: String,
}

impl Params {
    pub fn new<T: Into<String>>(store_id: T, passphrase: T) -> Self {
        Self {
            store_id: store_id.into(),
            passphrase: passphrase.into(),
        }
    }
}

impl fmt::Display for Params {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Params({})", self.store_id)
    }
}

impl cmp::PartialEq for Params {
    fn eq(&self, other: &Self) -> bool {
        self.store_id == other.store_id
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
    fn test_restore_database_ok() {
        // arrange
        let mut mock = MockRingLeader::new();
        mock.expect_restore_database().returning(move |_, _| Ok(()));
        // act
        let usecase = RestoreDatabase::new(Arc::new(mock));
        let params = Params::new("cafebabe", "Secret123");
        let result = usecase.call(params);
        // assert
        assert!(result.is_ok());
        let actual = result.unwrap();
        assert_eq!(actual, "ok");
    }

    #[test]
    fn test_restore_database_no_database_err() {
        // arrange
        let mut mock = MockRingLeader::new();
        mock.expect_restore_database()
            .returning(move |_, _| Err(anyhow!("no database archives available")));
        // act
        let usecase = RestoreDatabase::new(Arc::new(mock));
        let params = Params::new("cafebabe", "Secret123");
        let result = usecase.call(params);
        // assert
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("no database archives available"));
    }
}
