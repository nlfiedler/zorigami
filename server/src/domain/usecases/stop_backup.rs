//
// Copyright (c) 2022 Nathan Fiedler
//
use crate::tasks::leader::RingLeader;
use anyhow::Error;
use std::cmp;
use std::fmt;
use std::sync::Arc;

pub struct StopBackup {
    leader: Arc<dyn RingLeader>,
}

impl StopBackup {
    pub fn new(leader: Arc<dyn RingLeader>) -> Self {
        Self { leader }
    }
}

impl super::UseCase<(), Params> for StopBackup {
    fn call(&self, params: Params) -> Result<(), Error> {
        if let Some(req) = self.leader.get_backup_by_dataset(&params.dataset_id) {
            self.leader.cancel_backup(req.id)
        } else {
            Ok(())
        }
    }
}

pub struct Params {
    /// Unique identifier of the dataset.
    dataset_id: String,
}

impl Params {
    pub fn new(dataset_id: String) -> Self {
        Self { dataset_id }
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
    use crate::tasks::backup::Request;
    use crate::tasks::leader::MockRingLeader;
    use anyhow::anyhow;

    #[test]
    fn test_stop_backup_ok() {
        // arrange
        let dataset_id = xid::new().to_string();
        let request = Request::new(dataset_id.clone(), "tiger", None);
        let mut leader = MockRingLeader::new();
        leader
            .expect_get_backup_by_dataset()
            .returning(move |_| Some(request.clone()));
        leader.expect_cancel_backup().returning(move |_| Ok(()));
        // act
        let usecase = StopBackup::new(Arc::new(leader));
        let params = Params { dataset_id };
        let result = usecase.call(params);
        // assert
        assert!(result.is_ok());
    }

    #[test]
    fn test_stop_backup_err() {
        // arrange
        let dataset_id = xid::new().to_string();
        let request = Request::new(dataset_id.clone(), "tiger", None);
        let mut leader = MockRingLeader::new();
        leader
            .expect_get_backup_by_dataset()
            .returning(move |_| Some(request.clone()));
        leader
            .expect_cancel_backup()
            .returning(move |_| Err(anyhow!("oh no")));
        // act
        let usecase = StopBackup::new(Arc::new(leader));
        let params = Params { dataset_id };
        let result = usecase.call(params);
        // assert
        assert!(result.is_err());
    }
}
