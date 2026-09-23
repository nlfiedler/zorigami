//
// Copyright (c) 2026 Nathan Fiedler
//
use crate::domain::entities::BackgroundOperation;
use crate::domain::repositories::RecordRepository;
use crate::shared::packs;
use crate::tasks::leader::RingLeader;
use crate::tasks::prune;
use anyhow::{Error, anyhow};
use std::cmp;
use std::fmt;
use std::sync::Arc;

/// Start one of the periodic background tasks now, rather than waiting for
/// its interval to elapse.
pub struct StartTask {
    repo: Box<dyn RecordRepository>,
    leader: Arc<dyn RingLeader>,
}

impl StartTask {
    pub fn new(repo: Box<dyn RecordRepository>, leader: Arc<dyn RingLeader>) -> Self {
        Self { repo, leader }
    }
}

impl super::UseCase<(), Params> for StartTask {
    fn call(&self, params: Params) -> Result<(), Error> {
        match params.operation {
            BackgroundOperation::Prune => {
                // same as the scheduler, one request per dataset
                for set in self.repo.get_datasets()? {
                    self.leader.prune(prune::Request::new(set.id))?;
                }
                Ok(())
            }
            BackgroundOperation::RestoreTest => self.leader.restore_test(packs::get_passphrase()),
            BackgroundOperation::DatabaseScrub => self.leader.database_scrub(),
            BackgroundOperation::PackPrune => self.leader.prune_packs(),
            BackgroundOperation::WorkspaceCleanup => self.leader.cleanup_workspaces(),
            // backups are started per dataset via StartBackup
            BackgroundOperation::Backup => Err(anyhow!("use startBackup to start a backup")),
        }
    }
}

pub struct Params {
    /// The background task to be started.
    operation: BackgroundOperation,
}

impl Params {
    pub fn new(operation: BackgroundOperation) -> Self {
        Self { operation }
    }
}

impl fmt::Display for Params {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Params({})", self.operation)
    }
}

impl cmp::PartialEq for Params {
    fn eq(&self, other: &Self) -> bool {
        self.operation == other.operation
    }
}

impl cmp::Eq for Params {}

#[cfg(test)]
mod tests {
    use super::super::UseCase;
    use super::*;
    use crate::domain::entities::Dataset;
    use crate::domain::repositories::MockRecordRepository;
    use crate::tasks::leader::MockRingLeader;
    use std::path::Path;

    #[test]
    fn test_start_task_prune_each_dataset() {
        // arrange
        let mut repo = MockRecordRepository::new();
        repo.expect_get_datasets().returning(|| {
            Ok(vec![
                Dataset::new(Path::new("/home/planet")),
                Dataset::new(Path::new("/home/express")),
            ])
        });
        let mut leader = MockRingLeader::new();
        leader.expect_prune().times(2).returning(|_| Ok(()));
        // act
        let usecase = StartTask::new(Box::new(repo), Arc::new(leader));
        let result = usecase.call(Params::new(BackgroundOperation::Prune));
        // assert
        assert!(result.is_ok());
    }

    #[test]
    fn test_start_task_database_scrub() {
        // arrange
        let repo = MockRecordRepository::new();
        let mut leader = MockRingLeader::new();
        leader.expect_database_scrub().times(1).returning(|| Ok(()));
        // act
        let usecase = StartTask::new(Box::new(repo), Arc::new(leader));
        let result = usecase.call(Params::new(BackgroundOperation::DatabaseScrub));
        // assert
        assert!(result.is_ok());
    }

    #[test]
    fn test_start_task_pack_prune_err() {
        // arrange
        let repo = MockRecordRepository::new();
        let mut leader = MockRingLeader::new();
        leader
            .expect_prune_packs()
            .returning(|| Err(anyhow!("oh no")));
        // act
        let usecase = StartTask::new(Box::new(repo), Arc::new(leader));
        let result = usecase.call(Params::new(BackgroundOperation::PackPrune));
        // assert
        assert!(result.is_err());
    }

    #[test]
    fn test_start_task_backup_rejected() {
        // arrange
        let repo = MockRecordRepository::new();
        let leader = MockRingLeader::new();
        // act
        let usecase = StartTask::new(Box::new(repo), Arc::new(leader));
        let result = usecase.call(Params::new(BackgroundOperation::Backup));
        // assert
        assert!(result.is_err());
    }
}
