//
// Copyright (c) 2024 Nathan Fiedler
//
use crate::domain::managers::backup::Scheduler;
use crate::domain::managers::state::{RestorerState, StateStore};
use crate::domain::repositories::RecordRepository;
use anyhow::Error;
use log::{error, info};
use std::cmp;
use std::fmt;
use std::sync::Arc;

pub struct StartBackup {
    repo: Arc<dyn RecordRepository>,
    state: Arc<dyn StateStore>,
    processor: Arc<dyn Scheduler>,
}

impl StartBackup {
    pub fn new(
        repo: Arc<dyn RecordRepository>,
        state: Arc<dyn StateStore>,
        processor: Arc<dyn Scheduler>,
    ) -> Self {
        Self {
            repo,
            state,
            processor,
        }
    }
}

impl super::UseCase<(), Params> for StartBackup {
    fn call(&self, params: Params) -> Result<(), Error> {
        for dataset in self.repo.get_datasets()? {
            if dataset.id == params.dataset_id {
                info!("checking if backup can be started for {}", dataset.id);
                // ensure restorer is not doing anything right now
                let state = self.state.get_state();
                if state.restorer == RestorerState::Stopped {
                    // ensure supervisor has started, waiting for it to be ready
                    info!("starting backup for {}", dataset.id);
                    self.processor.start(self.repo.clone())?;
                    self.processor.start_backup(dataset.clone())?;
                    info!("backup started for {}", dataset.id);
                } else {
                    error!("restore in progress, cannot start backup");
                }
            }
        }
        Ok(())
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
    use crate::domain::entities::Dataset;
    use crate::domain::managers::backup::scheduler::MockScheduler;
    use crate::domain::managers::state::{self, MockStateStore};
    use crate::domain::repositories::MockRecordRepository;
    use anyhow::anyhow;
    use std::path::Path;

    #[test]
    fn test_start_backup_ok() {
        // arrange
        let datasets = vec![Dataset::new(Path::new("/home/planet"))];
        let dataset_id = datasets[0].id.clone();
        let mut repo = MockRecordRepository::new();
        repo.expect_get_datasets()
            .returning(move || Ok(datasets.clone()));
        let mut state = MockStateStore::new();
        state
            .expect_get_state()
            .returning(|| state::State::default());
        let mut processor = MockScheduler::new();
        processor.expect_start().returning(|_| Ok(()));
        processor.expect_start_backup().returning(|_| Ok(()));
        // act
        let usecase = StartBackup::new(Arc::new(repo), Arc::new(state), Arc::new(processor));
        let params = Params { dataset_id };
        let result = usecase.call(params);
        // assert
        assert!(result.is_ok());
    }

    #[test]
    fn test_start_backup_not_found() {
        // arrange
        let datasets = vec![Dataset::new(Path::new("/home/planet"))];
        let mut repo = MockRecordRepository::new();
        repo.expect_get_datasets()
            .returning(move || Ok(datasets.clone()));
        let state = MockStateStore::new();
        let processor = MockScheduler::new();
        // act
        let usecase = StartBackup::new(Arc::new(repo), Arc::new(state), Arc::new(processor));
        let params = Params {
            dataset_id: "nonesuch".to_owned(),
        };
        let result = usecase.call(params);
        // assert
        assert!(result.is_ok());
    }

    #[test]
    fn test_start_backup_err() {
        // arrange
        let mut repo = MockRecordRepository::new();
        repo.expect_get_datasets()
            .returning(|| Err(anyhow!("oh no")));
        let state = MockStateStore::new();
        let processor = MockScheduler::new();
        // act
        let usecase = StartBackup::new(Arc::new(repo), Arc::new(state), Arc::new(processor));
        let params = Params {
            dataset_id: "cafebabe".to_owned(),
        };
        let result = usecase.call(params);
        // assert
        assert!(result.is_err());
    }
}
