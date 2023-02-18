//
// Copyright (c) 2020 Nathan Fiedler
//
use crate::domain::managers::state::{StateStore, SupervisorAction};
use crate::domain::repositories::RecordRepository;
use anyhow::{anyhow, Error};
use log::{debug, error, info, log_enabled, Level};
use std::cmp;
use std::fmt;
use std::sync::Arc;

pub struct RestoreDatabase {
    repo: Box<dyn RecordRepository>,
}

impl RestoreDatabase {
    pub fn new(repo: Box<dyn RecordRepository>) -> Self {
        Self { repo }
    }
}

impl super::UseCase<String, Params> for RestoreDatabase {
    fn call(&self, params: Params) -> Result<String, Error> {
        if log_enabled!(Level::Debug) {
            if let Ok(datasets) = self.repo.get_datasets() {
                debug!("datasets before: {:?}", datasets);
            }
        }
        // Signal the supervisor to stop and wait for that to happen.
        info!("stopping backup supervisor...");
        params.state.supervisor_event(SupervisorAction::Stop);
        params.state.wait_for_supervisor(SupervisorAction::Stopped);
        let result = if let Some(store) = self.repo.get_store(&params.store_id)? {
            info!("found store {}", store.id);
            let pack_repo = self.repo.build_pack_repo(&store)?;
            let config = self.repo.get_configuration()?;
            let archive_file = tempfile::NamedTempFile::new()?;
            let archive_path = archive_file.into_temp_path();
            info!("retrieving latest database snapshot...");
            pack_repo.retrieve_latest_database(&config.computer_id, &archive_path)?;
            // By this point we can safely assume that the backup supervisor has
            // completely shut down and released its reference to the database.
            //
            // However, this may still fail if a backup operation is currently
            // in progress, in which case the user will need to either wait or
            // stop the backup before trying again. Of course, a running backup
            // would be unlikely given the use case scenario.
            info!("restoring database from backup...");
            self.repo.restore_from_backup(&archive_path)
        } else {
            Err(anyhow!("no pack stores defined"))
        };
        // Signal the processor to start a new backup supervisor.
        info!("starting backup supervisor again...");
        params.state.supervisor_event(SupervisorAction::Start);
        info!("database restore complete");
        if log_enabled!(Level::Debug) {
            if let Ok(datasets) = self.repo.get_datasets() {
                debug!("datasets after: {:?}", datasets);
            }
        }
        if let Err(err) = result {
            error!("database restore failed: {}", err);
            Err(err)
        } else {
            result.map(|_| String::from("ok"))
        }
    }
}

pub struct Params {
    /// Identifier of the pack store from which to retrieve the database.
    store_id: String,
    /// Reference to the application state store.
    state: Arc<dyn StateStore>,
}

impl Params {
    pub fn new<T: Into<String>>(store_id: T, state: Arc<dyn StateStore>) -> Self {
        Self {
            store_id: store_id.into(),
            state,
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
    use crate::domain::entities::{Configuration, Store, StoreType};
    use crate::domain::managers::state::MockStateStore;
    use crate::domain::repositories::{MockPackRepository, MockRecordRepository};
    use mockall::predicate::*;
    use std::collections::HashMap;

    #[test]
    fn test_restore_database_ok() {
        // arrange
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("basepath".to_owned(), "/home/planet".to_owned());
        let store = Store {
            id: "cafebabe".to_owned(),
            store_type: StoreType::LOCAL,
            label: "mylocalstore".to_owned(),
            properties,
        };
        let mut mock = MockRecordRepository::new();
        mock.expect_get_store()
            .with(eq("cafebabe"))
            .returning(move |_| Ok(Some(store.clone())));
        mock.expect_build_pack_repo().returning(move |_| {
            let mut mock_store = MockPackRepository::new();
            mock_store
                .expect_retrieve_latest_database()
                .returning(move |_, _| Ok(()));
            Ok(Box::new(mock_store))
        });
        let config: Configuration = Default::default();
        mock.expect_get_configuration()
            .returning(move || Ok(config.clone()));
        mock.expect_restore_from_backup().returning(|_| Ok(()));
        let mut stater = MockStateStore::new();
        stater
            .expect_supervisor_event()
            .with(eq(SupervisorAction::Stop))
            .return_const(());
        stater
            .expect_wait_for_supervisor()
            .with(eq(SupervisorAction::Stopped))
            .return_const(());
        stater
            .expect_supervisor_event()
            .with(eq(SupervisorAction::Start))
            .return_const(());
        let appstate: Arc<dyn StateStore> = Arc::new(stater);
        // act
        let usecase = RestoreDatabase::new(Box::new(mock));
        let params = Params::new("cafebabe", appstate);
        let result = usecase.call(params);
        // assert
        assert!(result.is_ok());
        let actual = result.unwrap();
        assert_eq!(actual, "ok");
    }

    #[test]
    fn test_restore_database_no_database_err() {
        // arrange
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("basepath".to_owned(), "/home/planet".to_owned());
        let store = Store {
            id: "cafebabe".to_owned(),
            store_type: StoreType::LOCAL,
            label: "mylocalstore".to_owned(),
            properties,
        };
        let mut mock = MockRecordRepository::new();
        mock.expect_get_store()
            .with(eq("cafebabe"))
            .returning(move |_| Ok(Some(store.clone())));
        mock.expect_build_pack_repo().returning(move |_| {
            let mut mock_store = MockPackRepository::new();
            mock_store
                .expect_retrieve_latest_database()
                .returning(move |_, _| Ok(()));
            Ok(Box::new(mock_store))
        });
        let config: Configuration = Default::default();
        mock.expect_get_configuration()
            .returning(move || Ok(config.clone()));
        mock.expect_restore_from_backup()
            .returning(|_| Err(anyhow!("no database archives available")));
        let mut stater = MockStateStore::new();
        stater
            .expect_supervisor_event()
            .with(eq(SupervisorAction::Stop))
            .return_const(());
        stater
            .expect_wait_for_supervisor()
            .with(eq(SupervisorAction::Stopped))
            .return_const(());
        stater
            .expect_supervisor_event()
            .with(eq(SupervisorAction::Start))
            .return_const(());
        let appstate: Arc<dyn StateStore> = Arc::new(stater);
        // act
        let usecase = RestoreDatabase::new(Box::new(mock));
        let params = Params::new("cafebabe", appstate);
        let result = usecase.call(params);
        // assert
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("no database archives available"));
    }

    #[test]
    fn test_restore_database_no_stores_err() {
        // arrange
        let mut mock = MockRecordRepository::new();
        mock.expect_get_store()
            .with(eq("cafebabe"))
            .returning(move |_| Ok(None));
        let mut stater = MockStateStore::new();
        stater
            .expect_supervisor_event()
            .with(eq(SupervisorAction::Stop))
            .return_const(());
        stater
            .expect_wait_for_supervisor()
            .with(eq(SupervisorAction::Stopped))
            .return_const(());
        stater
            .expect_supervisor_event()
            .with(eq(SupervisorAction::Start))
            .return_const(());
        let appstate: Arc<dyn StateStore> = Arc::new(stater);
        // act
        let usecase = RestoreDatabase::new(Box::new(mock));
        let params = Params::new("cafebabe", appstate);
        let result = usecase.call(params);
        // assert
        assert!(result.is_err());
    }
}
