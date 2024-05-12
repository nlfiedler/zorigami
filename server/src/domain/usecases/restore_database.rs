//
// Copyright (c) 2024 Nathan Fiedler
//
use crate::domain::managers::state::{RestorerAction, StateStore, SupervisorAction};
use crate::domain::repositories::RecordRepository;
use anyhow::{anyhow, Error};
use log::{debug, error, info, log_enabled, Level};
use std::borrow::Cow;
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

impl<'a> super::UseCase<String, Params<'a>> for RestoreDatabase {
    fn call(&self, params: Params) -> Result<String, Error> {
        if log_enabled!(Level::Debug) {
            if let Ok(datasets) = self.repo.get_datasets() {
                debug!("datasets before: {:?}", datasets);
            }
        }
        // Signal the supervisors to stop and wait for that to happen.
        info!("stopping backup supervisor...");
        params.state.stop_supervisor();
        info!("stopping restore supervisor...");
        params.state.stop_restorer();
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
            self.repo.restore_from_backup(&archive_path, &params.passphrase)
        } else {
            Err(anyhow!("no pack stores defined"))
        };
        if let Err(err) = result {
            error!("database restore failed: {}", err);
            Err(err)
        } else {
            // Signal the processor to start a new backup supervisor.
            info!("starting backup supervisor again...");
            params.state.supervisor_event(SupervisorAction::Start);
            info!("starting restore supervisor again...");
            params.state.restorer_event(RestorerAction::Start);
            info!("database restore complete");
            if log_enabled!(Level::Debug) {
                if let Ok(datasets) = self.repo.get_datasets() {
                    debug!("datasets after: {:?}", datasets);
                }
            }
            result.map(|_| String::from("ok"))
        }
    }
}

pub struct Params<'a> {
    /// Identifier of the pack store from which to retrieve the database.
    store_id: String,
    /// Reference to the application state store.
    state: Arc<dyn StateStore>,
    /// Pass phrase for decrypting the pack.
    passphrase: Cow<'a, str>,
}

impl<'a> Params<'a> {
    pub fn new<T: Into<String>>(store_id: T, state: Arc<dyn StateStore>, passphrase: T) -> Self {
        Self {
            store_id: store_id.into(),
            state,
            passphrase: Cow::from(passphrase.into()),
        }
    }
}

impl<'a> fmt::Display for Params<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Params({})", self.store_id)
    }
}

impl<'a> cmp::PartialEq for Params<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.store_id == other.store_id
    }
}

impl<'a> cmp::Eq for Params<'a> {}

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
        mock.expect_restore_from_backup().returning(|_, _| Ok(()));
        let mut stater = MockStateStore::new();
        stater
            .expect_stop_supervisor()
            .return_const(());
        stater
            .expect_supervisor_event()
            .with(eq(SupervisorAction::Start))
            .return_const(());
        stater
            .expect_stop_restorer()
            .return_const(());
        stater
            .expect_restorer_event()
            .with(eq(RestorerAction::Start))
            .return_const(());
        let appstate: Arc<dyn StateStore> = Arc::new(stater);
        // act
        let usecase = RestoreDatabase::new(Box::new(mock));
        let params = Params::new("cafebabe", appstate, "Secret123");
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
            .returning(|_, _| Err(anyhow!("no database archives available")));
        let mut stater = MockStateStore::new();
        stater
            .expect_stop_supervisor()
            .return_const(());
        stater
            .expect_supervisor_event()
            .with(eq(SupervisorAction::Start))
            .return_const(());
        stater
            .expect_stop_restorer()
            .return_const(());
        stater
            .expect_restorer_event()
            .with(eq(RestorerAction::Start))
            .return_const(());
        let appstate: Arc<dyn StateStore> = Arc::new(stater);
        // act
        let usecase = RestoreDatabase::new(Box::new(mock));
        let params = Params::new("cafebabe", appstate, "Secret123");
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
            .expect_stop_supervisor()
            .return_const(());
        stater
            .expect_supervisor_event()
            .with(eq(SupervisorAction::Start))
            .return_const(());
        stater
            .expect_stop_restorer()
            .return_const(());
        stater
            .expect_restorer_event()
            .with(eq(RestorerAction::Start))
            .return_const(());
        let appstate: Arc<dyn StateStore> = Arc::new(stater);
        // act
        let usecase = RestoreDatabase::new(Box::new(mock));
        let params = Params::new("cafebabe", appstate, "Secret123");
        let result = usecase.call(params);
        // assert
        assert!(result.is_err());
    }
}
