//
// Copyright (c) 2024 Nathan Fiedler
//
use crate::domain::entities::schedule::Schedule;
use crate::domain::entities::Dataset;
use crate::domain::entities::RetentionPolicy;
use crate::domain::repositories::RecordRepository;
use anyhow::Error;
use std::cmp;
use std::fmt;
use std::path::PathBuf;

pub struct UpdateDataset {
    repo: Box<dyn RecordRepository>,
}

impl UpdateDataset {
    pub fn new(repo: Box<dyn RecordRepository>) -> Self {
        Self { repo }
    }
}

impl super::UseCase<Dataset, Params> for UpdateDataset {
    fn call(&self, params: Params) -> Result<Dataset, Error> {
        // use the constructor to leverage some of the default behavior in case
        // not everything has been defined in the params
        let mut dataset = Dataset::with_pack_size(&params.basepath, params.pack_size);
        dataset.id = params.id;
        dataset.excludes = params
            .excludes
            .into_iter()
            .map(|e| e.trim().to_owned())
            .filter(|e| !e.is_empty())
            .collect();
        for schedule in params.schedules {
            dataset.add_schedule(schedule);
        }
        for store in params.stores.iter() {
            dataset.add_store(store);
        }
        if let Some(workspace) = params.workspace {
            dataset.workspace = workspace;
        }
        dataset.retention = if let Some(count) = params.retention_count {
            RetentionPolicy::COUNT(count)
        } else {
            RetentionPolicy::ALL
        };
        self.repo.put_dataset(&dataset)?;
        Ok(dataset)
    }
}

pub struct Params {
    /// Unique identifier of this dataset.
    id: String,
    /// Local base path of dataset to be saved.
    basepath: PathBuf,
    /// Set of schedules for when to run the backup.
    schedules: Vec<Schedule>,
    /// Path for temporary pack building.
    workspace: Option<PathBuf>,
    /// Target size in bytes for pack files.
    pack_size: u64,
    /// Identifiers of the stores to contain pack files.
    stores: Vec<String>,
    /// List of file/directory exclusion patterns.
    excludes: Vec<String>,
    /// Number of snapshots to retain, if set.
    retention_count: Option<u16>,
}

impl Params {
    pub fn new(
        id: String,
        basepath: PathBuf,
        schedules: Vec<Schedule>,
        workspace: Option<PathBuf>,
        pack_size: u64,
        stores: Vec<String>,
        excludes: Vec<String>,
        retention_count: Option<u16>,
    ) -> Self {
        Self {
            id,
            basepath,
            schedules,
            workspace,
            pack_size,
            stores,
            excludes,
            retention_count,
        }
    }
}

impl fmt::Display for Params {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Params({})", self.id)
    }
}

impl cmp::PartialEq for Params {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
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
    fn test_update_dataset_ok() {
        // arrange
        let mut mock = MockRecordRepository::new();
        mock.expect_put_dataset().returning(|_| Ok(()));
        // act
        #[cfg(target_family="unix")]
        let basepath = "/home/planet";
        #[cfg(target_family="windows")]
        let basepath = "\\home\\planet";
        let usecase = UpdateDataset::new(Box::new(mock));
        let params = Params {
            id: "cafebabe".to_owned(),
            basepath: PathBuf::from(basepath),
            schedules: vec![],
            workspace: None,
            pack_size: 33_554_432,
            stores: vec!["cafebabe".to_owned()],
            excludes: vec![],
            retention_count: None,
        };
        let result = usecase.call(params);
        // assert
        assert!(result.is_ok());
        let actual = result.unwrap();
        assert_eq!(actual.basepath.to_string_lossy(), basepath);
        #[cfg(target_family="unix")]
        let expected_workspace = "/home/planet/.tmp";
        #[cfg(target_family="windows")]
        let expected_workspace = "\\home\\planet\\.tmp";
        assert_eq!(actual.workspace.to_string_lossy(), expected_workspace);
    }

    #[test]
    fn test_update_dataset_workspace() {
        // arrange
        let mut mock = MockRecordRepository::new();
        mock.expect_put_dataset().returning(|_| Ok(()));
        // act
        #[cfg(target_family="unix")]
        let basepath = "/home/planet";
        #[cfg(target_family="windows")]
        let basepath = "\\home\\planet";
        #[cfg(target_family="unix")]
        let workspace = "/home/planet/tmpdir";
        #[cfg(target_family="windows")]
        let workspace = "\\home\\planet\\tmpdir";
        let usecase = UpdateDataset::new(Box::new(mock));
        let params = Params {
            id: "cafebabe".to_owned(),
            basepath: PathBuf::from(basepath),
            schedules: vec![],
            workspace: Some(PathBuf::from(workspace)),
            pack_size: 33_554_432,
            stores: vec!["cafebabe".to_owned()],
            excludes: vec![],
            retention_count: None,
        };
        let result = usecase.call(params);
        // assert
        assert!(result.is_ok());
        let actual = result.unwrap();
        assert_eq!(actual.basepath.to_string_lossy(), basepath);
        assert_eq!(actual.workspace.to_string_lossy(), workspace);
    }

    #[test]
    fn test_update_dataset_empty_excludes() {
        // arrange
        let mut mock = MockRecordRepository::new();
        mock.expect_put_dataset().returning(|_| Ok(()));
        // act
        #[cfg(target_family="unix")]
        let basepath = "/home/planet";
        #[cfg(target_family="windows")]
        let basepath = "\\home\\planet";
        let usecase = UpdateDataset::new(Box::new(mock));
        let params = Params {
            id: "cafebabe".to_owned(),
            basepath: PathBuf::from(basepath),
            workspace: None,
            schedules: vec![],
            pack_size: 33_554_432,
            stores: vec!["cafebabe".to_owned()],
            excludes: vec!["".to_owned()],
            retention_count: None,
        };
        let result = usecase.call(params);
        // assert
        assert!(result.is_ok());
        let actual = result.unwrap();
        assert_eq!(actual.basepath.to_string_lossy(), basepath);
        #[cfg(target_family="unix")]
        let expected_workspace = "/home/planet/.tmp";
        #[cfg(target_family="windows")]
        let expected_workspace = "\\home\\planet\\.tmp";
        assert_eq!(actual.workspace.to_string_lossy(), expected_workspace);
        assert_eq!(actual.pack_size, 33_554_432);
        assert_eq!(actual.stores.len(), 1);
        assert_eq!(actual.stores[0], "cafebabe");
        assert_eq!(actual.excludes.len(), 0);
    }

    #[test]
    fn test_update_dataset_err() {
        // arrange
        let mut mock = MockRecordRepository::new();
        mock.expect_put_dataset()
            .returning(|_| Err(anyhow!("oh no")));
        // act
        let usecase = UpdateDataset::new(Box::new(mock));
        let params = Params {
            id: "cafebabe".to_owned(),
            basepath: PathBuf::from("/home/planet"),
            schedules: vec![],
            workspace: None,
            pack_size: 33_554_432,
            stores: vec!["cafebabe".to_owned()],
            excludes: vec![],
            retention_count: None,
        };
        let result = usecase.call(params);
        // assert
        assert!(result.is_err());
    }
}
