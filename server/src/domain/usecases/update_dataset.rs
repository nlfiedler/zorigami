//
// Copyright (c) 2024 Nathan Fiedler
//
use crate::domain::entities::schedule::Schedule;
use crate::domain::entities::Dataset;
use crate::domain::entities::SnapshotRetention;
use crate::domain::repositories::RecordRepository;
use anyhow::{anyhow, Error};
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
        // read the existing dataset and overwrite certain properties, retaining
        // hidden values like the latest snapshot
        let mut dataset = self
            .repo
            .get_dataset(&params.id)?
            .ok_or_else(|| anyhow!("no such dataset: {}", params.id))?;
        dataset.basepath = params.basepath;
        dataset.pack_size = params.pack_size;
        dataset.excludes = params
            .excludes
            .into_iter()
            .map(|e| e.trim().to_owned())
            .filter(|e| !e.is_empty())
            .collect();
        dataset.schedules = params.schedules;
        dataset.stores = params.stores;
        if let Some(workspace) = params.workspace {
            dataset.workspace = workspace;
        }
        dataset.retention = if let Some(count) = params.retention_count {
            SnapshotRetention::COUNT(count)
        } else {
            SnapshotRetention::ALL
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
        #[cfg(target_family = "unix")]
        let basepath = PathBuf::from("/home/planet");
        #[cfg(target_family = "windows")]
        let basepath = PathBuf::from("C:\\home\\planet");
        let basepath_copy = basepath.clone();
        let mut mock = MockRecordRepository::new();
        mock.expect_get_dataset()
            .returning(move |_| Ok(Some(Dataset::new(&basepath_copy))));
        mock.expect_put_dataset().returning(|_| Ok(()));
        // act
        let usecase = UpdateDataset::new(Box::new(mock));
        let params = Params {
            id: "cafebabe".to_owned(),
            basepath: basepath.clone(),
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
        assert_eq!(actual.basepath, basepath);
        #[cfg(target_family = "unix")]
        let expected_workspace = "/home/planet/.tmp";
        #[cfg(target_family = "windows")]
        let expected_workspace = "C:\\home\\planet\\.tmp";
        assert_eq!(actual.workspace.to_string_lossy(), expected_workspace);
    }

    #[test]
    fn test_update_dataset_workspace() {
        // arrange
        #[cfg(target_family = "unix")]
        let basepath = PathBuf::from("/home/planet");
        #[cfg(target_family = "windows")]
        let basepath = PathBuf::from("C:\\home\\planet");
        let basepath_copy = basepath.clone();
        let mut mock = MockRecordRepository::new();
        mock.expect_get_dataset()
            .returning(move |_| Ok(Some(Dataset::new(&basepath_copy))));
        mock.expect_put_dataset().returning(|_| Ok(()));
        // act
        #[cfg(target_family = "unix")]
        let workspace = PathBuf::from("/home/planet/tmpdir");
        #[cfg(target_family = "windows")]
        let workspace = PathBuf::from("C:\\home\\planet\\tmpdir");
        let usecase = UpdateDataset::new(Box::new(mock));
        let params = Params {
            id: "cafebabe".to_owned(),
            basepath: basepath.clone(),
            schedules: vec![],
            workspace: Some(workspace.clone()),
            pack_size: 33_554_432,
            stores: vec!["cafebabe".to_owned()],
            excludes: vec![],
            retention_count: None,
        };
        let result = usecase.call(params);
        // assert
        assert!(result.is_ok());
        let actual = result.unwrap();
        assert_eq!(actual.basepath, basepath);
        assert_eq!(actual.workspace, workspace);
    }

    #[test]
    fn test_update_dataset_empty_excludes() {
        // arrange
        #[cfg(target_family = "unix")]
        let basepath = PathBuf::from("/home/planet");
        #[cfg(target_family = "windows")]
        let basepath = PathBuf::from("C:\\home\\planet");
        let basepath_copy = basepath.clone();
        let mut mock = MockRecordRepository::new();
        mock.expect_get_dataset()
            .returning(move |_| Ok(Some(Dataset::new(&basepath_copy))));
        mock.expect_put_dataset().returning(|_| Ok(()));
        // act
        let usecase = UpdateDataset::new(Box::new(mock));
        let params = Params {
            id: "cafebabe".to_owned(),
            basepath: basepath.clone(),
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
        assert_eq!(actual.basepath, basepath);
        #[cfg(target_family = "unix")]
        let expected_workspace = PathBuf::from("/home/planet/.tmp");
        #[cfg(target_family = "windows")]
        let expected_workspace = PathBuf::from("C:\\home\\planet\\.tmp");
        assert_eq!(actual.workspace, expected_workspace);
        assert_eq!(actual.pack_size, 33_554_432);
        assert_eq!(actual.stores.len(), 1);
        assert_eq!(actual.stores[0], "cafebabe");
        assert_eq!(actual.excludes.len(), 0);
    }

    #[test]
    fn test_update_dataset_err() {
        // arrange
        #[cfg(target_family = "unix")]
        let basepath = PathBuf::from("/home/planet");
        #[cfg(target_family = "windows")]
        let basepath = PathBuf::from("C:\\home\\planet");
        let basepath_copy = basepath.clone();
        let mut mock = MockRecordRepository::new();
        mock.expect_get_dataset()
            .returning(move |_| Ok(Some(Dataset::new(&basepath_copy))));
        mock.expect_put_dataset()
            .returning(|_| Err(anyhow!("oh no")));
        // act
        let usecase = UpdateDataset::new(Box::new(mock));
        let params = Params {
            id: "cafebabe".to_owned(),
            basepath: basepath,
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
