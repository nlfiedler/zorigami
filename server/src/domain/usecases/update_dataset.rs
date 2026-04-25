//
// Copyright (c) 2024 Nathan Fiedler
//
use crate::domain::entities::Dataset;
use crate::domain::entities::SnapshotRetention;
use crate::domain::entities::schedule::Schedule;
use crate::domain::repositories::RecordRepository;
use anyhow::{Error, anyhow};
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
        let meta = std::fs::metadata(&params.basepath).map_err(|e| {
            anyhow!(
                "basepath not accessible: {}: {}",
                params.basepath.display(),
                e
            )
        })?;
        if !meta.is_dir() {
            return Err(anyhow!(
                "basepath is not a directory: {}",
                params.basepath.display()
            ));
        }
        // read the existing dataset and overwrite certain properties, retaining
        // hidden values like the latest snapshot
        let mut dataset = self
            .repo
            .get_dataset(&params.id)?
            .ok_or_else(|| anyhow!("no such dataset: {}", params.id))?;
        if dataset.basepath != params.basepath && dataset.snapshot.is_some() {
            return Err(anyhow!(
                "cannot change basepath of dataset {} after snapshots have been created",
                params.id
            ));
        }
        dataset.basepath = params.basepath;
        dataset.chunk_size = params.chunk_size;
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
        dataset.retention = params.retention;
        self.repo.put_dataset(&dataset)?;
        Ok(dataset)
    }
}

#[derive(Debug)]
pub struct Params {
    /// Unique identifier of this dataset.
    id: String,
    /// Local base path of dataset to be saved.
    basepath: PathBuf,
    /// Set of schedules for when to run the backup.
    schedules: Vec<Schedule>,
    /// Path for temporary pack building.
    workspace: Option<PathBuf>,
    /// Target size in bytes for content-defined chunks.
    chunk_size: usize,
    /// Target size in bytes for pack files.
    pack_size: u64,
    /// Identifiers of the stores to contain pack files.
    stores: Vec<String>,
    /// List of file/directory exclusion patterns.
    excludes: Vec<String>,
    /// Snapshot retention policy.
    retention: SnapshotRetention,
}

impl Params {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: String,
        basepath: PathBuf,
        schedules: Vec<Schedule>,
        workspace: Option<PathBuf>,
        chunk_size: usize,
        pack_size: u64,
        stores: Vec<String>,
        excludes: Vec<String>,
        retention: SnapshotRetention,
    ) -> Self {
        Self {
            id,
            basepath,
            schedules,
            workspace,
            chunk_size,
            pack_size,
            stores,
            excludes,
            retention,
        }
    }
}

impl From<Dataset> for Params {
    fn from(val: Dataset) -> Self {
        Params::new(
            val.id,
            val.basepath,
            val.schedules,
            Some(val.workspace),
            val.chunk_size,
            val.pack_size,
            val.stores,
            val.excludes,
            val.retention,
        )
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
    use crate::domain::entities::Checksum;
    use crate::domain::repositories::MockRecordRepository;
    use anyhow::anyhow;

    #[test]
    fn test_update_dataset_ok() {
        // arrange
        let tmp = tempfile::tempdir().unwrap();
        let basepath = tmp.path().to_path_buf();
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
            chunk_size: 1_048_576,
            pack_size: 33_554_432,
            stores: vec!["cafebabe".to_owned()],
            excludes: vec![],
            retention: SnapshotRetention::ALL,
        };
        let result = usecase.call(params);
        // assert
        assert!(result.is_ok());
        let actual = result.unwrap();
        assert_eq!(actual.basepath, basepath);
        assert_eq!(actual.workspace, basepath.join(".tmp"));
    }

    #[test]
    fn test_update_dataset_workspace() {
        // arrange
        let tmp = tempfile::tempdir().unwrap();
        let basepath = tmp.path().to_path_buf();
        let basepath_copy = basepath.clone();
        let mut mock = MockRecordRepository::new();
        mock.expect_get_dataset()
            .returning(move |_| Ok(Some(Dataset::new(&basepath_copy))));
        mock.expect_put_dataset().returning(|_| Ok(()));
        // act
        let workspace = basepath.join("tmpdir");
        let usecase = UpdateDataset::new(Box::new(mock));
        let params = Params {
            id: "cafebabe".to_owned(),
            basepath: basepath.clone(),
            schedules: vec![],
            workspace: Some(workspace.clone()),
            chunk_size: 1_048_576,
            pack_size: 33_554_432,
            stores: vec!["cafebabe".to_owned()],
            excludes: vec![],
            retention: SnapshotRetention::ALL,
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
        let tmp = tempfile::tempdir().unwrap();
        let basepath = tmp.path().to_path_buf();
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
            chunk_size: 1_048_576,
            pack_size: 33_554_432,
            stores: vec!["cafebabe".to_owned()],
            excludes: vec!["".to_owned()],
            retention: SnapshotRetention::ALL,
        };
        let result = usecase.call(params);
        // assert
        assert!(result.is_ok());
        let actual = result.unwrap();
        assert_eq!(actual.basepath, basepath);
        assert_eq!(actual.workspace, basepath.join(".tmp"));
        assert_eq!(actual.pack_size, 33_554_432);
        assert_eq!(actual.stores.len(), 1);
        assert_eq!(actual.stores[0], "cafebabe");
        assert_eq!(actual.excludes.len(), 0);
    }

    #[test]
    fn test_update_dataset_err() {
        // arrange
        let tmp = tempfile::tempdir().unwrap();
        let basepath = tmp.path().to_path_buf();
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
            basepath,
            schedules: vec![],
            workspace: None,
            chunk_size: 1_048_576,
            pack_size: 33_554_432,
            stores: vec!["cafebabe".to_owned()],
            excludes: vec![],
            retention: SnapshotRetention::ALL,
        };
        let result = usecase.call(params);
        // assert
        assert!(result.is_err());
    }

    #[test]
    fn test_update_dataset_basepath_missing() {
        // arrange
        let mock = MockRecordRepository::new();
        // act
        let usecase = UpdateDataset::new(Box::new(mock));
        let params = Params {
            id: "cafebabe".to_owned(),
            basepath: PathBuf::from("/definitely/does/not/exist/zorigami-test"),
            schedules: vec![],
            workspace: None,
            chunk_size: 1_048_576,
            pack_size: 33_554_432,
            stores: vec!["cafebabe".to_owned()],
            excludes: vec![],
            retention: SnapshotRetention::ALL,
        };
        let result = usecase.call(params);
        // assert
        assert!(result.is_err());
        let msg = format!("{}", result.unwrap_err());
        assert!(msg.contains("basepath"));
    }

    #[test]
    fn test_update_dataset_basepath_change_with_snapshot() {
        // arrange
        let old_tmp = tempfile::tempdir().unwrap();
        let old_basepath = old_tmp.path().to_path_buf();
        let new_tmp = tempfile::tempdir().unwrap();
        let new_basepath = new_tmp.path().to_path_buf();
        let mut mock = MockRecordRepository::new();
        mock.expect_get_dataset().returning(move |_| {
            let mut ds = Dataset::new(&old_basepath);
            ds.snapshot = Some(Checksum::SHA1("abc123".to_owned()));
            Ok(Some(ds))
        });
        mock.expect_put_dataset().never();
        // act
        let usecase = UpdateDataset::new(Box::new(mock));
        let params = Params {
            id: "cafebabe".to_owned(),
            basepath: new_basepath,
            schedules: vec![],
            workspace: None,
            chunk_size: 1_048_576,
            pack_size: 33_554_432,
            stores: vec!["cafebabe".to_owned()],
            excludes: vec![],
            retention: SnapshotRetention::ALL,
        };
        let result = usecase.call(params);
        // assert
        assert!(result.is_err());
        let msg = format!("{}", result.unwrap_err());
        assert!(msg.contains("snapshots"));
    }

    #[test]
    fn test_update_dataset_basepath_same_with_snapshot() {
        // arrange
        let tmp = tempfile::tempdir().unwrap();
        let basepath = tmp.path().to_path_buf();
        let basepath_copy = basepath.clone();
        let mut mock = MockRecordRepository::new();
        mock.expect_get_dataset().returning(move |_| {
            let mut ds = Dataset::new(&basepath_copy);
            ds.snapshot = Some(Checksum::SHA1("abc123".to_owned()));
            Ok(Some(ds))
        });
        mock.expect_put_dataset().returning(|_| Ok(()));
        // act
        let usecase = UpdateDataset::new(Box::new(mock));
        let params = Params {
            id: "cafebabe".to_owned(),
            basepath: basepath.clone(),
            schedules: vec![],
            workspace: None,
            chunk_size: 1_048_576,
            pack_size: 33_554_432,
            stores: vec!["cafebabe".to_owned()],
            excludes: vec![],
            retention: SnapshotRetention::ALL,
        };
        let result = usecase.call(params);
        // assert
        assert!(result.is_ok());
        let actual = result.unwrap();
        assert_eq!(actual.basepath, basepath);
    }

    #[test]
    fn test_update_dataset_basepath_not_directory() {
        // arrange
        let tmp = tempfile::tempdir().unwrap();
        let file_path = tmp.path().join("a_file");
        std::fs::write(&file_path, b"not a directory").unwrap();
        let mock = MockRecordRepository::new();
        // act
        let usecase = UpdateDataset::new(Box::new(mock));
        let params = Params {
            id: "cafebabe".to_owned(),
            basepath: file_path,
            schedules: vec![],
            workspace: None,
            chunk_size: 1_048_576,
            pack_size: 33_554_432,
            stores: vec!["cafebabe".to_owned()],
            excludes: vec![],
            retention: SnapshotRetention::ALL,
        };
        let result = usecase.call(params);
        // assert
        assert!(result.is_err());
        let msg = format!("{}", result.unwrap_err());
        assert!(msg.contains("not a directory"));
    }
}
