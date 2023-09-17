//
// Copyright (c) 2023 Nathan Fiedler
//
use crate::domain::entities::Checksum;
use crate::domain::helpers::crypto;
use crate::domain::managers::restore::{Request, Restorer};
use anyhow::Error;
use std::cmp;
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;

pub struct RestoreFiles {
    restorer: Arc<dyn Restorer>,
}

impl RestoreFiles {
    pub fn new(restorer: Arc<dyn Restorer>) -> Self {
        Self { restorer }
    }
}

impl super::UseCase<(), Params> for RestoreFiles {
    fn call(&self, params: Params) -> Result<(), Error> {
        let mut request: Request = params.into();
        request.passphrase = crypto::get_passphrase();
        self.restorer.enqueue(request)
    }
}

pub struct Params {
    /// Digest of the tree containing the entry to restore.
    pub tree: Checksum,
    /// Name of the entry within the tree to be restored.
    pub entry: String,
    /// Relative path of entry to be restored.
    filepath: PathBuf,
    /// Identifier of the dataset containing the snapshot.
    dataset: String,
}

impl Params {
    pub fn new(tree: Checksum, entry: String, filepath: PathBuf, dataset: String) -> Self {
        Self {
            tree,
            entry,
            filepath,
            dataset,
        }
    }
}

impl fmt::Display for Params {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Params({}, {})", self.tree, self.entry)
    }
}

impl From<Params> for Request {
    fn from(val: Params) -> Self {
        Request::new(
            val.tree,
            val.entry,
            val.filepath,
            val.dataset,
            String::new(),
        )
    }
}

impl cmp::PartialEq for Params {
    fn eq(&self, other: &Self) -> bool {
        self.tree == other.tree && self.entry == other.entry
    }
}

impl cmp::Eq for Params {}

#[cfg(test)]
mod tests {
    use super::super::UseCase;
    use super::*;
    use crate::domain::managers::restore::MockRestorer;

    #[test]
    fn test_restore_files_ok() {
        // arrange
        let mut mock = MockRestorer::new();
        mock.expect_enqueue().returning(|_| Ok(()));
        // act
        let usecase = RestoreFiles::new(Arc::new(mock));
        let tree = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".into());
        let entry = String::from("entry.txt");
        let filepath = PathBuf::from("restored.txt");
        let dataset = String::from("dataset1");
        let params = Params::new(tree.clone(), entry.clone(), filepath.clone(), dataset);
        let result = usecase.call(params);
        // assert
        assert!(result.is_ok());
    }
}
