//
// Copyright (c) 2020 Nathan Fiedler
//
use crate::domain::entities::Checksum;
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
        request.passphrase = crate::domain::managers::get_passphrase();
        self.restorer.enqueue(request)
    }
}

pub struct Params {
    /// Hash digest of the file to restore.
    digest: Checksum,
    /// Relative path of file to be restored.
    filepath: PathBuf,
    /// Identifier of the dataset containing the snapshot.
    dataset: String,
}

impl Params {
    pub fn new(digest: Checksum, filepath: PathBuf, dataset: String) -> Self {
        Self {
            digest,
            filepath,
            dataset,
        }
    }
}

impl fmt::Display for Params {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Params({})", self.digest)
    }
}

impl Into<Request> for Params {
    fn into(self) -> Request {
        Request::new(self.digest, self.filepath, self.dataset, String::new())
    }
}

impl cmp::PartialEq for Params {
    fn eq(&self, other: &Self) -> bool {
        self.digest == other.digest
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
        let file_digest = Checksum::SHA1("b14c4909c3fce2483cd54b328ada88f5ef5e8f96".into());
        let filepath = PathBuf::from("restored.txt");
        let dataset = String::from("dataset1");
        let params = Params::new(file_digest.clone(), filepath.clone(), dataset);
        let result = usecase.call(params);
        // assert
        assert!(result.is_ok());
    }
}
